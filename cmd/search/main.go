package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	ginprometheus "github.com/zsais/go-gin-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_hits_total",
	Help: "The total number of cache hits",
}, []string{"cache_type"})

var cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_misses_total",
	Help: "The total number of cache misses",
}, []string{"cache_type"})

type ThreadViewCacheEntry struct {
	ThreadView []search.PostView
	Expiration time.Time
}

type API struct {
	PostRegistry       *search.PostRegistry
	ThreadViewCacheTTL time.Duration
	ThreadViewCache    *lru.ARCCache
}

func main() {
	ctx := context.Background()
	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING environment variable is required")
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Println("initializing tracer...")
		shutdown, err := installExportPipeline(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to create PostRegistry: %v", err)
	}
	defer postRegistry.Close()

	// Hellthread is around 160KB right now so 1000 worst-case threads should be around 160MB
	threadViewCache, err := lru.NewARC(1000)
	if err != nil {
		log.Fatalf("Failed to create threadViewCache: %v", err)
	}

	api := &API{
		PostRegistry:       postRegistry,
		ThreadViewCache:    threadViewCache,
		ThreadViewCacheTTL: 5 * time.Minute,
	}

	router := gin.Default()

	router.Use(otelgin.Middleware("BSkySearchAPI"))

	// CORS middleware
	router.Use(cors.New(
		cors.Config{
			AllowOrigins: []string{"https://bsky.jazco.dev"},
			AllowMethods: []string{"GET", "OPTIONS"},
			AllowHeaders: []string{"Origin", "Content-Length", "Content-Type"},
			AllowOriginFunc: func(origin string) bool {
				u, err := url.Parse(origin)
				if err != nil {
					return false
				}
				// Allow localhost and localnet requests for localdev
				return u.Hostname() == "localhost" || u.Hostname() == "10.0.6.32"
			},
		},
	))

	// Prometheus middleware
	p := ginprometheus.NewPrometheus("gin")
	p.Use(router)

	router.GET("/thread", func(c *gin.Context) {
		authorID := c.Query("authorID")
		authorHandle := c.Query("authorHandle")
		postID := c.Query("postID")
		api.processThreadRequest(c, authorID, authorHandle, postID)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}

func (api *API) processThreadRequest(c *gin.Context, authorID, authorHandle, postID string) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "processThreadRequest")
	defer span.End()
	span.SetAttributes(
		attribute.String("author_id", authorID),
		attribute.String("author_handle", authorHandle),
		attribute.String("post_id", postID),
	)

	if authorID == "" && authorHandle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorID or authorHandle must be provided"})
		return
	}

	if postID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "postID must be provided"})
		return
	}

	if authorID == "" {
		authors, err := api.PostRegistry.GetAuthorsByHandle(ctx, authorHandle)
		if err != nil {
			log.Printf("Error getting authors: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(authors) == 0 {
			log.Printf("Author with handle '%s' not found", authorHandle)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Author with handle '%s' not found", authorHandle)})
			return
		}
		authorID = authors[0].DID
		span.SetAttributes(attribute.String("resolved_author_id", authorID))
	}

	// Get highest level post in thread
	rootPost, err := api.getRootOrOldestParent(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Post with postID '%s' not found", postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Post with postID '%s' not found", postID)})
		} else {
			log.Printf("Error getting root post: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	// Check for the thread in the ARC Cache
	entry, ok := api.ThreadViewCache.Get(postID)
	if ok {
		cacheEntry := entry.(ThreadViewCacheEntry)
		if cacheEntry.Expiration.After(time.Now()) {
			cacheHits.WithLabelValues("thread").Inc()
			span.SetAttributes(attribute.Bool("thread_cache_hit", true))
			c.JSON(http.StatusOK, cacheEntry.ThreadView)
			return
		}
		// If the thread is expired, remove it from the cache
		api.ThreadViewCache.Remove(postID)
	}

	threadView, err := api.PostRegistry.GetThreadView(ctx, rootPost.ID, rootPost.AuthorDID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)})
		} else {
			log.Printf("Error getting thread: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	span.SetAttributes(attribute.Bool("thread_cache_hit", false))

	// Update the ARC Cache
	api.ThreadViewCache.Add(postID, ThreadViewCacheEntry{
		ThreadView: threadView,
		Expiration: time.Now().Add(api.ThreadViewCacheTTL),
	})

	c.JSON(http.StatusOK, threadView)
}

func (api *API) getRootOrOldestParent(ctx context.Context, postID string) (*search.Post, error) {
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "getRootOrOldestParent")
	defer span.End()
	// Get post from registry to look for root post
	span.AddEvent("getRootOrOldestParent:ResolvePrimaryPost")
	post, err := api.PostRegistry.GetPost(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			span.SetAttributes(attribute.Bool("primary_post_found", false))
			return nil, fmt.Errorf("post with postID '%s' not found: %w", postID, err)
		}
		return nil, err
	}

	span.SetAttributes(attribute.Bool("primary_post_found", true))

	// If post has a root post and we've stored it, return it
	if post.RootPostID != nil {
		span.AddEvent("getRootOrOldestParent:ResolveRootPost")
		rootPost, err := api.PostRegistry.GetPost(ctx, *post.RootPostID)
		if err != nil {
			// If we don't have the root post, continue to just return the oldest parent
			if !errors.As(err, &search.NotFoundError{}) {
				return nil, err
			}
			span.SetAttributes(attribute.Bool("root_found", false))
		}

		if rootPost != nil {
			span.SetAttributes(attribute.Bool("root_found", true))
			return rootPost, nil
		}
	}

	// Otherwise, get the oldest parent from the registry
	span.AddEvent("getRootOrOldestParent:ResolveOldestParent")
	oldestParent, err := api.PostRegistry.GetOldestPresentParent(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			span.SetAttributes(attribute.Bool("oldest_parent_found", false))
			return post, nil
		}
		return nil, err
	}

	if oldestParent != nil {
		span.SetAttributes(attribute.Bool("oldest_parent_found", true))
		return oldestParent, nil
	}

	return post, nil
}

func installExportPipeline(ctx context.Context) (func(context.Context) error, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	tracerProvider := newTraceProvider(exporter)
	otel.SetTracerProvider(tracerProvider)

	return tracerProvider.Shutdown, nil
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("BSkySearchAPI"),
		),
	)

	if err != nil {
		panic(err)
	}

	// initialize the traceIDRatioBasedSampler
	traceIDRatioBasedSampler := sdktrace.TraceIDRatioBased(1)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(traceIDRatioBasedSampler),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
