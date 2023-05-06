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

	"github.com/ericvolp12/bsky-experiments/pkg/layout"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.uber.org/zap"
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

type ThreadViewLayout struct {
	Post         search.Post `json:"post"`
	AuthorHandle string      `json:"author_handle"`
	Depth        int         `json:"depth"`
	X            float32     `json:"x"`
	Y            float32     `json:"y"`
}

type ThreadViewCacheEntry struct {
	ThreadView []search.PostView
	Expiration time.Time
}

type LayoutCacheEntry struct {
	Layout     []ThreadViewLayout
	Expiration time.Time
}

type API struct {
	PostRegistry       *search.PostRegistry
	LayoutServiceHost  string
	ThreadViewCacheTTL time.Duration
	ThreadViewCache    *lru.ARCCache
	LayoutCacheTTL     time.Duration
	LayoutCache        *lru.ARCCache
}

func main() {
	ctx := context.Background()
	var logger *zap.Logger

	if os.Getenv("DEBUG") == "true" {
		logger, _ = zap.NewDevelopment()
		logger.Info("Starting logger in DEBUG mode...")
	} else {
		logger, _ = zap.NewProduction()
		logger.Info("Starting logger in PRODUCTION mode...")
	}

	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	sugar := logger.Sugar()

	sugar.Info("Reading config from environment...")

	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING environment variable is required")
	}

	layoutServiceHost := os.Getenv("LAYOUT_SERVICE_HOST")
	if layoutServiceHost == "" {
		layoutServiceHost = "http://localhost:8086"
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

	layoutCache, err := lru.NewARC(250)
	if err != nil {
		log.Fatalf("Failed to create layoutCache: %v", err)
	}

	api := &API{
		PostRegistry:       postRegistry,
		LayoutServiceHost:  layoutServiceHost,
		ThreadViewCache:    threadViewCache,
		ThreadViewCacheTTL: 5 * time.Minute,
		LayoutCache:        layoutCache,
		LayoutCacheTTL:     5 * time.Minute,
	}

	router := gin.New()

	router.Use(gin.Recovery())

	router.Use(func() gin.HandlerFunc {
		return func(c *gin.Context) {
			start := time.Now()
			// These can get consumed during request processing
			path := c.Request.URL.Path
			query := c.Request.URL.RawQuery
			c.Next()

			end := time.Now().UTC()
			latency := end.Sub(start)

			if len(c.Errors) > 0 {
				// Append error field if this is an erroneous request.
				for _, e := range c.Errors.Errors() {
					logger.Error(e)
				}
			} else if path != "/metrics" {
				logger.Info(path,
					zap.Int("status", c.Writer.Status()),
					zap.String("method", c.Request.Method),
					zap.String("path", path),
					zap.String("query", query),
					zap.String("ip", c.ClientIP()),
					zap.String("user-agent", c.Request.UserAgent()),
					zap.String("time", end.Format(time.RFC3339)),
					zap.String("rootPostID", c.GetString("rootPostID")),
					zap.String("rootPostAuthorDID", c.GetString("rootPostAuthorDID")),
					zap.Duration("latency", latency),
				)
			}
		}
	}())

	router.Use(ginzap.RecoveryWithZap(logger, true))

	router.Use(otelgin.Middleware("BSkySearchAPI"))

	// CORS middleware
	router.Use(cors.New(
		cors.Config{
			AllowOrigins: []string{"https://bsky.jazco.dev", "https://hellthread-explorer.bsky-graph.pages.dev"},
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
	p := ginprometheus.NewPrometheus("gin", nil)
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

func (api *API) layoutThread(ctx context.Context, rootPostID string, threadView []search.PostView) ([]ThreadViewLayout, error) {
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "layoutThread")
	defer span.End()

	// Check for the layout in the ARC Cache
	entry, ok := api.LayoutCache.Get(rootPostID)
	if ok {
		cacheEntry := entry.(LayoutCacheEntry)
		if cacheEntry.Expiration.After(time.Now()) {
			cacheHits.WithLabelValues("layout").Inc()
			span.SetAttributes(attribute.Bool("layout_cache_hit", true))
			return cacheEntry.Layout, nil
		}
		// If the layout is expired, remove it from the cache
		api.LayoutCache.Remove(rootPostID)
	}

	cacheMisses.WithLabelValues("layout").Inc()

	edges := createEdges(threadView)
	points, err := layout.SendEdgeListRequest(ctx, api.LayoutServiceHost, edges)
	if err != nil {
		return nil, fmt.Errorf("error sending edge list request: %w", err)
	}

	// Build a ThreadViewLayout list from the threadView and points
	var threadViewLayout []ThreadViewLayout
	for i, post := range threadView {
		// Truncate X and Y to 2 decimal places
		points[i][0] = float32(int(points[i][0]*100)) / 100
		points[i][1] = float32(int(points[i][1]*100)) / 100

		threadViewLayout = append(threadViewLayout, ThreadViewLayout{
			Post:         post.Post,
			AuthorHandle: post.AuthorHandle,
			Depth:        post.Depth,
			X:            points[i][0],
			Y:            points[i][1],
		})
	}

	// Update the ARC Cache
	api.LayoutCache.Add(rootPostID, LayoutCacheEntry{
		Layout:     threadViewLayout,
		Expiration: time.Now().Add(api.LayoutCacheTTL),
	})

	return threadViewLayout, nil
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

	if rootPost == nil {
		log.Printf("Post with postID '%s' not found", postID)
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Post with postID '%s' not found", postID)})
		return
	}

	// Set the rootPostID in the context for the RequestLogger middleware
	c.Set("rootPostID", rootPost.ID)
	c.Set("rootPostAuthorDID", rootPost.AuthorDID)

	// Get thread view
	threadView, err := api.getThreadView(ctx, rootPost.ID, rootPost.AuthorDID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)})
		} else {
			log.Printf("Error getting thread view: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	if c.Query("layout") == "true" {
		threadViewLayout, err := api.layoutThread(ctx, rootPost.ID, threadView)
		if err != nil {
			log.Printf("Error laying out thread: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, threadViewLayout)
		return
	}

	c.JSON(http.StatusOK, threadView)
}

func (api *API) getThreadView(ctx context.Context, postID, authorID string) ([]search.PostView, error) {
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "getThreadView")
	defer span.End()

	// Check for the thread in the ARC Cache
	entry, ok := api.ThreadViewCache.Get(postID)
	if ok {
		cacheEntry := entry.(ThreadViewCacheEntry)
		if cacheEntry.Expiration.After(time.Now()) {
			cacheHits.WithLabelValues("thread").Inc()
			span.SetAttributes(attribute.Bool("thread_cache_hit", true))
			return cacheEntry.ThreadView, nil
		}
		// If the thread is expired, remove it from the cache
		api.ThreadViewCache.Remove(postID)
	}

	cacheMisses.WithLabelValues("thread").Inc()

	threadView, err := api.PostRegistry.GetThreadView(ctx, postID, authorID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, fmt.Errorf("thread with authorID '%s' and postID '%s' not found: %w", authorID, postID, err)
		}
		return nil, err
	}

	// Update the ARC Cache
	api.ThreadViewCache.Add(postID, ThreadViewCacheEntry{
		ThreadView: threadView,
		Expiration: time.Now().Add(api.ThreadViewCacheTTL),
	})

	return threadView, nil
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
			semconv.ServiceName("BSkySearchAPI-Staging"),
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

func createEdges(authors []search.PostView) []layout.Edge {
	var edges []layout.Edge
	nodes := make(map[string]int)
	counter := 0

	for _, author := range authors {
		post := author.Post
		if post.ParentPostID != nil {
			if _, ok := nodes[post.ID]; !ok {
				nodes[post.ID] = counter
				counter++
			}
			if _, ok := nodes[*post.ParentPostID]; !ok {
				nodes[*post.ParentPostID] = counter
				counter++
			}
			edges = append(edges, layout.Edge{nodes[*post.ParentPostID], nodes[post.ID]})
		}
	}

	return edges
}
