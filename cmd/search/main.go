package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/endpoints"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/ericvolp12/bsky-experiments/pkg/usercount"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type preheatItem struct {
	authorID string
	postID   string
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

	graphJSONUrl := os.Getenv("GRAPH_JSON_URL")
	if graphJSONUrl == "" {
		graphJSONUrl = "https://s3.jazco.io/exported_graph_enriched.json"
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Println("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "BSkySearchAPI", 1)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	binaryGraphPath := os.Getenv("BINARY_GRAPH_PATH")
	if binaryGraphPath == "" {
		log.Fatal("BINARY_GRAPH_PATH environment variable is required")
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to create PostRegistry: %v", err)
	}
	defer postRegistry.Close()

	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create XRPC client: %v", err)
	}

	userCount := usercount.NewUserCount(ctx, client)

	api, err := endpoints.NewAPI(
		postRegistry,
		userCount,
		binaryGraphPath,
		graphJSONUrl,
		layoutServiceHost,
		30*time.Minute, // Thread View Cache TTL
		30*time.Minute, // Layout Cache TTL
		5*time.Minute,  // Stats Cache TTL
	)

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

	// Serve static files from the public folder
	router.Static("/public", "./public")

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

	router.GET("/thread", api.ProcessThreadRequest)
	router.GET("/distance", api.GetSocialDistance)
	router.GET("/stats", api.GetAuthorStats)
	router.GET("/post/:id", api.GetPost)

	router.GET("/opted_out_authors", api.GetOptedOutAuthors)
	router.POST("/opt_out", api.GraphOptOut)
	router.POST("/opt_in", api.GraphOptIn)

	router.GET("/clusters", api.GetClusterList)
	router.GET("/users/by_handle/:handle/cluster", api.GetClusterForHandle)
	router.GET("/users/by_did/:did/cluster", api.GetClusterForDID)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Preheat the caches with some popular threads
	preheatList := []preheatItem{
		{authorID: "did:plc:wgaezxqi2spqm3mhrb5xvkzi", postID: "3juzlwllznd24"},
	}

	// Create a routine to preheat the caches every 30 minutes
	cachePreheatTicker := time.NewTicker(30*time.Minute + 45*time.Second)
	go func() {
		ctx := context.Background()
		tracer := otel.Tracer("search-api")
		for {
			ctx, span := tracer.Start(ctx, "preheatCaches")
			log.Printf("Preheating caches with %d threads", len(preheatList))
			for _, threadToHeat := range preheatList {
				threadView, err := api.GetThreadView(ctx, threadToHeat.postID, threadToHeat.authorID)
				if err != nil {
					log.Printf("Error preheating thread view cache: %v", err)
				}
				_, err = api.LayoutThread(ctx, threadToHeat.postID, threadView)
				if err != nil {
					log.Printf("Error preheating layout cache: %v", err)
				}
			}
			span.End()
			select {
			case <-cachePreheatTicker.C:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	statsRefreshTicker := time.NewTicker(5 * time.Minute)

	// Create a routine to refresh site stats every 5 minutes
	go func() {
		ctx := context.Background()
		tracer := otel.Tracer("search-api")
		for {
			ctx, span := tracer.Start(ctx, "refreshSiteStats")
			log.Printf("Refreshing site stats")
			err := api.RefreshSiteStats(ctx)
			if err != nil {
				log.Printf("Error refreshing site stats: %v", err)
			}
			span.End()
			select {
			case <-statsRefreshTicker.C:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}
