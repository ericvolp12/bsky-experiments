package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/auth"
	feedgenerator "github.com/ericvolp12/bsky-experiments/pkg/feed-generator"
	"github.com/ericvolp12/bsky-experiments/pkg/feed-generator/endpoints"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds/authorlabel"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds/bangers"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds/cluster"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds/firehose"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds/postlabel"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
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

	graphJSONUrl := os.Getenv("GRAPH_JSON_URL")
	if graphJSONUrl == "" {
		graphJSONUrl = "https://s3.jazco.io/exported_graph_enriched.json"
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Println("initializing tracer...")
		// Start tracer with 20% sampling rate
		shutdown, err := tracing.InstallExportPipeline(ctx, "BSky-Feed-Generator-Go", 0.2)
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

	feedActorDID := os.Getenv("FEED_ACTOR_DID")
	if feedActorDID == "" {
		log.Fatal("FEED_ACTOR_DID environment variable must be set")
	}

	// serviceEndpoint is a URL that the feed generator will be available at
	serviceEndpoint := os.Getenv("SERVICE_ENDPOINT")
	if serviceEndpoint == "" {
		log.Fatal("SERVICE_ENDPOINT environment variable must be set")
	}

	// Set the acceptable DIDs for the feed generator to respond to
	// We'll default to the feedActorDID and the Service Endpoint as a did:web
	serviceURL, err := url.Parse(serviceEndpoint)
	if err != nil {
		log.Fatal(fmt.Errorf("error parsing service endpoint: %w", err))
	}

	serviceWebDID := "did:web:" + serviceURL.Hostname()

	log.Printf("service DID Web: %s", serviceWebDID)

	acceptableDIDs := []string{feedActorDID, serviceWebDID}

	feedGenerator, err := feedgenerator.NewFeedGenerator(ctx, feedActorDID, serviceWebDID, acceptableDIDs, serviceEndpoint)
	if err != nil {
		log.Fatalf("Failed to create FeedGenerator: %v", err)
	}

	endpoints, err := endpoints.NewEndpoints(feedGenerator, graphJSONUrl, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create Endpoints: %v", err)
	}

	// Create a cluster feed
	clustersFeed, clusterFeedAliases, err := cluster.NewClusterFeed(ctx, feedActorDID, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create ClusterFeed: %v", err)
	}
	feedGenerator.AddFeed(clusterFeedAliases, clustersFeed)

	// Create a postlabel feed
	postLabelFeed, postLabelFeedAliases, err := postlabel.NewPostLabelFeed(ctx, feedActorDID, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create PostLabelFeed: %v", err)
	}
	feedGenerator.AddFeed(postLabelFeedAliases, postLabelFeed)

	// Create an authorlabel feed
	authorLabelFeed, authorLabelFeedAliases, err := authorlabel.NewAuthorLabelFeed(ctx, feedActorDID, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create AuthorLabelFeed: %v", err)
	}
	feedGenerator.AddFeed(authorLabelFeedAliases, authorLabelFeed)

	// Create a firehose feed
	firehoseFeed, firehoseFeedAliases, err := firehose.NewFirehoseFeed(ctx, feedActorDID, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create FirehoseFeed: %v", err)
	}
	feedGenerator.AddFeed(firehoseFeedAliases, firehoseFeed)

	// Create a Bangers feed
	bangersFeed, bangersFeedAliases, err := bangers.NewBangersFeed(ctx, feedActorDID, postRegistry)
	if err != nil {
		log.Fatalf("Failed to create BangersFeed: %v", err)
	}
	feedGenerator.AddFeed(bangersFeedAliases, bangersFeed)

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
					zap.String("feedQuery", c.GetString("feedQuery")),
					zap.String("feedName", c.GetString("feedName")),
					zap.Int64("limit", c.GetInt64("limit")),
					zap.String("cursor", c.GetString("cursor")),
					zap.Duration("latency", latency),
				)
			}
		}
	}())

	router.Use(ginzap.RecoveryWithZap(logger, true))

	// Serve static files from the public folder
	router.Static("/public", "./public")

	// Plug in OTEL Middleware and skip metrics endpoint
	router.Use(
		otelgin.Middleware(
			"BSky-Feed-Generator-Go",
			otelgin.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/metrics"
			}),
		),
	)

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
	p := ginprometheus.NewPrometheus("gin", nil)
	p.Use(router)

	auther, err := auth.NewAuth(
		10000,
		time.Hour*1,
		"https://plc.directory",
		5,
		"did:web:feedsky.jazco.io",
	)
	if err != nil {
		log.Fatalf("Failed to create Auth: %v", err)
	}

	keysJSONPath := os.Getenv("KEYS_JSON_PATH")
	if keysJSONPath == "" {
		log.Fatal("KEYS_JSON_PATH environment variable is required")
	}

	// Add Auth Entities to auth for API Key Auth
	// Read the JSON file
	file, err := os.Open(keysJSONPath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Decode the file into a slice of FeedAuthEntity structs
	entities := make([]*auth.FeedAuthEntity, 0)
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&entities); err != nil {
		log.Fatalf("Failed to decode file: %v", err)
	}

	// Add the entities to the Auth struct's map
	for _, entity := range entities {
		auther.UpdateAPIKeyFeedMapping(entity.APIKey, entity)
	}

	router.GET("/update_cluster_assignments", endpoints.UpdateClusterAssignments)
	router.GET("/.well-known/did.json", endpoints.GetWellKnownDID)

	// JWT Auth middleware
	router.Use(auther.AuthenticateGinRequestViaJWT)

	router.GET("/xrpc/app.bsky.feed.getFeedSkeleton", endpoints.GetFeedSkeleton)
	router.GET("/xrpc/app.bsky.feed.describeFeedGenerator", endpoints.DescribeFeedGenerator)

	// API Key Auth Middleware
	router.Use(auther.AuthenticateGinRequestViaAPIKey)
	router.PUT("/assign_user_to_feed", endpoints.AssignUserToFeed)
	router.PUT("/unassign_user_from_feed", endpoints.UnassignUserFromFeed)
	router.GET("/feed_members", endpoints.GetFeedMembers)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}
