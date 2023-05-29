package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	feedgenerator "github.com/ericvolp12/bsky-experiments/pkg/feed-generator"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	es256k "github.com/ericvolp12/jwt-go-secp256k1"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/multiformats/go-multibase"
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

type preheatItem struct {
	authorID string
	postID   string
}

type PLCEntry struct {
	Context            []string `json:"@context"`
	ID                 string   `json:"id"`
	AlsoKnownAs        []string `json:"alsoKnownAs"`
	VerificationMethod []struct {
		ID                 string `json:"id"`
		Type               string `json:"type"`
		Controller         string `json:"controller"`
		PublicKeyMultibase string `json:"publicKeyMultibase"`
	} `json:"verificationMethod"`
	Service []struct {
		ID              string `json:"id"`
		Type            string `json:"type"`
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}

type KeyCacheEntry struct {
	UserDID   string
	Key       *ecdsa.PublicKey
	ExpiresAt time.Time
}

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_hits_total",
	Help: "The total number of cache hits",
}, []string{"cache_type"})

var cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_misses_total",
	Help: "The total number of cache misses",
}, []string{"cache_type"})

var cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsky_cache_size_bytes",
	Help: "The size of the cache in bytes",
}, []string{"cache_type"})

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

	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create XRPC client: %v", err)
	}

	feedGenerator, err := feedgenerator.NewFeedGenerator(ctx, postRegistry, client, graphJSONUrl)
	if err != nil {
		log.Fatalf("Failed to create FeedGenerator: %v", err)
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

	keyCacheTTL := 60 * time.Minute
	keyCache, err := lru.NewARC(10000)
	if err != nil {
		log.Fatalf("Failed to create LRU cache: %v", err)
	}

	// Auth middleware
	router.Use(func(c *gin.Context) {
		tracer := otel.Tracer("feed-generator")
		ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:AuthMiddleware")

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			span.End()
			c.Next()
			return
		}

		authHeaderParts := strings.Split(authHeader, " ")
		if len(authHeaderParts) != 2 {
			span.End()
			c.Next()
			return
		}

		if authHeaderParts[0] != "Bearer" {
			span.End()
			c.Next()
			return
		}

		accessToken := authHeaderParts[1]

		claims := &jwt.StandardClaims{}

		parser := jwt.Parser{
			ValidMethods: []string{es256k.SigningMethodES256K.Alg()},
		}

		token, err := parser.ParseWithClaims(accessToken, claims, func(token *jwt.Token) (interface{}, error) {
			if claims, ok := token.Claims.(*jwt.StandardClaims); ok {
				// Get the user's key from PLC Directory: https://plc.directory/{did}
				userDID := claims.Issuer
				entry, ok := keyCache.Get(userDID)
				if ok {
					cacheEntry := entry.(KeyCacheEntry)
					if cacheEntry.ExpiresAt.After(time.Now()) {
						if cacheEntry.ExpiresAt.After(time.Now()) {
							cacheHits.WithLabelValues("key").Inc()
							span.SetAttributes(attribute.Bool("caches.keys.hit", true))
							return cacheEntry.Key, nil
						}
					}
				}

				cacheMisses.WithLabelValues("key").Inc()
				span.SetAttributes(attribute.Bool("caches.keys.hit", false))

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://plc.directory/%s", userDID), nil)
				if err != nil {
					return nil, fmt.Errorf("Failed to create PLC Directory request: %v", err)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return nil, fmt.Errorf("Failed to get user's key from PLC Directory: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					return nil, fmt.Errorf("Failed to get user's key from PLC Directory: %v", resp.Status)
				}

				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return nil, fmt.Errorf("Failed to read PLC Directory response: %v", err)
				}
				// Unmarshal into a PLC Entry
				plcEntry := &PLCEntry{}
				err = json.Unmarshal(body, plcEntry)
				if err != nil {
					return nil, fmt.Errorf("Failed to unmarshal PLC Entry: %v", err)
				}

				// Get the public key from the PLC Entry
				if len(plcEntry.VerificationMethod) == 0 {
					return nil, fmt.Errorf("No verification method found in PLC Entry")
				}

				// Get the public key from the PLC Entry
				publicKey := plcEntry.VerificationMethod[0].PublicKeyMultibase

				// Decode the public key
				_, decodedPublicKey, err := multibase.Decode(publicKey)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode public key: %v", err)
				}

				log.Printf("Decoded public key: %x", decodedPublicKey)

				pub, err := secp256k1.ParsePubKey(decodedPublicKey)
				if err != nil {
					return nil, fmt.Errorf("Failed to parse public key: %v", err)
				}

				ecdsaPubKey := pub.ToECDSA()

				// Add the key to the cache
				keyCache.Add(userDID, KeyCacheEntry{
					Key:       ecdsaPubKey,
					ExpiresAt: time.Now().Add(keyCacheTTL),
				})

				return ecdsaPubKey, nil
			}

			return nil, fmt.Errorf("Invalid authorization token")
		})

		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			span.End()
			c.Abort()
			return
		}

		if claims, ok := token.Claims.(*jwt.StandardClaims); ok {
			if claims.Audience != "did:web:feedsky.jazco.io" {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization token (invalid audience)"})
				c.Abort()
				return
			}
			// Set claims Issuer to context as user DID
			c.Set("user_did", claims.Issuer)
			span.SetAttributes(attribute.String("user.did", claims.Issuer))
			span.End()
			c.Next()
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization token (invalid claims)"})
			span.End()
			c.Abort()
		}
	})

	router.GET("/update_cluster_assignments", feedGenerator.UpdateClusterAssignments)
	router.GET("/.well-known/did.json", feedGenerator.GetWellKnownDID)

	router.GET("/xrpc/app.bsky.feed.getFeedSkeleton", feedGenerator.GetFeedSkeleton)
	router.GET("/xrpc/app.bsky.feed.describeFeedGenerator", feedGenerator.DescribeFeedGenerator)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
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
			semconv.ServiceName("BSky-Feed-Generator-Go"),
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
