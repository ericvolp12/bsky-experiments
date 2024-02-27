package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/plc"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"
)

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

	log := logger.Sugar()

	log.Info("Starting up PLC Mirror...")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "PLCMirror", 0.2)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
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
					zap.Duration("latency", latency),
				)
			}
		}
	}())

	router.Use(ginzap.RecoveryWithZap(logger, true))

	// Plug in OTEL Middleware and skip metrics endpoint
	router.Use(
		otelgin.Middleware(
			"BSky-PLC",
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

	buckets := prometheus.ExponentialBuckets(0.00001, 2, 20)
	// Prometheus middleware
	p := ginprometheus.NewPrometheus("gin", &ginprometheus.DefaultMetricOverrides{
		RequestDurationSecondsBuckets: &buckets,
	})
	p.Use(router)

	redisAddress := os.Getenv("REDIS_ADDRESS")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})

	var s *store.Store
	var err error

	storePostgresURL := os.Getenv("POSTGRES_URL")
	if storePostgresURL != "" {
		s, err = store.NewStore(storePostgresURL)
		if err != nil {
			log.Fatalf("failed to create store: %+v\n", err)
		}
	}

	// // Enable tracing instrumentation.
	// if err := redisotel.InstrumentTracing(redisClient); err != nil {
	// 	log.Fatalf("failed to instrument redis with tracing: %+v\n", err)
	// }

	// // Enable metrics instrumentation.
	// if err := redisotel.InstrumentMetrics(redisClient); err != nil {
	// 	log.Fatalf("failed to instrument redis with metrics: %+v\n", err)
	// }

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v\n", err)
	}

	plc, err := plc.NewDirectory("https://plc.directory/export", redisClient, s, "plc_directory", false)
	if err != nil {
		log.Fatalf("failed to create plc directory: %+v\n", err)
	}

	plc.Start()

	router.POST("/batch/by_did", func(c *gin.Context) {
		var dids []string
		err := c.BindJSON(&dids)
		if err != nil {
			c.JSON(400, gin.H{
				"error": err.Error(),
			})
			return
		}

		// Lowercase all the dids
		for i, did := range dids {
			dids[i] = strings.ToLower(did)
		}

		docs, err := plc.GetBatchEntriesForDID(c.Request.Context(), dids)
		if err != nil {
			c.JSON(500, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(200, docs)
	})

	router.POST("/batch/by_handle", func(c *gin.Context) {
		var handles []string
		err := c.BindJSON(&handles)
		if err != nil {
			c.JSON(400, gin.H{
				"error": err.Error(),
			})
			return
		}

		// Lowercase all the handles
		for i, handle := range handles {
			handles[i] = strings.ToLower(handle)
		}

		docs, err := plc.GetBatchEntriesForHandle(c.Request.Context(), handles)
		if err != nil {
			c.JSON(500, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(200, docs)
	})

	router.GET("/:lookup_target", func(c *gin.Context) {
		lookupTarget := c.Param("lookup_target")
		// Lowercase the lookup target
		lookupTarget = strings.ToLower(lookupTarget)
		if strings.HasPrefix(lookupTarget, "did:plc:") {
			doc, err := plc.GetEntryForDID(c.Request.Context(), lookupTarget)
			if err != nil {
				c.JSON(404, gin.H{
					"error": err.Error(),
				})
			} else {
				c.JSON(200, doc)
			}
		} else {
			doc, err := plc.GetEntryForHandle(c.Request.Context(), lookupTarget)
			if err != nil {
				c.JSON(404, gin.H{
					"error": err.Error(),
				})
			} else {
				c.JSON(200, doc)
			}
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Infof("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}
