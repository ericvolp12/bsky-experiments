package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/pubsky"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-contrib/cors"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "pubsky",
		Usage:   "bluesky public view api",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.IntFlag{
			Name:    "port",
			Usage:   "port to serve metrics on",
			Value:   8080,
			EnvVars: []string{"PORT"},
		},
		&cli.StringFlag{
			Name:    "redis-address",
			Usage:   "redis address for storing progress",
			Value:   "localhost:6379",
			EnvVars: []string{"REDIS_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "redis-prefix",
			Usage:   "redis prefix for storing progress",
			Value:   "pubsky",
			EnvVars: []string{"REDIS_PREFIX"},
		},
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
		&cli.StringFlag{
			Name:    "plc-mirror",
			Usage:   "URL of the PLC Directory Mirror",
			EnvVars: []string{"PLC_MIRROR"},
		},
	}

	app.Action = Pubsky

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer = otel.Tracer("pubsky")

// Pubsky is the main function for pubsky
func Pubsky(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	kill := make(chan struct{})

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v\n", err)
	}
	defer func() {
		log.Printf("main function teardown\n")
		err := rawlog.Sync()
		if err != nil {
			log.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar().With("source", "pubsky_main")

	log.Info("starting pubsky")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "pubsky", 1)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cctx.String("redis-address"),
		Password: "",
		DB:       0,
	})
	if err != nil {
		log.Fatalf("failed to create redis client: %+v\n", err)
	}

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		log.Fatalf("failed to instrument redis with tracing: %+v\n", err)
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		log.Fatalf("failed to instrument redis with metrics: %+v\n", err)
	}

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v\n", err)
	}

	// Create a Store
	store, err := store.NewStore(cctx.String("postgres-url"))
	if err != nil {
		log.Fatalf("failed to create store: %+v\n", err)
	}

	psky := pubsky.NewPubsky(ctx, store, redisClient, cctx.String("plc-mirror"), nil)

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
					rawlog.Error(e)
				}
			} else if path != "/metrics" {
				rawlog.Info(path,
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

	router.Use(ginzap.RecoveryWithZap(rawlog, true))

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
			AllowOrigins: []string{"https://psky.jazco.dev"},
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

	// Register the routes
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	v1 := router.Group("/api/v1")
	{
		v1.GET("/profile/:handle_or_did/post/:rkey", psky.HandleGetPost)
	}

	// Serve static files from the public folder
	router.StaticFile("/", "./public/index.html")
	router.GET("/profile/:handle_or_did/post/:rkey", psky.HandleIndex)
	router.Static("/assets", "./public/assets")

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: router,
	}

	serverShutdown := make(chan struct{})
	go func() {
		defer close(serverShutdown)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server shutdown: %+v\n", err)
		}
	}()

	select {
	case <-signals:
		log.Info("shutting down on signal")
	case <-ctx.Done():
		log.Info("shutting down on context done")
	case <-kill:
		log.Info("shutting down on kill")
	}

	log.Info("shutting down, waiting for workers to clean up...")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("failed to shutdown http server: %+v\n", err)
	}

	select {
	case <-serverShutdown:
		log.Info("http server shut down successfully")
	case <-ctx.Done():
		log.Info("http server shutdown timed out")
	}

	log.Info("shut down successfully")

	return nil
}
