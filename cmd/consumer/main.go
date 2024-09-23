package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	jetstreamclient "github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "consumer",
		Usage:   "atproto firehose consumer",
		Version: "0.0.2",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "ws-url",
			Usage:   "full websocket path to the Jetstream subscription endpoint",
			Value:   "wss://jetstream.atproto.tools/subscribe",
			EnvVars: []string{"WS_URL"},
		},
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
			Value:   "consumer",
			EnvVars: []string{"REDIS_PREFIX"},
		},
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
		&cli.StringFlag{
			Name:    "graphd-root",
			Usage:   "graphd root url",
			EnvVars: []string{"GRAPHD_ROOT"},
		},
		&cli.StringSliceFlag{
			Name:    "shard-db-nodes",
			Usage:   "list of scylla nodes for shard db",
			EnvVars: []string{"SHARD_DB_NODES"},
		},
	}

	app.Action = Consumer

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer = otel.Tracer("Consumer")

// Consumer is the main function for the consumer
func Consumer(cctx *cli.Context) error {
	ctx := cctx.Context

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	kill := make(chan struct{})

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v", err)
	}
	defer func() {
		log.Printf("main function teardown")
		err := rawlog.Sync()
		if err != nil {
			log.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar().With("source", "consumer_main")

	log.Info("starting consumer")

	u, err := url.Parse(cctx.String("ws-url"))
	if err != nil {
		log.Fatalf("failed to parse ws-url: %+v", err)
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "Consumer", 1)
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
		log.Fatalf("failed to create redis client: %+v", err)
	}

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		log.Fatalf("failed to instrument redis with tracing: %+v", err)
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		log.Fatalf("failed to instrument redis with metrics: %+v", err)
	}

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v", err)
	}

	// Create a Store
	store, err := store.NewStore(cctx.String("postgres-url"))
	if err != nil {
		log.Fatalf("failed to create store: %+v", err)
	}

	c, err := consumer.NewConsumer(
		ctx,
		log,
		redisClient,
		cctx.String("redis-prefix"),
		store,
		u.String(),
		cctx.String("graphd-root"),
		cctx.StringSlice("shard-db-nodes"),
	)
	if err != nil {
		log.Fatalf("failed to create consumer: %+v", err)
	}

	// Start a goroutine to manage the cursor, saving the current cursor every 5 seconds.
	shutdownCursorManager := make(chan struct{})
	cursorManagerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v", err)
		}
		log := rawlog.Sugar().With("source", "cursor_manager")

		for {
			select {
			case <-shutdownCursorManager:
				log.Info("shutting down cursor manager")
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Errorf("failed to write cursor: %+v", err)
				}
				log.Info("cursor manager shut down successfully")
				close(cursorManagerShutdown)
				return
			case <-ticker.C:
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Errorf("failed to write cursor: %+v", err)
				}
			}
		}
	}()

	// Start a goroutine to manage the liveness checker, shutting down if no events are received for 15 seconds
	shutdownLivenessChecker := make(chan struct{})
	livenessCheckerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		lastSeq := int64(0)

		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v", err)
		}
		log := rawlog.Sugar().With("source", "liveness_checker")

		for {
			select {
			case <-shutdownLivenessChecker:
				log.Info("shutting down liveness checker")
				close(livenessCheckerShutdown)
				return
			case <-ticker.C:
				seq, _ := c.Progress.Get()
				if seq == lastSeq {
					log.Errorf("no new events in last 15 seconds, shutting down for docker to restart me (last seq: %d)", seq)
					close(kill)
				} else {
					log.Infof("last event sequence: %d", seq)
					lastSeq = seq
				}
			}
		}
	}()

	// Start a goroutine to update daily stats for yesterday and today every hour
	shutdownDailyStatsUpdater := make(chan struct{})
	dailyStatsUpdaterShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v", err)
		}

		log := rawlog.Sugar().With("source", "daily_stats_updater")

		for {
			select {
			case <-shutdownDailyStatsUpdater:
				log.Info("shutting down daily stats updater")
				close(dailyStatsUpdaterShutdown)
				return
			case <-ticker.C:
				ctx := context.Background()
				log.Info("refreshing stats for yesterday and today")
				err = c.Store.Queries.RefreshStatsForDay(ctx, store_queries.RefreshStatsForDayParams{
					StartTodayOffset: -2,
					EndTodayOffset:   1,
				})
				if err != nil {
					log.Errorf("failed to refresh stats for today: %+v", err)
				}
				log.Info("finished refreshing stats for yesterday and today")
			}
		}
	}()

	// Start a goroutine to trim old recent posts from the DB every 5 minutes
	shutdownRecentPostTrimmer := make(chan struct{})
	recentPostTrimmerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v", err)
		}

		log := rawlog.Sugar().With("source", "recent_post_trimmer")

		for {
			select {
			case <-shutdownRecentPostTrimmer:
				log.Info("shutting down recent post trimmer")
				close(recentPostTrimmerShutdown)
				return
			case <-ticker.C:
				// Trim posts older than 3 days
				err := c.TrimRecentPosts(ctx, time.Hour*24*3)
				if err != nil {
					log.Errorf("failed to trim recent posts: %+v", err)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	metricServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: mux,
	}

	// Startup metrics server
	shutdownMetrics := make(chan struct{})
	metricsShutdown := make(chan struct{})
	go func() {
		logger := log.With("source", "metrics_server")

		logger.Infof("metrics server listening on port %d", cctx.Int("port"))

		go func() {
			if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Errorf("failed to start metrics server: %+v", err)
			}
		}()
		<-shutdownMetrics
		if err := metricServer.Shutdown(ctx); err != nil {
			logger.Errorf("failed to shutdown metrics server: %+v", err)
		}
		logger.Info("metrics server shut down")
		close(metricsShutdown)
	}()

	log.Infof("connecting to WebSocket at: %s", u.String())
	con, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{
		"User-Agent": []string{"jaz-consumer/0.0.1"},
	})
	if err != nil {
		log.Infof("failed to connect to websocket: %v", err)
		return err
	}
	defer con.Close()

	scheduler := parallel.NewScheduler(400, "jetstream-prod", slog.Default(), c.OnEvent)

	conf := jetstreamclient.DefaultClientConfig()
	conf.WantedCollections = []string{"app.bsky.*"}
	conf.WebsocketURL = u.String()
	conf.Compress = true
	jetstreamClient, err := jetstreamclient.NewClient(conf, slog.Default(), scheduler)
	if err != nil {
		log.Fatalf("failed to create Jetstream client: %v", err)
	}

	shutdownRepoStream := make(chan struct{})
	repoStreamShutdown := make(chan struct{})
	go func() {
		var cursor *int64
		if c.Progress.LastSeq > 0 {
			cursor = &c.Progress.LastSeq
		}

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			err = jetstreamClient.ConnectAndRead(ctx, cursor)
			if !errors.Is(err, context.Canceled) {
				log.Infof("HandleRepoStream returned unexpectedly, killing the consumer: %+v...", err)
				close(kill)
			} else {
				log.Info("HandleRepoStream closed on context cancel...")
			}
			close(repoStreamShutdown)
		}()
		<-shutdownRepoStream
		cancel()
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
	close(shutdownRepoStream)
	close(shutdownLivenessChecker)
	close(shutdownCursorManager)
	close(shutdownMetrics)
	close(shutdownRecentPostTrimmer)
	close(shutdownDailyStatsUpdater)

	<-repoStreamShutdown
	<-livenessCheckerShutdown
	<-cursorManagerShutdown
	<-metricsShutdown
	<-recentPostTrimmerShutdown
	<-dailyStatsUpdaterShutdown

	err = c.Shutdown()
	if err != nil {
		log.Errorf("failed to shut down consumer: %+v", err)
	}

	log.Info("shut down successfully")

	return nil
}
