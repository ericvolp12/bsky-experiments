package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "jazbot",
		Usage:   "atproto firehose bot",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "ws-url",
			Usage:   "full websocket path to the ATProto SubscribeRepos XRPC endpoint",
			Value:   "wss://bsky.social/xrpc/com.atproto.sync.subscribeRepos",
			EnvVars: []string{"WS_URL"},
		},
		&cli.IntFlag{
			Name:    "worker-count",
			Usage:   "number of workers to process events",
			Value:   10,
			EnvVars: []string{"WORKER_COUNT"},
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
			Value:   "jazbot",
			EnvVars: []string{"REDIS_PREFIX"},
		},
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
		&cli.StringFlag{
			Name:    "bot-did",
			Usage:   "did of the bot",
			EnvVars: []string{"BOT_DID"},
		},
	}

	app.Action = Jazbot

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer trace.Tracer

// Jazbot is the main function for the jazbot
func Jazbot(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	log := rawlog.Sugar().With("source", "jazbot_main")

	log.Info("starting jazbot")

	u, err := url.Parse(cctx.String("ws-url"))
	if err != nil {
		log.Fatalf("failed to parse ws-url: %+v\n", err)
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "Jazbot", 0.01)
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

	j, err := jazbot.NewJazbot(ctx, store, cctx.String("bot-did"))
	if err != nil {
		log.Fatalf("failed to create jazbot: %+v\n", err)
	}

	c, err := jazbot.NewConsumer(
		ctx,
		log,
		redisClient,
		cctx.String("redis-prefix"),
		j,
		u.String(),
	)
	if err != nil {
		log.Fatalf("failed to create jazbot: %+v\n", err)
	}

	wg := sync.WaitGroup{}

	pool := events.NewConsumerPool(cctx.Int("worker-count"), 10, c.HandleStreamEvent)

	// Start a goroutine to manage the cursor, saving the current cursor every 5 seconds.
	go func() {
		wg.Add(1)
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "cursor_manager")

		for {
			select {
			case <-ctx.Done():
				log.Info("shutting down cursor manager")
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Errorf("failed to write cursor: %+v\n", err)
				}
				log.Info("cursor manager shut down successfully")
				return
			case <-ticker.C:
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Errorf("failed to write cursor: %+v\n", err)
				}
			}
		}
	}()

	// Start a goroutine to manage the liveness checker, shutting down if no events are received for 15 seconds
	go func() {
		wg.Add(1)
		defer wg.Done()
		ticker := time.NewTicker(15 * time.Second)
		lastSeq := int64(0)

		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "liveness_checker")

		for {
			select {
			case <-ctx.Done():
				log.Info("shutting down liveness checker")
				return
			case <-ticker.C:
				c.ProgMux.Lock()
				seq := c.Progress.LastSeq
				c.ProgMux.Unlock()
				if seq == lastSeq {
					log.Errorf("no new events in last 15 seconds, shutting down for docker to restart me (last seq: %d)", seq)
					cancel()
				} else {
					log.Infof("last event sequence: %d", seq)
					lastSeq = seq
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	metricServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: mux,
	}

	// Startup metrics server
	go func() {
		wg.Add(1)
		defer wg.Done()
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "metrics_server")

		log.Infof("metrics server listening on port %d", cctx.Int("port"))

		if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("failed to start metrics server: %+v\n", err)
		}
		log.Info("metrics server shut down successfully")
	}()

	if c.Progress.LastSeq >= 0 {
		u.RawQuery = fmt.Sprintf("cursor=%d", c.Progress.LastSeq)
	}

	log.Infof("connecting to WebSocket at: %s", u.String())
	con, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Infof("failed to connect to websocket: %v", err)
		return err
	}
	defer con.Close()

	go func() {
		wg.Add(1)
		defer wg.Done()
		err = events.HandleRepoStream(ctx, con, pool)
		log.Infof("HandleRepoStream returned unexpectedly: %+v...", err)
		cancel()
	}()

	select {
	case <-signals:
		cancel()
		fmt.Println("shutting down on signal")
	case <-ctx.Done():
		fmt.Println("shutting down on context done")
	}

	log.Info("shutting down, waiting for workers to clean up...")

	if err := metricServer.Shutdown(ctx); err != nil {
		log.Errorf("failed to shut down metrics server: %+v\n", err)
		wg.Done()
	}

	wg.Wait()
	log.Info("shut down successfully")

	return nil
}
