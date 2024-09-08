package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "net/http/pprof"

	intEvents "github.com/ericvolp12/bsky-experiments/pkg/events"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-signals:
			cancel()
			fmt.Println("shutting down on signal")
		case <-ctx.Done():
			fmt.Println("shutting down on context done")
		}
	}()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})))
	logger := slog.Default().With("source", "graph_builder_main")

	logger.Info("starting graph builder...")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		logger.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "BSkyGraphBuilder", 0.2)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING is required")
	}

	redisAddress := os.Getenv("REDIS_ADDRESS")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		log.Fatalf("failed to instrument redis with tracing: %+v\n", err)
	}

	// Test the connection to redis
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v\n", err)
	}

	workerCount := 20
	logger.Info("initializing BSky Event Handler...")
	bsky, err := intEvents.NewBSky(
		ctx,
		logger,
		dbConnectionString,
		"graph_builder",
		redisClient,
		workerCount,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Server for pprof and prometheus via promhttp
	go func() {
		logger := logger.With("source", "pprof_server")
		logger.Info("starting pprof and prometheus server...")
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe("0.0.0.0:6060", nil)
		if err != nil {
			logger.Error("pprof and prometheus server failed", "error", err)
		}
		logger.Info("pprof and prometheus server stopped")
	}()

	logger = logger.With("source", "repo_stream_main_loop")

	// Run a routine that handles the events from the WebSocket
	err = connectToJetstream(ctx, bsky, logger)
	if err != nil {
		logger.Error("disconnecting from Jetstream", "error", err)
	}

	logger.Info("graph builder stopped")
}

func connectToJetstream(
	ctx context.Context,
	bsky *intEvents.BSky,
	logger *slog.Logger,
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Liveness check
	go func() {
		interval := 1 * time.Minute
		t := time.NewTicker(interval)
		defer t.Stop()
		logger := logger.With("source", "liveness_check")

		lastSeq := int64(0)
		for {
			select {
			case <-t.C:
				bsky.SeqMux.RLock()
				if lastSeq >= bsky.LastSeq {
					logger.Warn("no new events in the last minute", "last_seq", lastSeq, "current_seq", bsky.LastSeq)
					bsky.SeqMux.RUnlock()
					cancel()
				}
				lastSeq = bsky.LastSeq
				bsky.SeqMux.RUnlock()
			case <-streamCtx.Done():
				return
			}
		}
	}()

	// Try to read the seq number from Redis
	var cursor *int64
	cursorStr := bsky.GetCursor(ctx)
	if cursorStr != "" {
		logger.Info("found cursor in Redis", "cursor", cursorStr)
		cursorVal, err := strconv.ParseInt(cursorStr, 10, 64)
		if err != nil {
			logger.Error("failed to parse cursor from Redis, starting without a cursor", "error", err)
		} else {
			cursor = &cursorVal
		}
	}

	logger.Info("connecting to Jetstream WebSocket...")
	config := client.DefaultClientConfig()
	config.WebsocketURL = "wss://jetstream.atproto.tools/subscribe"
	config.WantedCollections = []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.graph.block",
	}

	c, err := client.NewClient(config, logger, bsky.Scheduler)
	if err != nil {
		log.Fatalf("failed to create Jetstream client: %v", err)
	}

	err = c.ConnectAndRead(streamCtx, cursor)
	if err != nil {
		logger.Error("failed to connect and read from Jetstream", "error", err)
	}

	return fmt.Errorf("connect and read errored out: %v", err)
}
