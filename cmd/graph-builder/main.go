package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	"go.uber.org/zap"
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

	log := rawlog.Sugar().With("source", "graph_builder_main")

	log.Info("starting graph builder...")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
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

	workerCount := 20

	postRegistryEnabled := false
	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString != "" {
		postRegistryEnabled = true
	}

	plcDirectoryMirror := os.Getenv("PLC_DIRECTORY_MIRROR")
	if plcDirectoryMirror == "" {
		plcDirectoryMirror = "https://plc.jazco.io"
	}

	redisAddress := os.Getenv("REDIS_ADDRESS")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})

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

	log.Info("initializing BSky Event Handler...")
	bsky, err := intEvents.NewBSky(
		ctx,
		postRegistryEnabled,
		dbConnectionString, "graph_builder", plcDirectoryMirror,
		redisClient,
		workerCount,
	)
	if err != nil {
		log.Fatal(err)
	}

	quit := make(chan struct{})

	wg := &sync.WaitGroup{}

	// Server for pprof and prometheus via promhttp
	go func() {
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "pprof_server")
		log.Info("starting pprof and prometheus server...")
		http.Handle("/metrics", promhttp.Handler())
		log.Info(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	log = log.With("source", "repo_stream_main_loop")

	// Run a routine that handles the events from the WebSocket
	log.Info("starting repo sync routine...")
	err = handleRepoStreamWithRetry(ctx, bsky, log)

	if err != nil {
		log.Errorf("Error: %v", err)
	}

	log.Info("waiting for routines to finish...")
	close(quit)
	wg.Wait()
	log.Info("routines finished, exiting...")
}

func handleRepoStreamWithRetry(
	ctx context.Context,
	bsky *intEvents.BSky,
	log *zap.SugaredLogger,
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run a goroutine to handle the graph update check
	go func() {
		// Start a timer to check for graph updates
		updateCheckDuration := 1 * time.Minute
		updateCheckTimer := time.NewTicker(updateCheckDuration)
		defer updateCheckTimer.Stop()

		lastSeq := int64(0)
		for {
			select {
			case <-updateCheckTimer.C:
				bsky.SeqMux.RLock()
				if lastSeq >= bsky.LastSeq {
					fmt.Printf("lastSeq: %d, bsky.LastSeq: %d | progress hasn't been made in %v, exiting...\n", lastSeq, bsky.LastSeq, updateCheckDuration)
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
	// var cursor *int64
	// cursorStr := bsky.GetCursor(ctx)
	// if cursorStr != "" {
	// 	log.Infow("found cursor in Redis", "cursor", cursorStr)
	// 	cursorVal, err := strconv.ParseInt(cursorStr, 10, 64)
	// 	if err != nil {
	// 		log.Errorf("failed to parse cursor from Redis: %v", err)
	// 	} else {
	// 		cursor = &cursorVal
	// 	}
	// }

	log.Info("connecting to Jetstream WebSocket...")
	config := client.DefaultClientConfig()
	config.WebsocketURL = "wss://jetstream.atproto.tools/subscribe"
	config.WantedCollections = []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.graph.block",
	}

	c, err := client.NewClient(config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	c.Handler = bsky

	err = c.ConnectAndRead(streamCtx, nil)
	if err != nil {
		log.Errorf("failed to connect and read: %v", err)
	}

	return fmt.Errorf("connect and read errored out: %v", err)
}
