package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/events"
	intEvents "github.com/ericvolp12/bsky-experiments/pkg/events"
	"github.com/ericvolp12/bsky-experiments/pkg/persistedgraph"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	maxBackoff       = 30 * time.Second
	maxBackoffFactor = 2
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

	// Replace with the WebSocket URL you want to connect to.
	u := url.URL{Scheme: "wss", Host: "bsky.social", Path: "/xrpc/com.atproto.sync.subscribeRepos"}

	includeLinks := os.Getenv("INCLUDE_LINKS") == "true"

	workerCount := 20

	postRegistryEnabled := false
	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString != "" {
		postRegistryEnabled = true
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

	redisGraph, err := persistedgraph.NewPersistedGraph(ctx, redisClient, "social-graph")
	if err != nil {
		log.Fatalf("failed to initialize persisted graph: %+v\n", err)
	}

	log.Info("initializing BSky Event Handler...")
	bsky, err := intEvents.NewBSky(
		ctx,
		includeLinks, postRegistryEnabled,
		dbConnectionString,
		redisGraph,
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
		// Create a handler to write out the plaintext graph
		http.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
			log.Info("writing graph to HTTP Response...")

			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Disposition", "attachment; filename=social-graph.txt")
			w.Header().Set("Content-Transfer-Encoding", "binary")
			w.Header().Set("Expires", "0")
			w.Header().Set("Cache-Control", "must-revalidate")
			w.Header().Set("Pragma", "public")

			err := bsky.PersistedGraph.Write(ctx, w)
			if err != nil {
				log.Errorf("error writing graph: %s", err)
			} else {
				log.Info("graph written to HTTP Response successfully")
			}
		})

		http.Handle("/metrics", promhttp.Handler())
		log.Info(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	log = log.With("source", "repo_stream_main_loop")

	// Run a routine that handles the events from the WebSocket
	log.Info("starting repo sync routine...")
	err = handleRepoStreamWithRetry(ctx, bsky, log, u, &intEvents.RepoStreamCtxCallbacks{
		RepoCommit: bsky.HandleRepoCommit,
		RepoInfo:   intEvents.HandleRepoInfo,
		Error:      intEvents.HandleError,
	})

	if err != nil {
		log.Errorf("Error: %v", err)
	}

	log.Info("waiting for routines to finish...")
	close(quit)
	wg.Wait()
	log.Info("routines finished, exiting...")
}

func getNextBackoff(currentBackoff time.Duration) time.Duration {
	if currentBackoff == 0 {
		return time.Second
	}

	return currentBackoff + time.Duration(rand.Int63n(int64(maxBackoffFactor*currentBackoff)))
}

func handleRepoStreamWithRetry(
	ctx context.Context,
	bsky *intEvents.BSky,
	log *zap.SugaredLogger,
	u url.URL,
	callbacks *intEvents.RepoStreamCtxCallbacks,
) error {
	var backoff time.Duration
	// Create a new context with a cancel function
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a timer to check for graph updates
	updateCheckDuration := 1 * time.Minute
	updateCheckTimer := time.NewTicker(updateCheckDuration)
	defer updateCheckTimer.Stop()

	// Run a goroutine to handle the graph update check
	go func() {
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

	for {
		// Try to read the seq number from Redis
		cursor := bsky.PersistedGraph.GetCursor(ctx)
		if cursor != "" {
			log.Infof("found cursor in redis: %s", cursor)
			u.RawQuery = fmt.Sprintf("cursor=%s", cursor)
		}

		log.Info("connecting to BSky WebSocket...")
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Infof("failed to connect to websocket: %v", err)
			continue
		}
		defer c.Close()

		go func() {
			select {
			case <-streamCtx.Done():
				log.Info("closing websocket...")
				c.Close()
			}
		}()

		pool := events.NewConsumerPool(16, 32, func(ctx context.Context, xe *events.XRPCStreamEvent) error {
			switch {
			case xe.RepoCommit != nil:
				callbacks.RepoCommit(ctx, xe.RepoCommit)
			case xe.RepoInfo != nil:
				callbacks.RepoInfo(ctx, xe.RepoInfo)
			case xe.Error != nil:
				callbacks.Error(ctx, xe.Error)
			}
			return nil
		})

		err = events.HandleRepoStream(streamCtx, c, pool)
		log.Info("HandleRepoStream returned unexpectedly: %w...", err)
		if err != nil {
			log.Infof("Error in event handler routine: %v", err)
			backoff = getNextBackoff(backoff)
			if backoff > maxBackoff {
				return fmt.Errorf("maximum backoff of %v reached, giving up", maxBackoff)
			}

			select {
			case <-time.After(backoff):
				log.Infof("Reconnecting after %v...", backoff)
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}
