package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sync"
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
	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v\n", err)
	}
	defer func() {
		err := rawlog.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar().With("source", "graph_builder_main")

	log.Info("starting graph builder...")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, 1)
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

	workerCount := 5

	postRegistryEnabled := false
	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString != "" {
		postRegistryEnabled = true
	}

	sentimentAnalysisEnabled := false
	sentimentServiceHost := os.Getenv("SENTIMENT_SERVICE_HOST")
	if sentimentServiceHost != "" {
		sentimentAnalysisEnabled = true
	}

	redisAddress := os.Getenv("REDIS_ADDRESS")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	meiliAddress := os.Getenv("MEILI_ADDRESS")
	if meiliAddress == "" {
		meiliAddress = "http://localhost:7700"
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
		log,
		includeLinks, postRegistryEnabled, sentimentAnalysisEnabled,
		dbConnectionString, sentimentServiceHost, meiliAddress,
		redisGraph,
		redisClient,
		workerCount,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Try to read the seq number from Redis
	cursor := redisGraph.Cursor
	if cursor != "" {
		log.Infof("found cursor in redis: %s", cursor)
		u.RawQuery = fmt.Sprintf("cursor=%s", cursor)
	}

	quit := make(chan struct{})

	wg := &sync.WaitGroup{}

	// Server for pprof and prometheus via promhttp
	go func() {
		log = log.With("source", "pprof_server")
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

	for {
		log.Info("connecting to BSky WebSocket...")
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Infof("failed to connect to websocket: %v", err)
			continue
		}
		defer c.Close()

		// Create a new context with a cancel function
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Start a timer to check for graph updates
		updateCheckDuration := 30 * time.Second
		updateCheckTimer := time.NewTicker(updateCheckDuration)
		defer updateCheckTimer.Stop()

		// Run a goroutine to handle the graph update check
		go func() {
			for {
				select {
				case <-updateCheckTimer.C:
					bsky.PersistedGraph.CursorMux.RLock()
					if bsky.PersistedGraph.LastUpdated.Add(updateCheckDuration).Before(time.Now()) {
						log.Infof("The graph hasn't been updated in the past %v seconds, exiting the graph builder (docker should restart it and get us in a good state)", updateCheckDuration)
						bsky.PersistedGraph.CursorMux.RUnlock()
						os.Exit(1)
						cancel()
						c.Close()
						return
					}
					bsky.PersistedGraph.CursorMux.RUnlock()
				case <-streamCtx.Done():
					return
				}
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
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}
