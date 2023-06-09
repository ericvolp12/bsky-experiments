package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/events"
	intEvents "github.com/ericvolp12/bsky-experiments/pkg/events"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
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

	// Replace with the WebSocket URL you want to connect to.
	u := url.URL{Scheme: "wss", Host: "bsky.social", Path: "/xrpc/com.atproto.sync.subscribeRepos"}

	includeLinks := os.Getenv("INCLUDE_LINKS") == "true"

	workerCount := 5

	graphFile := os.Getenv("BINARY_GRAPH_FILE")
	if graphFile == "" {
		graphFile = "social-graph.bin"
	}

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

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v\n", err)
	}

	redisReaderWriter := graph.NewRedisReaderWriter(redisClient)

	log.Info("initializing BSky Event Handler...")
	bsky, err := intEvents.NewBSky(
		ctx,
		log,
		includeLinks, postRegistryEnabled, sentimentAnalysisEnabled,
		dbConnectionString, sentimentServiceHost,
		workerCount,
	)
	if err != nil {
		log.Fatal(err)
	}

	binReaderWriter := graph.BinaryGraphReaderWriter{}

	log.Info("reading social graph from binary file...")
	resumedGraph, err := binReaderWriter.ReadGraph(graphFile)
	if err != nil {
		log.Infof("error reading social graph from binary: %s\n", err)
		log.Info("creating a new graph for this session...")
	} else {
		log.Info("social graph resumed successfully")
		bsky.SocialGraph = resumedGraph
	}

	// Try to read the seq number from file
	seqFile, err := os.Open(graphFile + ".seq")
	if err != nil {
		log.Infof("error reading seq number from file: %s\n", err)
		log.Info("starting from latest seq")
	} else {
		log.Info("reading seq number from file...")
		scanner := bufio.NewScanner(seqFile)
		scanner.Scan()
		seq, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			log.Infof("error reading seq number from file: %s\n", err)
			log.Info("starting from latest seq")
		} else {
			if seq != 0 {
				log.Infof("starting from seq %d", seq)
				bsky.LastSeq = seq
				u.RawQuery = fmt.Sprintf("cursor=%d", seq)
			}
		}
	}

	graphTicker := time.NewTicker(5 * time.Minute)
	quit := make(chan struct{})

	wg := &sync.WaitGroup{}

	// Run a routine that dumps graph data to a file every 5 minutes
	wg.Add(1)
	go func() {
		ctx := context.Background()
		log = log.With("source", "graph_dump")
		log.Info("starting graph dump routine...")
		for {
			select {
			case <-graphTicker.C:
				saveGraph(ctx, log, bsky, &binReaderWriter, redisReaderWriter, graphFile)
			case <-quit:
				graphTicker.Stop()
				wg.Done()
				return
			}
		}
	}()

	// Server for pprof and prometheus via promhttp
	go func() {
		log = log.With("source", "pprof_server")
		log.Info("starting pprof and prometheus server...")
		// Create a handler to write out the plaintext graph
		http.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
			log.Info("writing graph to HTTP Response...")
			bsky.SocialGraphMux.Lock()
			defer bsky.SocialGraphMux.Unlock()

			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Disposition", "attachment; filename=social-graph.txt")
			w.Header().Set("Content-Transfer-Encoding", "binary")
			w.Header().Set("Expires", "0")
			w.Header().Set("Cache-Control", "must-revalidate")
			w.Header().Set("Pragma", "public")

			err := bsky.SocialGraph.Write(w)
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
	err = handleRepoStreamWithRetry(ctx, bsky, log, u, &events.RepoStreamCallbacks{
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

func getHalfHourFileName(baseName string) string {
	now := time.Now()
	min := now.Minute()
	halfHourSuffix := "00"
	if min >= 30 {
		halfHourSuffix = "30"
	}

	fileExt := filepath.Ext(baseName)
	fileName := strings.TrimSuffix(baseName, fileExt)

	return fmt.Sprintf("%s-%s_%s%s", fileName, now.Format("2006_01_02_15"), halfHourSuffix, fileExt)
}

func saveGraph(
	ctx context.Context,
	log *zap.SugaredLogger,
	bsky *intEvents.BSky,
	binReaderWriter *graph.BinaryGraphReaderWriter,
	redisReaderWriter *graph.RedisReaderWriter,
	graphFileFromEnv string,
) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "SaveGraph")
	defer span.End()

	timestampedGraphFilePath := getHalfHourFileName(graphFileFromEnv)

	// Acquire a lock on the graph before we write it
	span.AddEvent("saveGraph:AcquireGraphLock")
	bsky.SocialGraphMux.RLock()
	span.AddEvent("saveGraph:GraphLockAcquired")
	// Copy the graph to a new memory buffer to save it without holding the lock
	newGraph := bsky.SocialGraph.DeepCopy()

	// Release the lock after we're done copying the graph
	span.AddEvent("saveGraph:ReleaseGraphLock")
	bsky.SocialGraphMux.RUnlock()

	log.Info("writing social graph to redis...")
	span.AddEvent("saveGraph:WriteGraphToRedis")
	err := redisReaderWriter.WriteGraph(*newGraph, "social-graph")
	if err != nil {
		log.Errorf("error writing social graph to redis: %s", err)
	}
	span.AddEvent("saveGraph:FinishedWritingGraphToRedis")

	logMsg := fmt.Sprintf("writing social graph to binary file, last updated: %s",
		bsky.SocialGraph.LastUpdate.Format("02.01.06 15:04:05"))

	log.Infow(logMsg,
		"graph_last_updated_at", newGraph.LastUpdate,
	)

	span.AddEvent("saveGraph:WriteGraphToBinaryFile")
	// Write the graph to a timestamped file
	err = binReaderWriter.WriteGraph(*newGraph, timestampedGraphFilePath)
	if err != nil {
		log.Errorf("error writing social graph to binary file: %s", err)
	}
	span.AddEvent("saveGraph:FinishedWritingGraphToBinaryFile")

	// Write the last seq number to a file
	span.AddEvent("saveGraph:WriteLastSeqNumber")
	err = ioutil.WriteFile(graphFileFromEnv+".seq", []byte(fmt.Sprintf("%d", bsky.LastSeq)), 0644)
	if err != nil {
		log.Errorf("error writing last seq number to file: %s", err)
	}

	// Copy the file to the "latest" path, we don't need to lock the graph for this
	span.AddEvent("saveGraph:CopyGraphFile")
	err = copyFile(timestampedGraphFilePath, graphFileFromEnv)
	if err != nil {
		log.Errorf("error copying binary file: %s", err)
	}
	span.AddEvent("saveGraph:FinishedCopyingGraphFile")

	log.Info("social graph written to binary file successfully")
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	return destinationFile.Sync()
}

func installExportPipeline(ctx context.Context) (func(context.Context) error, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	tracerProvider := newTraceProvider(exporter)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tracerProvider.Shutdown, nil
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("BSkyGraphBuilder"),
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
	callbacks *events.RepoStreamCallbacks,
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
					bsky.SocialGraphMux.RLock()
					if bsky.SocialGraph.LastUpdate.Add(updateCheckDuration).Before(time.Now()) {
						log.Infof("The graph hasn't been updated in the past %v seconds, exiting the graph builder (docker should restart it and get us in a good state)", updateCheckDuration)
						bsky.SocialGraphMux.RUnlock()
						os.Exit(1)
						cancel()
						c.Close()
						return
					}
					bsky.SocialGraphMux.RUnlock()
				case <-streamCtx.Done():
					return
				}
			}
		}()

		err = events.HandleRepoStream(streamCtx, c, callbacks)
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
		} else {
			return nil
		}
	}
}
