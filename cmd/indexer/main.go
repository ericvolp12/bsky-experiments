package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	objectdetection "github.com/ericvolp12/bsky-experiments/pkg/object-detection"
	"github.com/ericvolp12/bsky-experiments/pkg/sentiment"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"
)

type Index struct {
	Detection *objectdetection.ObjectDetectionImpl
	Sentiment *sentiment.Sentiment
	Logger    *slog.Logger
	Store     *store.Store

	PositiveConfidenceThreshold float64
	NegativeConfidenceThreshold float64
}

func main() {
	app := cli.App{
		Name:    "indexer",
		Usage:   "atproto post indexer",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.IntFlag{
			Name:    "post-page-size",
			Usage:   "number of posts to index per page",
			Value:   2000,
			EnvVars: []string{"POST_PAGE_SIZE"},
		},
		&cli.IntFlag{
			Name:    "image-page-size",
			Usage:   "number of images to index per page",
			Value:   60,
			EnvVars: []string{"IMAGE_PAGE_SIZE"},
		},
		&cli.IntFlag{
			Name:    "port",
			Usage:   "port to serve metrics on",
			Value:   8080,
			EnvVars: []string{"PORT"},
		},

		&cli.StringFlag{
			Name:     "store-postgres-url",
			Usage:    "postgres url for storing events",
			Required: false,
			EnvVars:  []string{"STORE_POSTGRES_URL"},
		},
		&cli.StringFlag{
			Name:    "object-detection-service-host",
			Usage:   "host for object detection service",
			Value:   "localhost:8081",
			EnvVars: []string{"OBJECT_DETECTION_SERVICE_HOST"},
		},
		&cli.StringFlag{
			Name:    "sentiment-service-host",
			Usage:   "host for sentiment service",
			Value:   "localhost:8082",
			EnvVars: []string{"SENTIMENT_SERVICE_HOST"},
		},
		&cli.Float64Flag{
			Name:    "positive-confidence-threshold",
			Usage:   "confidence threshold for positive sentiment",
			Value:   0.65,
			EnvVars: []string{"POSITIVE_CONFIDENCE_THRESHOLD"},
		},
		&cli.Float64Flag{
			Name:    "negative-confidence-threshold",
			Usage:   "confidence threshold for negative sentiment",
			Value:   0.65,
			EnvVars: []string{"NEGATIVE_CONFIDENCE_THRESHOLD"},
		},
	}

	app.Action = Indexer

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("indexer exited with error: %+v", err)
	}
}

var tracer = otel.Tracer("Indexer")

func Indexer(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Starting up BSky indexer...")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		logger.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(cctx.Context, "Indexer", 1)
		if err != nil {
			return fmt.Errorf("failed to initialize tracing: %w", err)
		}
		defer func() {
			if err := shutdown(cctx.Context); err != nil {
				logger.Error("failed to shutdown tracing", "error", err)
			}
		}()
	}

	var s *store.Store
	var err error
	if cctx.String("store-postgres-url") != "" {
		s, err = store.NewStore(cctx.String("store-postgres-url"))
		if err != nil {
			return fmt.Errorf("failed to initialize store: %w", err)
		}
		defer s.Close()
	}

	if cctx.String("object-detection-service-host") == "" {
		return fmt.Errorf("object detection service host is required")
	}

	detection := objectdetection.NewObjectDetection(cctx.String("object-detection-service-host"))

	if cctx.String("sentiment-service-host") == "" {
		return fmt.Errorf("sentiment service host is required")
	}

	sentiment := sentiment.NewSentiment(cctx.String("sentiment-service-host"))

	// Start up a Metrics and Profiling goroutine
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	metricServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: mux,
	}

	shutdownMetrics := make(chan struct{})
	metricsShutdown := make(chan struct{})

	// Startup metrics server
	go func() {
		logger := logger.With("component", "metrics_server")

		logger.Info("metrics server listening", "port", cctx.Int("port"))

		go func() {
			if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start metrics server", "error", err)
			}
		}()
		<-shutdownMetrics
		if err := metricServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown metrics server", "error", err)
		}
		close(metricsShutdown)

		logger.Info("metrics server shut down")
	}()

	index := &Index{
		Detection:                   detection,
		Sentiment:                   sentiment,
		PositiveConfidenceThreshold: cctx.Float64("positive-confidence-threshold"),
		NegativeConfidenceThreshold: cctx.Float64("negative-confidence-threshold"),
		Logger:                      logger,
	}

	if s != nil {
		index.Store = s
	}

	// Start the Image Indexing loop
	shutdownImages := make(chan struct{})
	imagesShutdown := make(chan struct{})
	go func() {
		logger := logger.With("component", "image_indexer")
		logger.Info("Starting image indexing loop...")
		for {
			ctx := context.Background()
			index.IndexImages(ctx, int32(cctx.Int("image-page-size")))
			select {
			case <-shutdownImages:
				logger.Info("Shutting down image indexing loop...")
				close(imagesShutdown)
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()

	select {
	case <-signals:
		cancel()
		logger.Info("shutting down on signal")
	case <-ctx.Done():
		logger.Info("shutting down on context done")
	}

	logger.Info("shutting down, waiting for workers to clean up...")

	close(shutdownImages)
	close(shutdownMetrics)

	<-imagesShutdown
	<-metricsShutdown

	logger.Info("shutdown complete")

	return nil
}

var imagesIndexedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_images_indexed_total",
	Help: "The total number of images indexed",
})

var successfullyIndexedImagesCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_images_indexed_successfully_total",
	Help: "The total number of images indexed successfully",
})

var failedToIndexImagesCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_images_indexed_failed_total",
	Help: "The total number of images failed to index",
})

func (index *Index) IndexImages(ctx context.Context, pageSize int32) {
	ctx, span := tracer.Start(ctx, "IndexImages")
	defer span.End()

	logger := index.Logger.With("component", "image_processor")
	logger.Info("Processing images...")
	start := time.Now()

	unprocessedImages, err := index.Store.Queries.ListImagesToProcess(ctx, pageSize)
	if err != nil {
		logger.Error("Failed to get unprocessed images", "error", err)
		return
	}

	if len(unprocessedImages) == 0 {
		logger.Info("No unprocessed images found, skipping process cycle...")
		return
	}

	imgMap := make(map[string]int64, len(unprocessedImages))
	imageIDs := make([]int64, len(unprocessedImages))
	for i, img := range unprocessedImages {
		imgMap[fmt.Sprintf("%s_%s_%s", img.PostActorDid, img.PostRkey, img.Cid)] = img.SubjectID
		imageIDs[i] = img.ID
	}

	imagesIndexedCounter.Add(float64(len(unprocessedImages)))

	imageMetas := make([]*objectdetection.ImageMeta, len(unprocessedImages))
	for i, image := range unprocessedImages {
		url := fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", image.PostActorDid, image.Cid)
		if image.IsVideo {
			url = fmt.Sprintf("https://video.bsky.app/watch/%s/%s/thumbnail.jpg", image.PostActorDid, image.Cid)
		}
		imageMetas[i] = &objectdetection.ImageMeta{
			PostID:   image.PostRkey,
			ActorDID: image.PostActorDid,
			CID:      image.Cid,
			URL:      url,
		}
	}

	results, err := index.Detection.ProcessImages(ctx, imageMetas)
	if err != nil {
		logger.Error("Failed to process images", "error", err)
		return
	}

	successCount := atomic.NewInt32(0)

	sem := semaphore.NewWeighted(10)
	for _, result := range results {
		if err := sem.Acquire(ctx, 1); err != nil {
			logger.Error("Failed to acquire semaphore", "error", err)
			break
		}
		go func(result *objectdetection.ImageResult) {
			defer sem.Release(1)

			if len(result.Results) > 0 {
				successCount.Inc()
			}

			imageLabels := []string{}
			for _, class := range result.Results {
				if class.Confidence >= 0.75 {
					imageLabels = append(imageLabels, class.Label)
				}
			}

			for _, label := range imageLabels {
				postLabel := fmt.Sprintf("%s:%s", "cv", label)

				// Get the SubjectID for the post
				subjectID, ok := imgMap[fmt.Sprintf("%s_%s_%s", result.Meta.ActorDID, result.Meta.PostID, result.Meta.CID)]
				if !ok {
					logger.Error("Failed to get SubjectID for image", "actor_did", result.Meta.ActorDID, "post_id", result.Meta.PostID, "cid", result.Meta.CID)
					continue
				}

				err = index.Store.Queries.CreateRecentPostLabel(ctx, store_queries.CreateRecentPostLabelParams{
					ActorDid:  result.Meta.ActorDID,
					Rkey:      result.Meta.PostID,
					Label:     postLabel,
					SubjectID: sql.NullInt64{Int64: subjectID, Valid: true},
				})
				if err != nil {
					logger.Error("Failed to create recent post label", "error", err, "actor_did", result.Meta.ActorDID, "rkey", result.Meta.PostID, "label", postLabel)
				}
			}
		}(result)
	}

	if err := sem.Acquire(ctx, 10); err != nil {
		logger.Error("Failed to acquire semaphore", "error", err)
	}

	// Dequeue the images
	err = index.Store.Queries.DequeueImages(ctx, imageIDs)
	if err != nil {
		logger.Error("Failed to dequeue images", "error", err)
	}

	successes := int(successCount.Load())

	successfullyIndexedImagesCounter.Add(float64(successes))
	failedToIndexImagesCounter.Add(float64(len(unprocessedImages) - successes))

	span.SetAttributes(
		attribute.Int("batch_size", len(unprocessedImages)),
		attribute.Int("successful_image_count", successes),
		attribute.String("processing_time", time.Since(start).String()),
	)

	logger.Info("Finished processing images...",
		"batch_size", len(unprocessedImages),
		"successfully_processed_image_count", successes,
		"processing_time", time.Since(start),
	)
}
