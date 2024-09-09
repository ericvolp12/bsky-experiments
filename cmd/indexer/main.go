package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/ericvolp12/bsky-experiments/pkg/search"
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
	PostRegistry *search.PostRegistry
	Detection    *objectdetection.ObjectDetectionImpl
	Sentiment    *sentiment.Sentiment
	Logger       *slog.Logger
	Store        *store.Store

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
			Name:     "registry-postgres-url",
			Usage:    "postgres url for storing registry data",
			Required: false,
			EnvVars:  []string{"REGISTRY_POSTGRES_URL"},
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

	var postRegistry *search.PostRegistry
	if cctx.String("registry-postgres-url") != "" {
		postRegistry, err := search.NewPostRegistry(cctx.String("registry-postgres-url"))
		if err != nil {
			return fmt.Errorf("failed to initialize post registry: %w", err)
		}
		defer postRegistry.Close()
	}

	var s *store.Store
	if cctx.String("store-postgres-url") != "" {
		st, err := store.NewStore(cctx.String("store-postgres-url"))
		if err != nil {
			return fmt.Errorf("failed to initialize store: %w", err)
		}
		defer st.Close()
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
	if postRegistry != nil {
		index.PostRegistry = postRegistry
	}

	// Start the Image Indexing loop
	shutdownImages := make(chan struct{})
	imagesShutdown := make(chan struct{})
	go func() {
		logger := logger.With("component", "image_indexer")
		if index.PostRegistry == nil {
			logger.Info("no post registry, skipping image indexing loop...")
			close(imagesShutdown)
			return
		}

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

	// Start the Sentiment Indexing loop
	shutdownSentiments := make(chan struct{})
	sentimentsShutdown := make(chan struct{})
	go func() {
		logger := logger.With("component", "sentiment_indexer")
		if index.PostRegistry == nil {
			logger.Info("no post registry, skipping sentiment indexing loop...")
			close(sentimentsShutdown)
			return
		}

		logger.Info("Starting sentiment indexing loop...")
		for {
			ctx := context.Background()
			index.IndexPostSentiments(ctx, int32(cctx.Int("post-page-size")))
			select {
			case <-shutdownSentiments:
				logger.Info("Shutting down sentiment indexing loop...")
				close(sentimentsShutdown)
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Start the new post indexing loop
	shutdownNewSentiments := make(chan struct{})
	newSentimentsShutdown := make(chan struct{})
	go func() {
		logger := logger.With("component", "new_sentiment_indexer")
		if index.Store == nil {
			logger.Info("no store, skipping new sentiment indexing loop...")
			close(newSentimentsShutdown)
			return
		}
		logger.Info("Starting new sentiment indexing loop...")
		for {
			ctx := context.Background()
			gofast := index.IndexNewPostSentiments(ctx, int32(cctx.Int("post-page-size")))
			select {
			case <-shutdownNewSentiments:
				logger.Info("Shutting down new sentiment indexing loop...")
				close(newSentimentsShutdown)
				return
			default:
				if !gofast {
					time.Sleep(1 * time.Second)
				}
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
	close(shutdownSentiments)
	close(shutdownMetrics)
	close(shutdownNewSentiments)

	<-imagesShutdown
	<-sentimentsShutdown
	<-metricsShutdown
	<-newSentimentsShutdown

	logger.Info("shutdown complete")

	return nil
}

var postsAnalyzedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_analyzed_posts",
	Help: "The total number of posts analyzed",
}, []string{"type"})

var postsIndexedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_indexed_posts",
	Help: "The total number of posts indexed",
}, []string{"type"})

var positiveSentimentCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_positive_sentiment",
	Help: "The total number of posts with positive sentiment",
}, []string{"type"})

var negativeSentimentCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_negative_sentiment",
	Help: "The total number of posts with negative sentiment",
}, []string{"type"})

func (index *Index) IndexNewPostSentiments(ctx context.Context, pageSize int32) bool {
	ctx, span := tracer.Start(ctx, "IndexNewPostSentiments")
	defer span.End()

	logger := index.Logger.With("component", "new_post_indexer")
	logger.Info("new index loop waking up...")
	start := time.Now()
	logger.Info("getting unindexed posts...")

	jobs, err := index.Store.Queries.GetUnprocessedSentimentJobs(ctx, pageSize)
	if err != nil {
		logger.Error("failed to get unprocessed sentiment jobs", "error", err)
		return false
	}

	numJobs := len(jobs)

	postsIndexedCounter.WithLabelValues("store").Add(float64(numJobs))

	if numJobs == 0 {
		logger.Info("no posts to index, sleeping...")
		return false
	}

	fetchDone := time.Now()

	logger.Info("submtting posts for Sentiment Analysis...")

	sentimentPosts := []*sentiment.SentimentPost{}
	for i := range jobs {
		job := jobs[i]
		sentimentPosts = append(sentimentPosts, &sentiment.SentimentPost{
			Rkey:     job.Rkey,
			ActorDID: job.ActorDid,
			Text:     job.Content.String,
		})
	}

	span.AddEvent("GetPostsSentiment")

	sentimentResults, err := index.Sentiment.GetPostsSentiment(ctx, sentimentPosts)
	if err != nil {
		span.SetAttributes(attribute.String("sentiment.error", err.Error()))
		logger.Error("failed to get posts sentiment", "error", err)
		return false
	}

	mlDone := time.Now()

	dbSentimentParams := []store_queries.SetSentimentForPostParams{}

	postsAnalyzed := 0

	for i := range jobs {
		job := jobs[i]
		resIndex := -1
		for j := range sentimentResults {
			res := sentimentResults[j]
			if res.Rkey == job.Rkey && res.ActorDID == job.ActorDid {
				resIndex = j
				break
			}
		}

		params := store_queries.SetSentimentForPostParams{
			ActorDid:    job.ActorDid,
			Rkey:        job.Rkey,
			CreatedAt:   job.CreatedAt.Time,
			ProcessedAt: sql.NullTime{Time: mlDone, Valid: true},
		}

		if resIndex >= 0 {
			result := sentimentResults[resIndex]
			if result != nil {
				params.DetectedLangs = result.Langs
				if result.Decision != nil {
					postsAnalyzed++
					switch result.Decision.Sentiment {
					case sentiment.POSITIVE:
						params.Sentiment = sql.NullString{
							String: store.POSITIVE,
							Valid:  true,
						}
						positiveSentimentCounter.WithLabelValues("store").Inc()
					case sentiment.NEGATIVE:
						params.Sentiment = sql.NullString{
							String: store.NEGATIVE,
							Valid:  true,
						}
						negativeSentimentCounter.WithLabelValues("store").Inc()
					case sentiment.NEUTRAL:
						params.Sentiment = sql.NullString{
							String: store.NEUTRAL,
							Valid:  true,
						}
					}
					params.Confidence = sql.NullFloat64{
						Float64: result.Decision.Confidence,
						Valid:   true,
					}
				}
			}
		}

		dbSentimentParams = append(dbSentimentParams, params)
	}

	span.AddEvent("SetSentimentForPost")

	logger.Info("setting sentiment results...")
	sem := semaphore.NewWeighted(10)

	for i := range dbSentimentParams {
		sem.Acquire(ctx, 1)
		go func(i int) {
			defer sem.Release(1)
			err := index.Store.Queries.SetSentimentForPost(context.Background(), dbSentimentParams[i])
			if err != nil {
				logger.Error("failed to set sentiment for post", "error", err)
				return
			}
		}(i)
	}

	sem.Acquire(ctx, 10)

	updateDone := time.Now()

	span.SetAttributes(
		attribute.Int("posts_indexed", numJobs),
		attribute.Int("posts_analyzed", postsAnalyzed),
		attribute.Int("posts_labeled_with_sentiment", len(dbSentimentParams)),
		attribute.String("indexing_time", time.Since(start).String()),
		attribute.String("fetch_time", fetchDone.Sub(start).String()),
		attribute.String("analyze_time", mlDone.Sub(fetchDone).String()),
		attribute.String("update_time", updateDone.Sub(mlDone).String()),
	)

	logger.Info("finished indexing posts, sleeping...",
		"posts_indexed", numJobs,
		"posts_analyzed", postsAnalyzed,
		"posts_labeled_with_sentiment", len(dbSentimentParams),
		"indexing_time", time.Since(start),
		"fetch_time", fetchDone.Sub(start),
		"analyze_time", mlDone.Sub(fetchDone),
		"update_time", updateDone.Sub(mlDone),
	)

	if numJobs < int(pageSize) {
		return false
	}

	return true
}

func (index *Index) IndexPostSentiments(ctx context.Context, pageSize int32) {
	ctx, span := tracer.Start(ctx, "IndexPostSentiments")
	defer span.End()

	logger := index.Logger.With("component", "post_indexer")
	logger.Info("index loop waking up...")
	start := time.Now()
	logger.Info("getting unindexed posts...")
	posts, err := index.PostRegistry.GetUnindexedPostPage(ctx, pageSize, 0)
	if err != nil {
		// Check if error is a not found error
		if errors.Is(err, search.PostsNotFound) {
			logger.Info("no posts to index, sleeping...")
			return
		}
		logger.Error("error getting posts", "error", err)
		return
	}

	fetchDone := time.Now()

	logger.Info("submtting posts for Sentiment Analysis...")
	// If sentiment is enabled, get the sentiment for the post
	span.AddEvent("GetPostsSentiment")
	sentimentPosts := []*sentiment.SentimentPost{}
	for i := range posts {
		post := posts[i]
		sentimentPosts = append(sentimentPosts, &sentiment.SentimentPost{
			Rkey:     post.ID,
			ActorDID: post.AuthorDID,
			Text:     post.Text,
		})
	}
	sentimentResults, err := index.Sentiment.GetPostsSentiment(ctx, sentimentPosts)
	if err != nil {
		span.SetAttributes(attribute.String("sentiment.error", err.Error()))
		logger.Error("error getting sentiment for posts", "error", err)
	}
	sentimentDone := time.Now()

	logger.Info("setting sentiment results...")
	errs := index.PostRegistry.SetSentimentResults(ctx, sentimentResults)
	if len(errs) > 0 {
		logger.Error("error(s) setting sentiment results", "errors", errs)
	}

	postIDsToLabel := make([]string, 0)
	authorDIDsToLabel := make([]string, 0)
	labels := make([]string, 0)
	postsAnalyzed := 0

	for i := range sentimentResults {
		res := sentimentResults[i]
		if res != nil {
			if res.Decision != nil {
				postsAnalyzed++
				postsAnalyzedCounter.WithLabelValues("registry").Inc()
				if res.Decision.Sentiment == sentiment.POSITIVE && res.Decision.Confidence >= index.PositiveConfidenceThreshold {
					postIDsToLabel = append(postIDsToLabel, res.Rkey)
					authorDIDsToLabel = append(authorDIDsToLabel, res.ActorDID)
					labels = append(labels, "sentiment:pos")
					positiveSentimentCounter.WithLabelValues("registry").Inc()
				} else if res.Decision.Sentiment == sentiment.NEGATIVE && res.Decision.Confidence >= index.NegativeConfidenceThreshold {
					postIDsToLabel = append(postIDsToLabel, res.Rkey)
					authorDIDsToLabel = append(authorDIDsToLabel, res.ActorDID)
					labels = append(labels, "sentiment:neg")
					negativeSentimentCounter.WithLabelValues("registry").Inc()
				}
			}
		}
	}

	logger.Info("setting sentiment labels...", "num_labels", len(postIDsToLabel))
	err = index.PostRegistry.AddOneLabelPerPost(ctx, labels, postIDsToLabel, authorDIDsToLabel)
	if err != nil {
		logger.Error("error setting sentiment labels", "error", err)
	}

	appliedSentimentDone := time.Now()

	// Set indexed at timestamp on posts
	postIds := make([]string, len(posts))
	for i, post := range posts {
		postIds[i] = post.ID
	}

	postsIndexedCounter.WithLabelValues("registry").Add(float64(len(postIds)))

	logger.Info("setting indexed at timestamp...", "num_posts", len(postIds))
	err = index.PostRegistry.SetIndexedAtTimestamp(ctx, postIds, time.Now())
	if err != nil {
		logger.Error("error setting indexed at timestamp", "error", err)
		return
	}

	updateDone := time.Now()

	span.SetAttributes(
		attribute.Int("posts_indexed", len(posts)),
		attribute.Int("posts_analyzed", postsAnalyzed),
		attribute.Int("posts_labeled_with_sentiment", len(postIDsToLabel)),
		attribute.String("indexing_time", time.Since(start).String()),
		attribute.String("fetch_time", fetchDone.Sub(start).String()),
		attribute.String("index_time", appliedSentimentDone.Sub(fetchDone).String()),
		attribute.String("update_time", updateDone.Sub(appliedSentimentDone).String()),
		attribute.String("sentiment_time", sentimentDone.Sub(fetchDone).String()),
		attribute.String("sentiment_db_time", appliedSentimentDone.Sub(sentimentDone).String()),
	)

	logger.Info("finished indexing posts, sleeping...",
		"posts_indexed", len(posts),
		"posts_analyzed", postsAnalyzed,
		"posts_labeled_with_sentiment", len(postIDsToLabel),
		"indexing_time", time.Since(start),
		"fetch_time", fetchDone.Sub(start),
		"index_time", appliedSentimentDone.Sub(fetchDone),
		"update_time", updateDone.Sub(appliedSentimentDone),
		"sentiment_time", sentimentDone.Sub(fetchDone),
		"sentiment_db_time", appliedSentimentDone.Sub(sentimentDone),
	)
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

	unprocessedImages, err := index.PostRegistry.GetUnprocessedImages(ctx, pageSize)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			logger.Info("No unprocessed images found, skipping process cycle...")

		} else {
			logger.Error("Failed to get unprocessed images, skipping process cycle", "error", err)
		}
		return
	}

	if len(unprocessedImages) == 0 {
		logger.Info("No unprocessed images found, skipping process cycle...")
		return
	}

	imagesIndexedCounter.Add(float64(len(unprocessedImages)))

	imageMetas := make([]*objectdetection.ImageMeta, len(unprocessedImages))
	for i, image := range unprocessedImages {
		imageMetas[i] = &objectdetection.ImageMeta{
			PostID:    image.PostID,
			ActorDID:  image.AuthorDID,
			CID:       image.CID,
			URL:       fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", image.AuthorDID, image.CID),
			MimeType:  image.MimeType,
			CreatedAt: image.CreatedAt,
		}
	}

	results, err := index.Detection.ProcessImages(ctx, imageMetas)
	if err != nil {
		logger.Error("Failed to process images", "error", err)
		return
	}

	executionTime := time.Now()

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

			cvClasses, err := json.Marshal(result.Results)
			if err != nil {
				logger.Error("Failed to marshal classes", "error", err)
				return
			}

			err = index.PostRegistry.AddCVDataToImage(
				ctx,
				result.Meta.CID,
				result.Meta.PostID,
				executionTime,
				cvClasses,
			)
			if err != nil {
				logger.Error("Failed to update image", "error", err)
				return
			}

			imageLabels := []string{}
			for _, class := range result.Results {
				if class.Confidence >= 0.75 {
					imageLabels = append(imageLabels, class.Label)
				}
			}

			for _, label := range imageLabels {
				postLabel := fmt.Sprintf("%s:%s", "cv", label)
				err = index.PostRegistry.AddPostLabel(ctx, result.Meta.PostID, result.Meta.ActorDID, postLabel)
				if err != nil {
					logger.Error("Failed to add label to post", "error", err)
					continue
				}
			}
		}(result)
	}

	if err := sem.Acquire(ctx, 10); err != nil {
		logger.Error("Failed to acquire semaphore", "error", err)
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
