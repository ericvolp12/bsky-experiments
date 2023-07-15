package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	objectdetection "github.com/ericvolp12/bsky-experiments/pkg/object-detection"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/sentiment"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/meilisearch/meilisearch-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type Indexer struct {
	PostRegistry *search.PostRegistry
	MeiliClient  *meilisearch.Client
	Detection    *objectdetection.ObjectDetectionImpl
	Sentiment    *sentiment.Sentiment
	Logger       *zap.SugaredLogger

	PositiveConfidenceThreshold float64
	NegativeConfidenceThreshold float64
}

func main() {
	ctx := context.Background()
	var logger *zap.Logger

	if os.Getenv("DEBUG") == "true" {
		logger, _ = zap.NewDevelopment()
		logger.Info("Starting logger in DEBUG mode...")
	} else {
		logger, _ = zap.NewProduction()
		logger.Info("Starting logger in PRODUCTION mode...")
	}

	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := logger.Sugar()

	log.Info("Starting up BSky indexer...")

	log.Info("Reading config from environment...")

	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING environment variable is required")
	}

	// meiliAddress := os.Getenv("MEILI_ADDRESS")
	// if meiliAddress == "" {
	// 	log.Fatal("MEILI_ADDRESS environment variable is required")
	// }

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "BSkyIndexer", 1)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to create PostRegistry: %v", err)
	}
	defer postRegistry.Close()

	objectDetectionServiceHost := os.Getenv("OBJECT_DETECTION_SERVICE_HOST")
	if objectDetectionServiceHost == "" {
		log.Fatal("OBJECT_DETECTION_SERVICE_HOST environment variable is required")
	}

	detection := objectdetection.NewObjectDetection(objectDetectionServiceHost)

	// meiliClient := meilisearch.NewClient(meilisearch.ClientConfig{
	// 	Host: meiliAddress,
	// })

	sentimentServiceHost := os.Getenv("SENTIMENT_SERVICE_HOST")
	if sentimentServiceHost == "" {
		log.Fatal("SENTIMENT_SERVICE_HOST environment variable is required")
	}

	sentiment := sentiment.NewSentiment(sentimentServiceHost)

	// Start up a Metrics and Profiling goroutine
	go func() {
		log = log.With("source", "pprof_server")
		log.Info("starting pprof and prometheus server...")
		http.Handle("/metrics", promhttp.Handler())
		log.Info(http.ListenAndServe("0.0.0.0:8094", nil))
	}()

	indexer := &Indexer{
		PostRegistry: postRegistry,
		// MeiliClient:                 meiliClient,
		Detection:                   detection,
		Sentiment:                   sentiment,
		Logger:                      log,
		PositiveConfidenceThreshold: 0.65,
		NegativeConfidenceThreshold: 0.65,
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a goroutine to listen for SIGINT and SIGTERM signals
	go func() {
		log = log.With("source", "signal_handler")
		log.Info("starting signal handler...")
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Info("received shutdown signal, shutting down...")
		cancel()
	}()

	// Start the Image Processing loop
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			indexer.ProcessImages(ctx)
			select {
			case <-ctx.Done():
				log.Info("Context cancelled, exiting...")
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Start the MeiliSearch index loop
	wg.Add(1)
	go func() {
		log = log.With("source", "meilisearch_indexer")
		defer wg.Done()
		for {
			indexer.IndexPosts(ctx)
			select {
			case <-ctx.Done():
				log.Info("Context cancelled, exiting...")
				return
			default:
				time.Sleep(1 * time.Second)
			}

		}
	}()

	// Wait for the loops to finish
	wg.Wait()

	log.Info("Exiting...")
}

var postsAnalyzedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_analyzed_posts",
	Help: "The total number of posts analyzed",
})

var postsIndexedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_indexed_posts",
	Help: "The total number of posts indexed",
})

var positiveSentimentCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_positive_sentiment",
	Help: "The total number of posts with positive sentiment",
})

var negativeSentimentCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_negative_sentiment",
	Help: "The total number of posts with negative sentiment",
})

func (indexer *Indexer) IndexPosts(ctx context.Context) {
	tracer := otel.Tracer("MeilisearchIndexer")
	ctx, span := tracer.Start(ctx, "IndexPosts")
	defer span.End()

	log := indexer.Logger.With("source", "meilisearch_indexer")
	log.Info("index loop waking up...")
	start := time.Now()
	log.Info("getting unindexed posts...")
	posts, err := indexer.PostRegistry.GetUnindexedPostPage(ctx, 2000, 0)
	if err != nil {
		// Check if error is a not found error
		if errors.Is(err, search.PostsNotFound) {
			log.Info("no posts to index, sleeping...")
			return
		}
		log.Errorf("error getting posts: %v", err)
		return
	}

	fetchDone := time.Now()

	log.Info("submtting posts for Sentiment Analysis...")
	// If sentiment is enabled, get the sentiment for the post
	span.AddEvent("GetPostsSentiment")
	sentimentResults, err := indexer.Sentiment.GetPostsSentiment(ctx, posts)
	if err != nil {
		span.SetAttributes(attribute.String("sentiment.error", err.Error()))
		log.Errorf("error getting sentiment for posts: %+v\n", err)
	}
	sentimentDone := time.Now()

	log.Info("setting sentiment results...")
	errs := indexer.PostRegistry.SetSentimentResults(ctx, sentimentResults)
	if len(errs) > 0 {
		log.Errorf("error(s) setting sentiment results: %+v\n", errs)
	}

	postIDsToLabel := make([]string, 0)
	authorDIDsToLabel := make([]string, 0)
	labels := make([]string, 0)
	postsAnalyzed := 0

	for i := range sentimentResults {
		post := sentimentResults[i]
		if post != nil {
			if post.Sentiment != nil && post.SentimentConfidence != nil {
				postsAnalyzed++
				postsAnalyzedCounter.Inc()
				if strings.Contains(*post.Sentiment, "p") && *post.SentimentConfidence >= indexer.PositiveConfidenceThreshold {
					postIDsToLabel = append(postIDsToLabel, post.ID)
					authorDIDsToLabel = append(authorDIDsToLabel, post.AuthorDID)
					labels = append(labels, "sentiment:pos")
					positiveSentimentCounter.Inc()
				} else if strings.Contains(*post.Sentiment, "n") && *post.SentimentConfidence >= indexer.NegativeConfidenceThreshold {
					postIDsToLabel = append(postIDsToLabel, post.ID)
					authorDIDsToLabel = append(authorDIDsToLabel, post.AuthorDID)
					labels = append(labels, "sentiment:neg")
					negativeSentimentCounter.Inc()
				}
			}
		}
	}

	log.Infof("setting %d sentiment labels...", len(labels))
	err = indexer.PostRegistry.AddOneLabelPerPost(ctx, labels, postIDsToLabel, authorDIDsToLabel)
	if err != nil {
		log.Errorf("error setting sentiment labels: %+v\n", err)
	}

	appliedSentimentDone := time.Now()

	// Set indexed at timestamp on posts
	postIds := make([]string, len(posts))
	for i, post := range posts {
		postIds[i] = post.ID
	}

	postsIndexedCounter.Add(float64(len(postIds)))

	log.Infof("setting indexed at timestamp on %d posts...", len(postIds))
	err = indexer.PostRegistry.SetIndexedAtTimestamp(ctx, postIds, time.Now())
	if err != nil {
		log.Errorf("error setting indexed at timestamp: %v", err)
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

	log.Infow("finished indexing posts, sleeping...",
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

func (indexer *Indexer) ProcessImages(ctx context.Context) {
	tracer := otel.Tracer("ImageProcessor")
	ctx, span := tracer.Start(ctx, "ProcessImages")
	defer span.End()

	log := indexer.Logger.With("source", "image_processor")
	log.Info("Processing images...")
	start := time.Now()
	unprocessedImages, err := indexer.PostRegistry.GetUnprocessedImages(ctx, 50)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Info("No unprocessed images found, skipping process cycle...")

		} else {
			log.Errorf("Failed to get unprocessed images, skipping process cycle: %v", err)
		}
		return
	}

	if len(unprocessedImages) == 0 {
		log.Info("No unprocessed images found, skipping process cycle...")
		return
	}

	imageMetas := make([]*objectdetection.ImageMeta, len(unprocessedImages))
	for i, image := range unprocessedImages {
		imageMetas[i] = &objectdetection.ImageMeta{
			CID:       image.CID,
			URL:       image.FullsizeURL,
			MimeType:  image.MimeType,
			CreatedAt: image.CreatedAt,
		}
	}

	results, err := indexer.Detection.ProcessImages(ctx, imageMetas)
	if err != nil {
		log.Errorf("Failed to process images: %v", err)
		return
	}

	executionTime := time.Now()

	successCount := 0

	for idx, result := range results {
		if len(result.Results) > 0 {
			successCount++
		}

		cvClasses, err := json.Marshal(result.Results)
		if err != nil {
			log.Errorf("Failed to marshal classes: %v", err)
			continue
		}

		err = indexer.PostRegistry.AddCVDataToImage(
			ctx,
			result.Meta.CID,
			unprocessedImages[idx].PostID,
			executionTime,
			cvClasses,
		)
		if err != nil {
			log.Errorf("Failed to update image: %v", err)
			continue
		}

		imageLabels := []string{}
		for _, class := range result.Results {
			if class.Confidence >= 0.75 {
				imageLabels = append(imageLabels, class.Label)
			}
		}

		for _, label := range imageLabels {
			postLabel := fmt.Sprintf("%s:%s", "cv", label)
			err = indexer.PostRegistry.AddPostLabel(ctx, unprocessedImages[idx].PostID, unprocessedImages[idx].AuthorDID, postLabel)
			if err != nil {
				log.Errorf("Failed to add label to post: %v", err)
				continue
			}
		}
	}

	span.SetAttributes(
		attribute.Int("batch_size", len(unprocessedImages)),
		attribute.Int("successful_image_count", successCount),
		attribute.String("processing_time", time.Since(start).String()),
	)

	log.Infow("Finished processing images...",
		"batch_size", len(unprocessedImages),
		"successfully_processed_image_count", successCount,
		"processing_time", time.Since(start),
	)
}
