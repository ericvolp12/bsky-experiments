package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	objectdetection "github.com/ericvolp12/bsky-experiments/pkg/object-detection"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/meilisearch/meilisearch-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

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

	meiliAddress := os.Getenv("MEILI_ADDRESS")
	if meiliAddress == "" {
		log.Fatal("MEILI_ADDRESS environment variable is required")
	}

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

	meiliClient := meilisearch.NewClient(meilisearch.ClientConfig{
		Host: meiliAddress,
	})

	// Start up a Metrics and Profiling goroutine
	go func() {
		log = log.With("source", "pprof_server")
		log.Info("starting pprof and prometheus server...")
		http.Handle("/metrics", promhttp.Handler())
		log.Info(http.ListenAndServe("0.0.0.0:8094", nil))
	}()

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
		log = log.With("source", "image_processor")
		defer wg.Done()
		for {
			log.Info("Processing images...")
			start := time.Now()
			unprocessedImages, err := postRegistry.GetUnprocessedImages(ctx, 50)
			if err != nil {
				if errors.As(err, &search.NotFoundError{}) {
					log.Info("No unprocessed images found, skipping process cycle...")

				} else {
					log.Errorf("Failed to get unprocessed images, skipping process cycle: %v", err)
				}
				time.Sleep(5 * time.Second)
				continue
			}

			if len(unprocessedImages) == 0 {
				log.Info("No unprocessed images found, skipping process cycle...")
				time.Sleep(5 * time.Second)
				continue
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

			results, err := detection.ProcessImages(ctx, imageMetas)
			if err != nil {
				log.Errorf("Failed to process images: %v", err)
				time.Sleep(5 * time.Second)
				continue
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

				err = postRegistry.AddCVDataToImage(
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
					err = postRegistry.AddPostLabel(ctx, unprocessedImages[idx].PostID, postLabel)
					if err != nil {
						log.Errorf("Failed to add label to post: %v", err)
						continue
					}
				}
			}

			log.Infow("Finished processing images...",
				"batch_size", len(unprocessedImages),
				"successfully_processed_image_count", successCount,
				"processing_time", time.Since(start),
			)
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
			select {
			case <-ctx.Done():
				log.Info("Context cancelled, exiting...")
				return
			default:
				time.Sleep(30 * time.Second)
			}
			log.Info("index loop waking up...")
			start := time.Now()
			log.Info("getting unindexed posts...")
			posts, err := postRegistry.GetUnindexedPostPage(ctx, 10000, 0)
			if err != nil {
				// Check if error is a not found error
				if errors.Is(err, search.PostsNotFound) {
					log.Info("no posts to index, sleeping...")
					continue
				}
				log.Errorf("error getting posts: %v", err)
				continue
			}

			fetchDone := time.Now()

			log.Info("submitting %d posts to meilisearch...", len(posts))

			ti, err := meiliClient.Index("posts").UpdateDocuments(posts, "id")
			if err != nil {
				log.Errorf("error indexing posts: %v", err)
				continue
			}

			indexDone := time.Now()

			log.Infof("...%d posts queued for index in meilisearch: %d", len(posts), ti.TaskUID)

			// Set indexed at timestamp on posts
			postIds := make([]string, len(posts))
			for i, post := range posts {
				postIds[i] = post.ID
			}

			log.Infof("setting indexed at timestamp on %d posts...", len(postIds))
			err = postRegistry.SetIndexedAtTimestamp(ctx, postIds, time.Now())
			if err != nil {
				log.Errorf("error setting indexed at timestamp: %v", err)
				continue
			}

			updateDone := time.Now()

			log.Infow("finished indexing posts, sleeping...",
				"posts_indexed", len(posts),
				"indexing_time", time.Since(start),
				"fetch_time", fetchDone.Sub(start),
				"index_time", indexDone.Sub(fetchDone),
				"update_time", updateDone.Sub(indexDone),
			)
		}
	}()

	// Wait for the loops to finish
	wg.Wait()

	log.Info("Exiting...")
}
