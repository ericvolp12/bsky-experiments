package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

type Index struct {
	PostRegistry *search.PostRegistry
	Detection    *objectdetection.ObjectDetectionImpl
	Sentiment    *sentiment.Sentiment
	Logger       *zap.SugaredLogger
	Store        *store.Store

	PositiveConfidenceThreshold float64
	NegativeConfidenceThreshold float64

	Limiter *rate.Limiter
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
			Value:   50,
			EnvVars: []string{"IMAGE_PAGE_SIZE"},
		},
		&cli.IntFlag{
			Name:    "actor-page-size",
			Usage:   "number of actors to index per page",
			Value:   50,
			EnvVars: []string{"ACTOR_PAGE_SIZE"},
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

	rawLogger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	logger := rawLogger.Sugar()

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
				logger.Errorf("failed to shutdown tracing: %+v", err)
			}
		}()
	}

	var postRegistry *search.PostRegistry
	if cctx.String("registry-postgres-url") != "" {
		postRegistry, err = search.NewPostRegistry(cctx.String("registry-postgres-url"))
		if err != nil {
			return fmt.Errorf("failed to initialize post registry: %w", err)
		}
		defer postRegistry.Close()
	}

	var st *store.Store
	if cctx.String("store-postgres-url") != "" {
		st, err = store.NewStore(cctx.String("store-postgres-url"))
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

	metricServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: mux,
	}

	shutdownMetrics := make(chan struct{})
	metricsShutdown := make(chan struct{})

	// Startup metrics server
	go func() {
		logger := logger.With("source", "metrics_server")

		logger.Info("metrics server listening on port %d", cctx.Int("port"))

		go func() {
			if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start metrics server: %+v\n", err)
			}
		}()
		<-shutdownMetrics
		if err := metricServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown metrics server: %+v\n", err)
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
		Limiter:                     rate.NewLimiter(rate.Limit(4), 1),
	}

	if st != nil {
		index.Store = st
	}
	if postRegistry != nil {
		index.PostRegistry = postRegistry
	}

	// Start the Image Indexing loop
	shutdownImages := make(chan struct{})
	imagesShutdown := make(chan struct{})
	go func() {
		logger := logger.With("source", "image_indexer")
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
		logger := logger.With("source", "sentiment_indexer")
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
		logger := logger.With("source", "new_sentiment_indexer")
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

	// Start the actor profile picture indexing loop
	shutdownProfilePictures := make(chan struct{})
	profilePicturesShutdown := make(chan struct{})
	go func() {
		logger := logger.With("source", "profile_picture_indexer")
		if index.Store == nil {
			logger.Info("no store, skipping profile picture indexing loop...")
			close(profilePicturesShutdown)
			return
		}
		logger.Info("Starting profile picture indexing loop...")
		for {
			ctx := context.Background()
			gofast := index.IndexActorProfilePictures(ctx, int32(cctx.Int("actor-page-size")))
			select {
			case <-shutdownProfilePictures:
				logger.Info("Shutting down profile picture indexing loop...")
				close(profilePicturesShutdown)
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

	log := index.Logger.With("source", "new_post_indexer")
	log.Info("new index loop waking up...")
	start := time.Now()
	log.Info("getting unindexed posts...")

	jobs, err := index.Store.Queries.GetUnprocessedSentimentJobs(ctx, pageSize)
	if err != nil {
		log.Error("failed to get unprocessed sentiment jobs: %+v", err)
		return false
	}

	postsIndexedCounter.WithLabelValues("store").Add(float64(len(jobs)))

	if len(jobs) == 0 {
		log.Info("no posts to index, sleeping...")
		return false
	}

	fetchDone := time.Now()

	log.Info("submtting posts for Sentiment Analysis...")

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
		log.Error("failed to get posts sentiment: %+v", err)
		return false
	}

	mlDone := time.Now()

	dbSentimentParams := []store_queries.SetSentimentForPostParams{}

	postsAnalyzed := 0

	for i := range jobs {
		job := jobs[i]
		var result *sentiment.SentimentPost
		for j := range sentimentResults {
			if sentimentResults[j].Rkey == job.Rkey && sentimentResults[j].ActorDID == job.ActorDid {
				result = sentimentResults[j]
				break
			}
		}

		var s sql.NullString
		var confidence sql.NullFloat64
		if result != nil && result.Decision != nil {
			postsAnalyzed++
			switch result.Decision.Sentiment {
			case sentiment.POSITIVE:
				s = sql.NullString{
					String: store.POSITIVE,
					Valid:  true,
				}
				positiveSentimentCounter.WithLabelValues("store").Inc()
			case sentiment.NEGATIVE:
				s = sql.NullString{
					String: store.NEGATIVE,
					Valid:  true,
				}
				negativeSentimentCounter.WithLabelValues("store").Inc()
			case sentiment.NEUTRAL:
				s = sql.NullString{
					String: store.NEUTRAL,
					Valid:  true,
				}
			}
			confidence = sql.NullFloat64{
				Float64: result.Decision.Confidence,
				Valid:   true,
			}
		}

		dbSentimentParams = append(dbSentimentParams, store_queries.SetSentimentForPostParams{
			ActorDid:    job.ActorDid,
			Rkey:        job.Rkey,
			CreatedAt:   job.CreatedAt.Time,
			ProcessedAt: sql.NullTime{Time: time.Now(), Valid: true},
			Sentiment:   s,
			Confidence:  confidence,
		})
	}

	span.AddEvent("SetSentimentForPost")

	log.Info("setting sentiment results...")
	sem := semaphore.NewWeighted(10)

	wg := sync.WaitGroup{}

	for i := range dbSentimentParams {
		p := dbSentimentParams[i]
		wg.Add(1)
		go func(param store_queries.SetSentimentForPostParams) {
			defer wg.Done()
			ctx := context.Background()
			sem.Acquire(ctx, 1)
			defer sem.Release(1)
			err := index.Store.Queries.SetSentimentForPost(ctx, param)
			if err != nil {
				log.Error("failed to set sentiment for post: %+v", err)
				return
			}
		}(p)
	}

	wg.Wait()

	updateDone := time.Now()

	span.SetAttributes(
		attribute.Int("posts_indexed", len(jobs)),
		attribute.Int("posts_analyzed", postsAnalyzed),
		attribute.Int("posts_labeled_with_sentiment", len(dbSentimentParams)),
		attribute.String("indexing_time", time.Since(start).String()),
		attribute.String("fetch_time", fetchDone.Sub(start).String()),
		attribute.String("analyze_time", mlDone.Sub(fetchDone).String()),
		attribute.String("update_time", updateDone.Sub(mlDone).String()),
	)

	log.Infow("finished indexing posts, sleeping...",
		"posts_indexed", len(jobs),
		"posts_analyzed", postsAnalyzed,
		"posts_labeled_with_sentiment", len(dbSentimentParams),
		"indexing_time", time.Since(start),
		"fetch_time", fetchDone.Sub(start),
		"analyze_time", mlDone.Sub(fetchDone),
		"update_time", updateDone.Sub(mlDone),
	)

	if len(jobs) < int(pageSize) {
		return false
	}

	return true
}

type ProfileRecords struct {
	Records []struct {
		URI   string `json:"uri"`
		Cid   string `json:"cid"`
		Value struct {
			Type   string `json:"$type"`
			Avatar struct {
				Type string `json:"$type"`
				Cid  string `json:"cid"`
				Ref  struct {
					Link string `json:"$link"`
				} `json:"ref"`
				MimeType string `json:"mimeType"`
				Size     int    `json:"size"`
			} `json:"avatar"`
			Banner struct {
				Type string `json:"$type"`
				Ref  struct {
					Link string `json:"$link"`
				} `json:"ref"`
				MimeType string `json:"mimeType"`
				Size     int    `json:"size"`
			} `json:"banner"`
			Description string `json:"description"`
			DisplayName string `json:"displayName"`
		} `json:"value"`
	} `json:"records"`
	Cursor string `json:"cursor"`
}

var profilePicturesIndexedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_indexed_profile_pictures",
	Help: "The total number of profile pictures indexed",
}, []string{})

var xrpcRequestsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_xrpc_requests",
	Help: "The total number of xrpc requests made",
}, []string{})

func (index *Index) IndexActorProfilePictures(ctx context.Context, pageSize int32) bool {
	ctx, span := tracer.Start(ctx, "IndexActorProfilePictures")
	defer span.End()

	log := index.Logger.With("source", "actor_profile_picture_indexer")
	log.Info("propic index loop waking up...")
	start := time.Now()
	log.Info("getting unindexed actors...")

	actors, err := index.Store.Queries.GetActorsWithoutPropic(ctx, pageSize)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Info("no actors to index, sleeping...")
			return false
		}

		log.Error("failed to get unindexed actors: %+v", err)
		return false
	}

	// GET https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.actor.profile
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	span.AddEvent("GetActorsProfilePictures")

	wg := sync.WaitGroup{}

	for i := range actors {
		actor := actors[i]
		wg.Add(1)
		go func(actor store_queries.Actor) {
			defer wg.Done()
			ctx := context.Background()
			req, err := http.NewRequest("GET", fmt.Sprintf("https://bsky.social/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.actor.profile", actor.Did), nil)
			if err != nil {
				log.Error("failed to create request for actor: %+v", err)
				return
			}

			index.Limiter.Wait(ctx)
			resp, err := client.Do(req.WithContext(ctx))
			xrpcRequestsCounter.WithLabelValues().Inc()
			if err != nil {
				log.Error("failed to get profile pictures for actor: %+v", err)
				return
			}
			defer resp.Body.Close()

			// Read the response body into a mirrorResponse
			profileResponse := ProfileRecords{}
			err = json.NewDecoder(resp.Body).Decode(&profileResponse)
			if err != nil {
				log.Error("failed to decode response body for actor: %+v", err)
				return
			}

			proPicCid := ""

			if len(profileResponse.Records) > 0 {
				// Set profile picture
				proPicCid = profileResponse.Records[0].Value.Avatar.Ref.Link
				if proPicCid == "" {
					proPicCid = profileResponse.Records[0].Value.Avatar.Cid
				}
			}

			err = index.Store.Queries.UpdateActorPropic(ctx, store_queries.UpdateActorPropicParams{
				Did:       actor.Did,
				ProPicCid: sql.NullString{Valid: true, String: proPicCid},
				UpdatedAt: sql.NullTime{Valid: true, Time: time.Now()},
			})
			if err != nil {
				log.Error("failed to set profile picture for actor: %+v", err)
				return
			}
		}(actor)
	}

	wg.Wait()

	profilePicturesIndexedCounter.WithLabelValues().Add(float64(len(actors)))

	span.SetAttributes(
		attribute.Int("actors_indexed", len(actors)),
		attribute.String("indexing_time", time.Since(start).String()),
	)

	log.Infow("finished indexing actors, sleeping...",
		"actors_indexed", len(actors),
		"indexing_time", time.Since(start),
	)

	if len(actors) < int(pageSize) {
		return false
	}

	return true
}

func (index *Index) IndexPostSentiments(ctx context.Context, pageSize int32) {
	ctx, span := tracer.Start(ctx, "IndexPostSentiments")
	defer span.End()

	log := index.Logger.With("source", "post_indexer")
	log.Info("index loop waking up...")
	start := time.Now()
	log.Info("getting unindexed posts...")
	posts, err := index.PostRegistry.GetUnindexedPostPage(ctx, pageSize, 0)
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
		log.Errorf("error getting sentiment for posts: %+v\n", err)
	}
	sentimentDone := time.Now()

	log.Info("setting sentiment results...")
	errs := index.PostRegistry.SetSentimentResults(ctx, sentimentResults)
	if len(errs) > 0 {
		log.Errorf("error(s) setting sentiment results: %+v\n", errs)
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

	log.Infof("setting %d sentiment labels...", len(labels))
	err = index.PostRegistry.AddOneLabelPerPost(ctx, labels, postIDsToLabel, authorDIDsToLabel)
	if err != nil {
		log.Errorf("error setting sentiment labels: %+v\n", err)
	}

	appliedSentimentDone := time.Now()

	// Set indexed at timestamp on posts
	postIds := make([]string, len(posts))
	for i, post := range posts {
		postIds[i] = post.ID
	}

	postsIndexedCounter.WithLabelValues("registry").Add(float64(len(postIds)))

	log.Infof("setting indexed at timestamp on %d posts...", len(postIds))
	err = index.PostRegistry.SetIndexedAtTimestamp(ctx, postIds, time.Now())
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

	log := index.Logger.With("source", "image_processor")
	log.Info("Processing images...")
	start := time.Now()

	unprocessedImages, err := index.PostRegistry.GetUnprocessedImages(ctx, pageSize)
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

	imagesIndexedCounter.Add(float64(len(unprocessedImages)))

	imageMetas := make([]*objectdetection.ImageMeta, len(unprocessedImages))
	for i, image := range unprocessedImages {
		imageMetas[i] = &objectdetection.ImageMeta{
			PostID:    image.PostID,
			ActorDID:  image.AuthorDID,
			CID:       image.CID,
			URL:       image.FullsizeURL,
			MimeType:  image.MimeType,
			CreatedAt: image.CreatedAt,
		}
	}

	results, err := index.Detection.ProcessImages(ctx, imageMetas)
	if err != nil {
		log.Errorf("Failed to process images: %v", err)
		return
	}

	executionTime := time.Now()

	successCount := 0

	for _, result := range results {
		if len(result.Results) > 0 {
			successCount++
		}

		cvClasses, err := json.Marshal(result.Results)
		if err != nil {
			log.Errorf("Failed to marshal classes: %v", err)
			continue
		}

		err = index.PostRegistry.AddCVDataToImage(
			ctx,
			result.Meta.CID,
			result.Meta.PostID,
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
			err = index.PostRegistry.AddPostLabel(ctx, result.Meta.PostID, result.Meta.ActorDID, postLabel)
			if err != nil {
				log.Errorf("Failed to add label to post: %v", err)
				continue
			}
		}
	}

	successfullyIndexedImagesCounter.Add(float64(successCount))
	failedToIndexImagesCounter.Add(float64(len(unprocessedImages) - successCount))

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
