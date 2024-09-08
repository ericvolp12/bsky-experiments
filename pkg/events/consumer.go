package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/jetstream/pkg/client/schedulers/parallel"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	logger      *slog.Logger
	SeqMux      sync.RWMutex
	LastUpdated time.Time
	LastSeq     int64 // LastSeq is the last sequence number processed

	redisClient    *redis.Client
	redisPrefix    string
	cursorKey      string
	lastUpdatedKey string

	WorkerCount int
	Scheduler   *parallel.Scheduler

	PostRegistryEnabled bool
	PostRegistry        *search.PostRegistry
}

var tracer = otel.Tracer("graph-builder")

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(
	ctx context.Context,
	logger *slog.Logger,
	dbConnectionString, redisPrefix string,
	redisClient *redis.Client,
	workerCount int,
) (*BSky, error) {

	var postRegistry *search.PostRegistry
	var err error

	postRegistry, err = search.NewPostRegistry(dbConnectionString)
	if err != nil {
		return nil, err
	}

	logger = logger.With("source", "graph_builder_consumer")

	bsky := &BSky{
		logger:         logger,
		SeqMux:         sync.RWMutex{},
		LastUpdated:    time.Now(),
		redisPrefix:    redisPrefix,
		redisClient:    redisClient,
		cursorKey:      redisPrefix + ":cursor",
		lastUpdatedKey: redisPrefix + ":last-updated",
		WorkerCount:    workerCount,
		PostRegistry:   postRegistry,
	}

	scheduler := parallel.NewScheduler(workerCount, "graph-builder", logger, bsky.OnEvent)
	bsky.Scheduler = scheduler

	return bsky, nil
}

// SetCursor sets the cursor for the graph.
func (bsky *BSky) SetCursor(ctx context.Context, cursor int64) error {
	ctx, span := tracer.Start(ctx, "SetCursor")
	defer span.End()
	// Set the cursor
	cmd := bsky.redisClient.Set(ctx, bsky.cursorKey, cursor, 0)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting cursor in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	bsky.SeqMux.Lock()
	bsky.LastSeq = cursor
	bsky.LastUpdated = time.Now()
	bsky.redisClient.Set(ctx, bsky.lastUpdatedKey, bsky.LastUpdated, 0)
	bsky.SeqMux.Unlock()

	return nil
}

// GetCursor returns the cursor for the graph.
func (bsky *BSky) GetCursor(ctx context.Context) string {
	ctx, span := tracer.Start(ctx, "GetCursor")
	defer span.End()
	// Get the cursor
	cursor, err := bsky.redisClient.Get(ctx, bsky.cursorKey).Result()
	if err != nil {
		return ""
	}

	return cursor
}

func (bsky *BSky) OnEvent(ctx context.Context, evt *models.Event) error {
	ctx, span := tracer.Start(ctx, "OnEvent")
	defer span.End()

	eventsSeen.Inc()

	bsky.SeqMux.Lock()
	bsky.LastUpdated = time.Now()
	bsky.LastSeq = evt.TimeUS
	bsky.SeqMux.Unlock()

	lastSeq.Set(float64(evt.TimeUS))

	logger := bsky.logger.With("repo", evt.Did, "time_us", evt.TimeUS)

	evtTime := time.UnixMicro(evt.TimeUS)
	lastSeqCreatedAt.Set(float64(evtTime.UnixNano()))
	lastSeqProcessedAt.Set(float64(time.Now().UnixNano()))

	err := bsky.SetCursor(ctx, evt.TimeUS)
	if err != nil {
		logger.Error("failed to set cursor", "error", err)
	}

	if evt.Commit == nil {
		return nil
	}

	switch evt.Commit.OpType {
	case models.CommitDeleteRecord:
		deleteRecordsProcessed.Inc()
		return nil
	case models.CommitUpdateRecord:
		return nil
	}

	switch evt.Commit.Collection {
	case "app.bsky.feed.post":
		var post appbsky.FeedPost
		err := json.Unmarshal(evt.Commit.Record, &post)
		if err != nil {
			logger.Error("failed to unmarshal post", "error", err)
			return nil
		}

		// Process the post
		err = bsky.ProcessPost(ctx, evt.Did, evt.Commit.RKey, &post)
		if err != nil {
			logger.Error("failed to process post", "error", err, "repo", evt.Did, "rkey", evt.Commit.RKey)
		}
	case "app.bsky.feed.like":
		var like appbsky.FeedLike
		err := json.Unmarshal(evt.Commit.Record, &like)
		if err != nil {
			logger.Error("failed to unmarshal like", "error", err)
			return nil
		}

		subjectURI, err := syntax.ParseATURI(like.Subject.Uri)
		if err != nil {
			logger.Error("failed to parse like subject URI", "error", err)
			return nil
		}

		err = bsky.PostRegistry.AddLikeToPost(ctx, subjectURI.RecordKey().String(), evt.Did)
		if err != nil {
			logger.Error("failed to add like to post", "error", err)
		}
		likesProcessedCounter.Inc()
	case "app.bsky.graph.block":
		var block appbsky.GraphBlock
		err := json.Unmarshal(evt.Commit.Record, &block)
		if err != nil {
			logger.Error("failed to unmarshal block", "error", err)
			return nil
		}

		createdAt, err := syntax.ParseDatetimeLenient(block.CreatedAt)
		if err != nil {
			logger.Error("failed to parse block created at", "error", err)
			return nil
		}

		err = bsky.PostRegistry.AddAuthorBlock(ctx, evt.Did, block.Subject, createdAt.Time())
		if err != nil {
			logger.Error("failed to add author block", "error", err)
		}
	}

	return nil
}
