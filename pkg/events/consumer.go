package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type PostEvent struct {
	ctx      context.Context
	post     *appbsky.FeedPost
	repo     string
	rkey     string
	workerID int
}

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	Logger *zap.SugaredLogger

	SeqMux      sync.RWMutex
	LastUpdated time.Time
	LastSeq     int64 // LastSeq is the last sequence number processed

	redisClient    *redis.Client
	redisPrefix    string
	cursorKey      string
	lastUpdatedKey string

	PostQueue chan *PostEvent

	WorkerCount int
	Workers     []*Worker

	PostRegistryEnabled bool
	PostRegistry        *search.PostRegistry

	PLCMirrorRoot string
}

var tracer = otel.Tracer("graph-builder")

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(
	ctx context.Context,
	postRegistryEnabled bool,
	dbConnectionString, redisPrefix, plcMirrorRoot string,
	redisClient *redis.Client,
	workerCount int,
) (*BSky, error) {

	var postRegistry *search.PostRegistry
	var err error

	if postRegistryEnabled {
		postRegistry, err = search.NewPostRegistry(dbConnectionString)
		if err != nil {
			return nil, err
		}
	}

	rawlog, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("failed to create logger: %+v\n", err)
		return nil, err
	}
	log := rawlog.Sugar().With("source", "event_handler")

	bsky := &BSky{
		Logger: log,

		SeqMux:      sync.RWMutex{},
		LastUpdated: time.Now(),

		redisPrefix:    redisPrefix,
		redisClient:    redisClient,
		cursorKey:      redisPrefix + ":cursor",
		lastUpdatedKey: redisPrefix + ":last-updated",

		PostQueue: make(chan *PostEvent, 1_000),

		WorkerCount: workerCount,
		Workers:     make([]*Worker, workerCount),

		PostRegistryEnabled: postRegistryEnabled,
		PostRegistry:        postRegistry,

		PLCMirrorRoot: plcMirrorRoot,
	}

	// Initialize the workers, each with their own BSky Client and Mutex
	// Workers share a single WorkQueue and SocialGraph/Mutex
	for i := 0; i < workerCount; i++ {
		bsky.Workers[i] = &Worker{
			WorkerID: i,
		}

		go bsky.worker(ctx, i)
	}

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

var sem = semaphore.NewWeighted(100)

func (bsky *BSky) OnEvent(ctx context.Context, evt *models.Event) error {
	ctx, span := tracer.Start(ctx, "OnEvent")
	defer span.End()

	eventsSeen.Inc()

	bsky.SeqMux.Lock()
	bsky.LastUpdated = time.Now()
	bsky.LastSeq = evt.TimeUS
	bsky.SeqMux.Unlock()

	lastSeq.Set(float64(evt.TimeUS))

	log := bsky.Logger.With("repo", evt.Did, "time_us", evt.TimeUS)

	evtTime := time.UnixMicro(evt.TimeUS)
	lastSeqCreatedAt.Set(float64(evtTime.UnixNano()))
	lastSeqProcessedAt.Set(float64(time.Now().UnixNano()))

	err := bsky.SetCursor(ctx, evt.TimeUS)
	if err != nil {
		log.Errorf("failed to set cursor: %+v\n", err)
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
			log.Errorf("failed to unmarshal post: %+v\n", err)
			return nil
		}

		// Add the post to the queue
		bsky.PostQueue <- &PostEvent{
			ctx:  ctx,
			post: &post,
			repo: evt.Did,
			rkey: evt.Commit.RKey,
		}
		postsProcessedCounter.Inc()
	case "app.bsky.feed.like":
		var like appbsky.FeedLike
		err := json.Unmarshal(evt.Commit.Record, &like)
		if err != nil {
			log.Errorf("failed to unmarshal like: %+v\n", err)
			return nil
		}

		subjectURI, err := syntax.ParseATURI(like.Subject.Uri)
		if err != nil {
			log.Errorf("failed to parse subject URI: %+v\n", err)
			return nil
		}

		// Add the Like to the DB
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Errorf("failed to acquire semaphore: %+v\n", err)
			return nil
		}
		go func() {
			defer sem.Release(1)
			err = bsky.PostRegistry.AddLikeToPost(ctx, subjectURI.RecordKey().String(), evt.Did)
			if err != nil {
				log.Errorf("failed to add like to post: %+v\n", err)
			}
			likesProcessedCounter.Inc()
		}()
	case "app.bsky.graph.block":
		var block appbsky.GraphBlock
		err := json.Unmarshal(evt.Commit.Record, &block)
		if err != nil {
			log.Errorf("failed to unmarshal block: %+v\n", err)
			return nil
		}

		createdAt, err := syntax.ParseDatetimeLenient(block.CreatedAt)
		if err != nil {
			log.Errorf("failed to parse block created at: %+v\n", err)
			return nil
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			log.Errorf("failed to acquire semaphore: %+v\n", err)
			return nil
		}
		go func() {
			defer sem.Release(1)
			err = bsky.PostRegistry.AddAuthorBlock(ctx, evt.Did, block.Subject, createdAt.Time())
			if err != nil {
				log.Errorf("failed to add author block to registry: %+v\n", err)
			}
		}()
	}

	return nil

}
