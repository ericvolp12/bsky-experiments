package events

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/api/label"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/ericvolp12/bsky-experiments/pkg/persistedgraph"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type RepoStreamCtxCallbacks struct {
	RepoCommit    func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error
	RepoHandle    func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Handle) error
	RepoInfo      func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Info) error
	RepoMigrate   func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Migrate) error
	RepoTombstone func(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Tombstone) error
	LabelLabels   func(ctx context.Context, evt *label.SubscribeLabels_Labels) error
	LabelInfo     func(ctx context.Context, evt *label.SubscribeLabels_Info) error
	Error         func(ctx context.Context, evt *events.ErrorFrame) error
}

// RepoRecord holds data needed for processing a RepoRecord
type RepoRecord struct {
	ctx       context.Context
	seq       int64
	pst       appbsky.FeedPost
	opPath    string
	repoName  string
	eventTime string
	workerID  int
}

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	IncludeLinks bool

	PersistedGraph *persistedgraph.PersistedGraph

	Logger *zap.SugaredLogger

	SeqMux      sync.RWMutex
	LastUpdated time.Time
	LastSeq     int64 // LastSeq is the last sequence number processed

	redisClient     *redis.Client
	cachesPrefix    string
	profileCacheTTL time.Duration
	postCacheTTL    time.Duration

	RepoRecordQueue chan RepoRecord

	// Rate Limiter for requests against the BSky API
	bskyLimiter      *rate.Limiter
	directoryLimiter *rate.Limiter

	WorkerCount int
	Workers     []*Worker

	PostRegistryEnabled bool
	PostRegistry        *search.PostRegistry
}

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(
	ctx context.Context,
	includeLinks, postRegistryEnabled bool,
	dbConnectionString string,
	persistedGraph *persistedgraph.PersistedGraph,
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
		IncludeLinks: includeLinks,

		PersistedGraph: persistedGraph,

		Logger: log,

		SeqMux:      sync.RWMutex{},
		LastUpdated: time.Now(),

		// 60 minute Cache TTLs
		cachesPrefix:    "graph_builder",
		redisClient:     redisClient,
		profileCacheTTL: time.Hour * 12,
		postCacheTTL:    time.Minute * 60,

		RepoRecordQueue:  make(chan RepoRecord, 1),
		bskyLimiter:      rate.NewLimiter(rate.Every(time.Millisecond*125), 1),
		directoryLimiter: rate.NewLimiter(rate.Every(time.Millisecond*125), 1),

		WorkerCount: workerCount,
		Workers:     make([]*Worker, workerCount),

		PostRegistryEnabled: postRegistryEnabled,
		PostRegistry:        postRegistry,
	}

	// Initialize the workers, each with their own BSky Client and Mutex
	// Workers share a single WorkQueue and SocialGraph/Mutex
	for i := 0; i < workerCount; i++ {
		client, err := intXRPC.GetXRPCClient(ctx)
		if err != nil {
			return nil, err
		}

		bsky.Workers[i] = &Worker{
			WorkerID:  i,
			Client:    client,
			ClientMux: sync.RWMutex{},
		}

		go bsky.worker(ctx, i)
	}

	return bsky, nil
}

// HandleRepoCommit is called when a repo commit is received and prints its contents
func (bsky *BSky) HandleRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "HandleRepoCommit")
	defer span.End()

	span.AddEvent("AcquireSeqLock")
	bsky.SeqMux.Lock()
	span.AddEvent("SeqLockAcquired")
	bsky.LastUpdated = time.Now()
	bsky.LastSeq = evt.Seq
	bsky.SeqMux.Unlock()
	span.AddEvent("ReleaseSeqLock")

	lastSeq.Set(float64(evt.Seq))

	log := bsky.Logger.With("repo", evt.Repo, "seq", evt.Seq)

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.Errorf("failed to read repo from car: %+v\n", err)
		return nil
	}
	if evt.Rebase {
		rebaseEventsProcessed.Inc()
		return nil
	}
	for _, op := range evt.Ops {
		ek := repomgr.EventKind(op.Action)
		switch ek {
		case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
			span.SetAttributes(attribute.String("op.path", op.Path))
			// Check if this record is modifying the user's profile
			if op.Path == "app.bsky.actor.profile/self" {
				log.Infof("found profile update for %s", evt.Repo)
				return nil
			}

			// Parse time from the event time string
			t, err := time.Parse(time.RFC3339, evt.Time)
			if err != nil {
				log.Errorf("error parsing time: %+v", err)
				return nil
			}

			lastSeqCreatedAt.Set(float64(t.UnixNano()))
			lastSeqProcessedAt.Set(float64(time.Now().UnixNano()))

			err = bsky.PersistedGraph.SetCursor(ctx, fmt.Sprintf("%d", evt.Seq))
			if err != nil {
				log.Errorf("failed to set cursor: %+v\n", err)
			}

			// Grab the record from the merkel tree
			rc, rec, err := rr.GetRecord(ctx, op.Path)
			if err != nil {
				e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
				log.Errorf("failed to get a record from the event: %+v\n", e)
				return nil
			}

			// Verify that the record cid matches the cid in the event
			if lexutil.LexLink(rc) != *op.Cid {
				e := fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
				log.Errorf("failed to LexLink the record in the event: %+v\n", e)
				return nil
			}

			// Unpack the record and process it
			switch rec := rec.(type) {
			case *appbsky.FeedPost:
				span.AddEvent("Adding to Queue")
				// Add the RepoRecord to the Queue
				bsky.RepoRecordQueue <- RepoRecord{
					ctx:       ctx,
					seq:       evt.Seq,
					pst:       *rec,
					repoName:  evt.Repo,
					opPath:    op.Path,
					eventTime: evt.Time,
				}
				span.AddEvent("Added to Queue")
			case *appbsky.FeedLike:
				span.SetAttributes(attribute.String("repo.name", evt.Repo))
				span.SetAttributes(attribute.String("event.type", "app.bsky.feed.like"))
				if rec.Subject != nil {
					span.SetAttributes(attribute.String("like.subject.uri", rec.Subject.Uri))

					_, postID := path.Split(rec.Subject.Uri)
					span.SetAttributes(attribute.String("like.subject.post_id", postID))

					// Add the Like to the DB
					err := bsky.PostRegistry.AddLikeToPost(ctx, postID, evt.Repo)
					if err != nil {
						log.Errorf("failed to add like to post: %+v\n", err)
						span.SetAttributes(attribute.String("error", err.Error()))
					}
					likesProcessedCounter.Inc()
				}
				return nil
			case *appbsky.FeedRepost:
				// Ignore reposts for now
			case *appbsky.GraphBlock:
				span.SetAttributes(attribute.String("repo.name", evt.Repo))
				span.SetAttributes(attribute.String("event.type", "app.bsky.graph.block"))
				span.SetAttributes(attribute.String("block.subject", rec.Subject))
				err = bsky.PostRegistry.AddAuthorBlock(ctx, evt.Repo, rec.Subject, t)
				if err != nil {
					log.Errorf("failed to add author block to registry: %+v\n", err)
					return nil
				}
				log.Infow("processed graph block", "target", rec.Subject, "source", evt.Repo)
				return nil

			case *appbsky.GraphFollow:
				// Ignore follows for now
			case *appbsky.ActorProfile:
				// Ignore profile updates for now
			case *appbsky.FeedGenerator:
				// Ignore feed generator updates for now
			case *appbsky.GraphList:
				// Ignore mute list creation for now
			case *appbsky.GraphListitem:
				// Ignore mute list updates for now
			default:
				log.Warnf("unknown record type: %+v", rec)
			}

		case repomgr.EvtKindDeleteRecord:
			deleteRecordsProcessed.Inc()
			span.SetAttributes(attribute.String("evt.kind", "delete"))
			span.SetAttributes(attribute.String("op.path", op.Path))
		}
	}
	return nil
}

func HandleRepoInfo(ctx context.Context, info *comatproto.SyncSubscribeRepos_Info) error {
	return nil
}

func HandleError(ctx context.Context, errf *events.ErrorFrame) error {
	return nil
}
