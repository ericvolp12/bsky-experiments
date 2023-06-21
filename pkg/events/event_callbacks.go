package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
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
	"github.com/ericvolp12/bsky-experiments/pkg/sentiment"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
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

	LastSeq int64 // LastSeq is the last sequence number processed

	// Generate a Profile Cache with a TTL
	profileCache    *lru.ARCCache[string, ProfileCacheEntry]
	profileCacheTTL time.Duration

	// Generate a Post Cache with a TTL
	postCache    *lru.ARCCache[string, PostCacheEntry]
	postCacheTTL time.Duration

	RepoRecordQueue chan RepoRecord

	WorkerCount int
	Workers     []*Worker

	PostRegistryEnabled bool
	PostRegistry        *search.PostRegistry

	SentimentAnalysisEnabled bool
	SentimentAnalysis        *sentiment.Sentiment
}

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(
	ctx context.Context,
	log *zap.SugaredLogger,
	includeLinks, postRegistryEnabled, sentimentAnalysisEnabled bool,
	dbConnectionString, sentimentServiceHost string,
	persistedGraph *persistedgraph.PersistedGraph,
	workerCount int,
) (*BSky, error) {
	postCache, err := lru.NewARC[string, PostCacheEntry](5000)
	if err != nil {
		return nil, err
	}

	profileCache, err := lru.NewARC[string, ProfileCacheEntry](15000)
	if err != nil {
		return nil, err
	}

	var postRegistry *search.PostRegistry

	if postRegistryEnabled {
		postRegistry, err = search.NewPostRegistry(dbConnectionString)
		if err != nil {
			return nil, err
		}
	}

	var sentimentAnalysis *sentiment.Sentiment

	if sentimentAnalysisEnabled {
		sentimentAnalysis = sentiment.NewSentiment(sentimentServiceHost)
	}

	log = log.With("source", "event_handler")

	bsky := &BSky{
		IncludeLinks: includeLinks,

		PersistedGraph: persistedGraph,

		Logger: log,

		profileCache: profileCache,
		postCache:    postCache,

		// 60 minute Cache TTLs
		profileCacheTTL: time.Minute * 60,
		postCacheTTL:    time.Minute * 60,

		RepoRecordQueue: make(chan RepoRecord, 100),
		WorkerCount:     workerCount,
		Workers:         make([]*Worker, workerCount),

		PostRegistryEnabled: postRegistryEnabled,
		PostRegistry:        postRegistry,

		SentimentAnalysisEnabled: sentimentAnalysisEnabled,
		SentimentAnalysis:        sentimentAnalysis,
	}

	// Initialize the workers, each with their own BSky Client and Mutex
	// Workers share a single WorkQueue and SocialGraph/Mutex
	for i := 0; i < workerCount; i++ {
		client, err := intXRPC.GetXRPCClient(ctx)
		if err != nil {
			return nil, err
		}

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

		log := rawlog.Sugar().With("worker_id", i)

		bsky.Workers[i] = &Worker{
			WorkerID:  i,
			Client:    client,
			ClientMux: sync.RWMutex{},
			Logger:    log,
		}

		go bsky.worker(i)
	}

	return bsky, nil
}

// HandleRepoCommit is called when a repo commit is received and prints its contents
func (bsky *BSky) HandleRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "HandleRepoCommit")
	defer span.End()

	log := bsky.Logger.With("repo", evt.Repo, "seq", evt.Seq)

	bsky.LastSeq = evt.Seq

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
			// Grab the record from the merkel tree
			rc, rec, err := rr.GetRecord(ctx, op.Path)
			if err != nil {
				e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
				log.Errorf("failed to get a record from the event: %+v\n", e)
				return nil
			}

			err = bsky.PersistedGraph.SetCursor(ctx, fmt.Sprintf("%d", evt.Seq))
			if err != nil {
				log.Errorf("failed to set cursor: %+v\n", err)
			}

			// Verify that the record cid matches the cid in the event
			if lexutil.LexLink(rc) != *op.Cid {
				e := fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
				log.Errorf("failed to LexLink the record in the event: %+v\n", e)
				return nil
			}

			recordAsCAR := lexutil.LexiconTypeDecoder{
				Val: rec,
			}

			// Attempt to Unpack the CAR Blocks into JSON Byte Array
			b, err := recordAsCAR.MarshalJSON()
			if err != nil {
				log.Errorf("failed to marshal record as CAR: %+v\n", err)
				return nil
			}

			// Parse time from the event time string
			t, err := time.Parse(time.RFC3339, evt.Time)
			if err != nil {
				log.Errorf("error parsing time: %+v", err)
				return nil
			}

			// If the record is a block, try to unmarshal it into a GraphBlock and log it
			if strings.HasPrefix(op.Path, "app.bsky.graph.block") {
				span.SetAttributes(attribute.String("repo.name", evt.Repo))
				span.SetAttributes(attribute.String("event.type", "app.bsky.graph.block"))
				// Unmarshal the JSON Byte Array into a Block
				var block = appbsky.GraphBlock{}
				err = json.Unmarshal(b, &block)
				if err != nil {
					log.Errorf("failed to unmarshal block into a GraphBlock: %+v\n", err)
					return nil
				}
				span.SetAttributes(attribute.String("block.subject", block.Subject))
				err = bsky.PostRegistry.AddAuthorBlock(ctx, evt.Repo, block.Subject, t)
				if err != nil {
					log.Errorf("failed to add author block to registry: %+v\n", err)
					return nil
				}
				log.Infow("processed graph block", "target", block.Subject, "source", evt.Repo)
				return nil
			}

			// If the record is a like, try to unmarshal it into a Like and stick it in the DB
			if strings.HasPrefix(op.Path, "app.bsky.feed.like") {
				span.SetAttributes(attribute.String("repo.name", evt.Repo))
				span.SetAttributes(attribute.String("event.type", "app.bsky.feed.like"))
				// Unmarshal the JSON Byte Array into a Like
				var like = appbsky.FeedLike{}
				err = json.Unmarshal(b, &like)
				if err != nil {
					log.Errorf("failed to unmarshal like into a Like: %+v\n", err)
					return nil
				}

				if like.Subject != nil {
					span.SetAttributes(attribute.String("like.subject.uri", like.Subject.Uri))

					_, postID := path.Split(like.Subject.Uri)
					span.SetAttributes(attribute.String("like.subject.post_id", postID))

					// Add the Like to the DB
					err := bsky.PostRegistry.AddLikeToPost(ctx, postID)
					if err != nil {
						log.Errorf("failed to add like to post: %+v\n", err)
						span.SetAttributes(attribute.String("error", err.Error()))
					}
					likesProcessedCounter.Inc()
				}
				return nil
			}

			// Unmarshal the JSON Byte Array into a FeedPost
			var pst = appbsky.FeedPost{}
			err = json.Unmarshal(b, &pst)
			if err != nil {
				log.Errorf("failed to unmarshal post into a FeedPost: %+v\n", err)
				return nil
			}

			// Add the RepoRecord to the Queue
			bsky.RepoRecordQueue <- RepoRecord{
				ctx:       ctx,
				pst:       pst,
				repoName:  evt.Repo,
				opPath:    op.Path,
				eventTime: evt.Time,
			}
		case repomgr.EvtKindDeleteRecord:
			deleteRecordsProcessed.Inc()
			span.SetAttributes(attribute.String("evt.kind", "delete"))
			span.SetAttributes(attribute.String("op.path", op.Path))
			return nil
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
