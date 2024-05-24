package consumer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	graphdclient "github.com/ericvolp12/bsky-experiments/pkg/graphd/client"
	"github.com/ericvolp12/bsky-experiments/pkg/sharddb"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sqlc-dev/pqtype"
	typegen "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/time/rate"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Consumer is the consumer of the firehose
type Consumer struct {
	SocketURL string
	Progress  *Progress
	Logger    *zap.SugaredLogger

	RedisClient *redis.Client
	ProgressKey string

	Store *store.Store

	BackfillStatus *xsync.MapOf[string, *BackfillRepoStatus]
	SyncLimiter    *rate.Limiter

	magicHeaderKey string
	magicHeaderVal string

	graphdClient *graphdclient.Client
	shardDB      *sharddb.ShardDB

	tags      *TagTracker
	bitmapper *Bitmapper
}

// Progress is the cursor for the consumer
type Progress struct {
	LastSeq            int64     `json:"last_seq"`
	LastSeqProcessedAt time.Time `json:"last_seq_processed_at"`
	lk                 sync.RWMutex
}

func (p *Progress) Update(seq int64, processedAt time.Time) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.LastSeq = seq
	p.LastSeqProcessedAt = processedAt
}

func (p *Progress) Get() (int64, time.Time) {
	p.lk.RLock()
	defer p.lk.RUnlock()
	return p.LastSeq, p.LastSeqProcessedAt
}

type Delete struct {
	repo string
	path string
}

var tracer = otel.Tracer("consumer")

func (c *Consumer) Shutdown() error {
	return c.bitmapper.Shutdown()
}

// WriteCursor writes the cursor to redis
func (c *Consumer) WriteCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "WriteCursor")
	defer span.End()

	// Marshal the cursor JSON
	seq, processedAt := c.Progress.Get()
	p := Progress{
		LastSeq:            seq,
		LastSeqProcessedAt: processedAt,
	}
	data, err := json.Marshal(&p)
	if err != nil {
		return fmt.Errorf("failed to marshal cursor JSON: %+v", err)
	}

	// Write the cursor JSON to redis
	err = c.RedisClient.Set(ctx, c.ProgressKey, data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to write cursor to redis: %+v", err)
	}

	return nil
}

// ReadCursor reads the cursor from redis
func (c *Consumer) ReadCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "ReadCursor")
	defer span.End()

	// Read the cursor from redis
	data, err := c.RedisClient.Get(ctx, c.ProgressKey).Bytes()
	if err != nil {
		return fmt.Errorf("failed to read cursor from redis: %+v", err)
	}

	// Unmarshal the cursor JSON
	err = json.Unmarshal(data, c.Progress)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cursor JSON: %+v", err)
	}

	return nil
}

// NewConsumer creates a new consumer
func NewConsumer(
	ctx context.Context,
	logger *zap.SugaredLogger,
	redisClient *redis.Client,
	redisPrefix string,
	store *store.Store,
	socketURL string,
	magicHeaderKey string,
	magicHeaderVal string,
	graphdRoot string,
	shardDBNodes []string,
) (*Consumer, error) {
	h := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	var shardDB *sharddb.ShardDB
	var err error
	if len(shardDBNodes) > 0 {
		shardDB, err = sharddb.NewShardDB(ctx, shardDBNodes, slog.Default())
		if err != nil {
			return nil, fmt.Errorf("failed to create sharddb: %+v", err)
		}

		err := shardDB.CreatePostTable(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create post table: %+v", err)
		}
	}

	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
		},
		Logger:      logger,
		RedisClient: redisClient,
		ProgressKey: fmt.Sprintf("%s:progress", redisPrefix),
		Store:       store,

		BackfillStatus: xsync.NewMapOf[string, *BackfillRepoStatus](),
		SyncLimiter:    rate.NewLimiter(2, 1),

		magicHeaderKey: magicHeaderKey,
		magicHeaderVal: magicHeaderVal,

		shardDB: shardDB,
	}

	if graphdRoot != "" {
		c.graphdClient = graphdclient.NewClient(graphdRoot, &h)
	}

	if magicHeaderKey != "" && magicHeaderVal != "" {
		c.SyncLimiter = rate.NewLimiter(40, 1)
	}

	// Create the tag tracker
	tagTracker, err := NewTagTracker(redisClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create tag tracker: %+v", err)
	}

	c.tags = tagTracker

	// Create a Bitmapper
	bitmapper, err := NewBitmapper(store)
	if err != nil {
		return nil, fmt.Errorf("failed to create bitmapper: %+v", err)
	}

	c.bitmapper = bitmapper

	// Check to see if the cursor exists in redis
	err = c.ReadCursor(context.Background())
	if err != nil {
		if !strings.Contains(err.Error(), "redis: nil") {
			return nil, fmt.Errorf("failed to read cursor from redis: %+v", err)
		}
		logger.Warn("cursor not found in redis, starting from live")
	}

	pageSize := 500_000
	totalRecords := 0

	// Populate the backfill status from the database
	for {
		records, err := c.Store.Queries.GetRepoBackfillRecords(ctx, store_queries.GetRepoBackfillRecordsParams{
			Limit:  int32(pageSize),
			Offset: int32(totalRecords),
		})
		if err != nil {
			if err == sql.ErrNoRows {
				break
			}
			return nil, fmt.Errorf("failed to list repo backfill status: %+v", err)
		}

		for _, backfillRecord := range records {
			c.BackfillStatus.Store(backfillRecord.Repo, &BackfillRepoStatus{
				RepoDid:      backfillRecord.Repo,
				Seq:          backfillRecord.SeqStarted,
				State:        backfillRecord.State,
				DeleteBuffer: []*Delete{},
			})
			if backfillRecord.State == "enqueued" {
				backfillJobsEnqueued.WithLabelValues(c.SocketURL).Inc()
			}
		}

		totalRecords += len(records)

		if len(records) < pageSize {
			break
		}
	}

	logger.Infow("backfill records found", "count", totalRecords)

	// Start the backfill processor
	go c.BackfillProcessor(ctx)

	return &c, nil
}

// TrimRecentPosts trims the recent posts from the recent_posts table and the active posters from redis
func (c *Consumer) TrimRecentPosts(ctx context.Context, maxAge time.Duration) error {
	ctx, span := tracer.Start(ctx, "TrimRecentPosts")
	defer span.End()

	start := time.Now()

	span.SetAttributes(attribute.String("maxAge", maxAge.String()))

	// Trim the Recent Posts table in postgres
	numDeleted, err := c.Store.Queries.TrimOldRecentPosts(ctx, int32(maxAge.Hours()))
	if err != nil {
		return fmt.Errorf("failed to trim recent posts: %+v", err)
	}

	span.SetAttributes(attribute.Int64("num_deleted", numDeleted))

	postsTrimmed.WithLabelValues(c.SocketURL).Add(float64(numDeleted))

	// Trim the Active Posters sorted set in redis
	upperBound := fmt.Sprintf("(%d", time.Now().Add(-maxAge).UnixNano())
	numActiveTrimmed, err := c.RedisClient.ZRemRangeByScore(ctx, activePostersKey, "-inf", upperBound).Result()
	if err != nil {
		return fmt.Errorf("failed to trim active posters: %+v", err)
	}

	span.SetAttributes(attribute.Int64("num_active_posters_trimmed", numActiveTrimmed))

	c.Logger.Infow("trimmed recent posts", "num_deleted", numDeleted, "duration", time.Since(start).Seconds(), "num_active_posters_trimmed", numActiveTrimmed)

	return nil
}

// HandleStreamEvent handles a stream event from the firehose
func (c *Consumer) HandleStreamEvent(ctx context.Context, xe *events.XRPCStreamEvent) error {
	ctx, span := tracer.Start(ctx, "HandleStreamEvent")
	defer span.End()

	switch {
	case xe.RepoCommit != nil:
		eventsProcessedCounter.WithLabelValues("repo_commit", c.SocketURL).Inc()
		return c.HandleRepoCommit(ctx, xe.RepoCommit)
	case xe.RepoHandle != nil:
		eventsProcessedCounter.WithLabelValues("repo_handle", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoHandle.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoHandle.Time)
		if err != nil {
			log.Errorf("error parsing time: %+v", err)
			return nil
		}
		err = c.Store.Queries.UpsertActor(ctx, store_queries.UpsertActorParams{
			Did:       xe.RepoHandle.Did,
			Handle:    xe.RepoHandle.Handle,
			CreatedAt: sql.NullTime{Time: t, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to upsert actor: %+v", err)
		}
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoHandle.Seq))
	case xe.RepoInfo != nil:
		eventsProcessedCounter.WithLabelValues("repo_info", c.SocketURL).Inc()
	case xe.RepoMigrate != nil:
		eventsProcessedCounter.WithLabelValues("repo_migrate", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoHandle.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoMigrate.Time)
		if err != nil {
			log.Errorf("error parsing time: %+v", err)
			return nil
		}
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoHandle.Seq))
	case xe.RepoTombstone != nil:
		eventsProcessedCounter.WithLabelValues("repo_tombstone", c.SocketURL).Inc()
	case xe.LabelInfo != nil:
		eventsProcessedCounter.WithLabelValues("label_info", c.SocketURL).Inc()
	case xe.LabelLabels != nil:
		eventsProcessedCounter.WithLabelValues("label_labels", c.SocketURL).Inc()
	case xe.Error != nil:
		eventsProcessedCounter.WithLabelValues("error", c.SocketURL).Inc()
	}
	return nil
}

var knownCollections = map[string]struct{}{
	"app.bsky.actor.profile":   {},
	"app.bsky.feed.post":       {},
	"app.bsky.feed.repost":     {},
	"app.bsky.feed.like":       {},
	"app.bsky.feed.threadgate": {},
	"app.bsky.graph.list":      {},
	"app.bsky.graph.listitem":  {},
	"app.bsky.graph.follow":    {},
	"app.bsky.graph.listblock": {},
	"app.bsky.graph.block":     {},
	"app.bsky.feed.generator":  {},
}

// HandleRepoCommit handles a repo commit event from the firehose and processes the records
func (c *Consumer) HandleRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	ctx, span := tracer.Start(ctx, "HandleRepoCommit")
	defer span.End()

	processedAt := time.Now()

	c.Progress.Update(evt.Seq, processedAt)

	lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(evt.Seq))

	log := c.Logger.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit)

	backfill, ok := c.BackfillStatus.Load(evt.Repo)
	if !ok {
		span.SetAttributes(attribute.Bool("new_backfill_enqueued", true))
		log.Infof("backfill not in progress, adding repo %s to queue", evt.Repo)

		state := "enqueued"
		if evt.Since == nil {
			state = "complete"
		}

		backfill = &BackfillRepoStatus{
			RepoDid:      evt.Repo,
			Seq:          evt.Seq,
			State:        state,
			DeleteBuffer: []*Delete{},
		}

		c.BackfillStatus.Store(evt.Repo, backfill)

		err := c.Store.Queries.CreateRepoBackfillRecord(ctx, store_queries.CreateRepoBackfillRecordParams{
			Repo:         evt.Repo,
			LastBackfill: time.Now(),
			SeqStarted:   evt.Seq,
			State:        state,
		})
		if err != nil {
			log.Errorf("failed to create repo backfill record: %+v", err)
		}

		backfillJobsEnqueued.WithLabelValues(c.SocketURL).Inc()
	}

	if evt.TooBig {
		span.SetAttributes(attribute.Bool("too_big", true))
		log.Info("repo commit too big, skipping")
		tooBigEventsCounter.WithLabelValues(c.SocketURL).Inc()
		return nil
	}

	span.AddEvent("Read Repo From Car")
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.Errorf("failed to read repo from car: %+v", err)
		return nil
	}

	if evt.Rebase {
		log.Debug("rebase")
		rebasesProcessedCounter.WithLabelValues(c.SocketURL).Inc()
	}

	// Parse time from the event time string
	evtCreatedAt, err := time.Parse(time.RFC3339, evt.Time)
	if err != nil {
		log.Errorf("error parsing time: %+v", err)
		return nil
	}

	lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.UnixNano()))
	lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.UnixNano()))
	lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(evtCreatedAt).Seconds()))

	for _, op := range evt.Ops {
		collection := strings.Split(op.Path, "/")[0]
		rkey := strings.Split(op.Path, "/")[1]

		ek := repomgr.EventKind(op.Action)
		log = log.With("action", op.Action, "collection", collection)

		metricCollection := collection
		if _, ok := knownCollections[collection]; !ok {
			metricCollection = "unknown"
		}
		opsProcessedCounter.WithLabelValues(op.Action, metricCollection, c.SocketURL).Inc()

		// recordURI := "at://" + evt.Repo + "/" + op.Path
		span.SetAttributes(attribute.String("repo", evt.Repo))
		span.SetAttributes(attribute.String("collection", collection))
		span.SetAttributes(attribute.String("rkey", rkey))
		span.SetAttributes(attribute.Int64("seq", evt.Seq))
		span.SetAttributes(attribute.String("event_kind", op.Action))
		switch ek {
		case repomgr.EvtKindCreateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}
			// Grab the record from the merkel tree
			blk, err := rr.Blockstore().Get(ctx, cid.Cid(*op.Cid))
			if err != nil {
				e := fmt.Errorf("getting block %s within seq %d for %s: %w", *op.Cid, evt.Seq, evt.Repo, err)
				log.Errorf("failed to get a block from the event: %+v", e)
				break
			}

			rec, err := lexutil.CborDecodeValue(blk.RawData())
			if err != nil {
				log.Errorf("failed to decode cbor: %+v", err)
				break
			}
			recCreatedAt, err := c.HandleCreateRecord(ctx, evt.Repo, op.Path, rec)
			if err != nil {
				log.Errorf("failed to handle create record: %+v", err)
			}

			if recCreatedAt != nil && !recCreatedAt.IsZero() {
				lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(recCreatedAt.UnixNano()))
				lastEvtCreatedRecordCreatedGapGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.Sub(*recCreatedAt).Seconds()))
				lastRecordCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(*recCreatedAt).Seconds()))
			}
		case repomgr.EvtKindUpdateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}
			// Grab the record from the merkel tree
			blk, err := rr.Blockstore().Get(ctx, cid.Cid(*op.Cid))
			if err != nil {
				e := fmt.Errorf("getting block %s within seq %d for %s: %w", *op.Cid, evt.Seq, evt.Repo, err)
				log.Errorf("failed to get a block from the event: %+v", e)
				break
			}

			rec, err := lexutil.CborDecodeValue(blk.RawData())
			if err != nil {
				log.Errorf("failed to decode cbor: %+v", err)
				break
			}

			// Unpack the record and process it
			switch rec := rec.(type) {
			case *bsky.ActorProfile:
				// Process profile updates
				span.SetAttributes(attribute.String("record_type", "actor_profile"))
				recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()

				upsertParams := store_queries.UpsertActorFromFirehoseParams{
					Did:       evt.Repo,
					Handle:    "",
					UpdatedAt: sql.NullTime{Time: time.Now(), Valid: true},
					CreatedAt: sql.NullTime{Time: time.Now(), Valid: true},
				}

				if rec.DisplayName != nil && *rec.DisplayName != "" {
					upsertParams.DisplayName = sql.NullString{String: *rec.DisplayName, Valid: true}
				}

				if rec.Description != nil && *rec.Description != "" {
					upsertParams.Bio = sql.NullString{String: *rec.Description, Valid: true}
				}

				if rec.Avatar != nil {
					upsertParams.ProPicCid = sql.NullString{String: rec.Avatar.Ref.String(), Valid: true}
				}

				if rec.Banner != nil {
					upsertParams.BannerCid = sql.NullString{String: rec.Banner.Ref.String(), Valid: true}
				}

				err := c.Store.Queries.UpsertActorFromFirehose(ctx, upsertParams)
				if err != nil {
					log.Errorf("failed to upsert actor from firehose: %+v", err)
				}
			}
		case repomgr.EvtKindDeleteRecord:
			// Buffer the delete if a backfill is in progress
			backfill.lk.Lock()
			if backfill.State == "in_progress" || backfill.State == "enqueued" {
				log.Debugf("backfill scheduled for %s, buffering delete (%+v)", evt.Repo, op.Path)
				backfill.DeleteBuffer = append(backfill.DeleteBuffer, &Delete{
					repo: evt.Repo,
					path: op.Path,
				})
				backfill.lk.Unlock()
				backfillDeletesBuffered.WithLabelValues(c.SocketURL).Inc()
				return nil
			}
			backfill.lk.Unlock()

			err := c.HandleDeleteRecord(ctx, evt.Repo, op.Path)
			if err != nil {
				log.Errorf("failed to handle delete record: %+v", err)
			}
		default:
			log.Warnf("unknown event kind from op action: %+v", op.Action)
		}
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
}

// HandleDeleteRecord handles a delete record event from the firehose
func (c *Consumer) HandleDeleteRecord(
	ctx context.Context,
	repo string,
	path string,
) error {
	ctx, span := tracer.Start(ctx, "HandleDeleteRecord")
	collection := strings.Split(path, "/")[0]
	rkey := strings.Split(path, "/")[1]
	switch collection {
	case "app.bsky.feed.post":
		span.SetAttributes(attribute.String("record_type", "feed_post"))
		err := c.Store.Queries.DeletePost(ctx, store_queries.DeletePostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			log.Errorf("failed to delete post: %+v", err)
			// Don't return an error here, because we still want to try to delete the images
		}

		err = c.Store.Queries.DeleteRecentPost(ctx, store_queries.DeleteRecentPostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})

		// Delete sentiment for the post
		err = c.Store.Queries.DeleteSentimentJob(ctx, store_queries.DeleteSentimentJobParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			log.Errorf("failed to delete sentiment job: %+v", err)
			// Don't return an error here, because we still want to try to delete the images
		}

		// Delete images for the post
		err = c.Store.Queries.DeleteImagesForPost(ctx, store_queries.DeleteImagesForPostParams{
			PostActorDid: repo,
			PostRkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete images for post: %+v", err)
		}
	case "app.bsky.feed.like":
		span.SetAttributes(attribute.String("record_type", "feed_like"))
		// Get the like from the database to get the subject
		like, err := c.Store.Queries.GetLike(ctx, store_queries.GetLikeParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("like not found, so we can't delete it: %+v", err)
			}
			return fmt.Errorf("can't delete like: %+v", err)
		}

		// Delete the like from the database
		err = c.Store.Queries.DeleteLike(ctx, store_queries.DeleteLikeParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete like: %+v", err)
		}

		// Decrement the like count
		err = c.Store.Queries.DecrementLikeCountByN(ctx, store_queries.DecrementLikeCountByNParams{
			ActorDid:   like.SubjectActorDid,
			Collection: like.SubjectNamespace,
			Rkey:       like.SubjectRkey,
			NumLikes:   1,
		})
		if err != nil {
			return fmt.Errorf("failed to decrement like count: %+v", err)
		}
	case "app.bsky.feed.repost":
		span.SetAttributes(attribute.String("record_type", "feed_repost"))
		// Get the repost from the database to get the subject
		repost, err := c.Store.Queries.GetRepost(ctx, store_queries.GetRepostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("repost not found, so we can't delete it: %+v", err)
			}
			return fmt.Errorf("can't delete repost: %+v", err)
		}

		// Delete the repost from the database
		err = c.Store.Queries.DeleteRepost(ctx, store_queries.DeleteRepostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete repost: %+v", err)
		}

		// Decrement the repost count
		err = c.Store.Queries.DecrementRepostCountByN(ctx, store_queries.DecrementRepostCountByNParams{
			ActorDid:   repost.SubjectActorDid,
			Collection: repost.SubjectNamespace,
			Rkey:       repost.SubjectRkey,
			NumReposts: 1,
		})
		if err != nil {
			return fmt.Errorf("failed to decrement repost count: %+v", err)
		}
	case "app.bsky.graph.follow":
		span.SetAttributes(attribute.String("record_type", "graph_follow"))
		follow, err := c.Store.Queries.GetFollow(ctx, store_queries.GetFollowParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("follow not found, so we can't delete it: %+v", err)
			}
			return fmt.Errorf("can't delete follow: %+v", err)
		}

		if c.graphdClient != nil {
			err = c.graphdClient.Unfollow(ctx, repo, follow.TargetDid)
			if err != nil {
				log.Errorf("failed to propagate unfollow to GraphD: %+v", err)
			}
		}

		err = c.Store.Queries.DeleteFollow(ctx, store_queries.DeleteFollowParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete follow: %+v", err)
		}
		err = c.Store.Queries.DecrementFollowerCountByN(ctx, store_queries.DecrementFollowerCountByNParams{
			ActorDid:     follow.TargetDid,
			NumFollowers: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to decrement follower count: %+v", err)
			// Don't return an error here, because we still want to try to decrement the following count
		}
		err = c.Store.Queries.DecrementFollowingCountByN(ctx, store_queries.DecrementFollowingCountByNParams{
			ActorDid:     repo,
			NumFollowing: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to decrement following count: %+v", err)
		}

	case "app.bsky.graph.block":
		span.SetAttributes(attribute.String("record_type", "graph_block"))
		block, err := c.Store.Queries.GetBlock(ctx, store_queries.GetBlockParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("block not found, so we can't delete it: %+v", err)
			}
			return fmt.Errorf("can't delete block: %+v", err)
		}
		err = c.Store.Queries.DeleteBlock(ctx, store_queries.DeleteBlockParams{
			ActorDid: block.ActorDid,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete block: %+v", err)
		}

	}

	return nil
}

func (c *Consumer) FanoutWrite(
	ctx context.Context,
	repo string,
	path string,
) error {
	ctx, span := tracer.Start(ctx, "FanoutWrite")
	defer span.End()

	// Get followers of the repo
	// follows, err := c.Store.Queries.GetFollowsByTarget(ctx, store_queries.GetFollowsByTargetParams{
	// 	TargetDid: repo,
	// 	Limit:     100_000,
	// })
	// if err != nil {
	// 	return fmt.Errorf("failed to get followers: %+v", err)
	// }

	// Write to the timelines of all followers
	// pipeline := c.RedisClient.Pipeline()
	// for _, follow := range follows {
	// 	pipeline.ZAdd(ctx, fmt.Sprintf("wfo:%s:timeline", follow.ActorDid), redis.Z{
	// 		Score:  float64(time.Now().UnixNano()),
	// 		Member: fmt.Sprintf("at://%s/%s", repo, path),
	// 	})
	// 	// Randomly trim the timeline to 1000 records every 1/5th of the time
	// 	if rand.Intn(5) == 0 {
	// 		pipeline.ZRemRangeByRank(ctx, fmt.Sprintf("wfo:%s:timeline", follow.ActorDid), 0, -1000)
	// 	}
	// }

	// _, err = pipeline.Exec(ctx)
	// if err != nil {
	// 	return fmt.Errorf("failed to write to timelines: %+v", err)
	// }

	followCount, err := c.Store.Queries.GetFollowerCount(ctx, repo)
	if err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("failed to get follower count: %+v", err)
		}
	}

	// Observe the number of timelines we would have written to
	postsFannedOut.WithLabelValues(c.SocketURL).Inc()
	postFanoutHist.WithLabelValues(c.SocketURL).Observe(float64(followCount.NumFollowers))

	return nil
}

var activePostersKey = "consumer:active_posters"

// MarkPosterActive marks a poster as active in a redis sorted set
func (c *Consumer) MarkPosterActive(
	ctx context.Context,
	repo string,
	scoredAt time.Time,
) error {
	ctx, span := tracer.Start(ctx, "MarkPosterActive")
	defer span.End()

	// Add the poster to the active posters sorted set
	// only update scores of existing members if the new score is greater
	_, err := c.RedisClient.ZAddGT(ctx, activePostersKey, redis.Z{
		Score:  float64(scoredAt.UnixNano()),
		Member: repo,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to add poster to active posters: %+v", err)
	}

	return nil
}

// IntersectActivePosters returns the intersection of the active posters sorted set and the given set of posters
func (c *Consumer) IntersectActivePosters(
	ctx context.Context,
	dids []string,
) ([]string, error) {
	ctx, span := tracer.Start(ctx, "IntersectActivePosters")
	defer span.End()

	// Intersect the active posters sorted set and the given set of posters
	// to find the posters that are both active and in the given set

	zs := make([]redis.Z, len(dids))
	for i, did := range dids {
		zs[i] = redis.Z{
			Score:  1,
			Member: did,
		}
	}

	// Add the posters to a temporary set
	tempSetKey := fmt.Sprintf("consumer:active_posters:temp:%s", uuid.New().String())
	_, err := c.RedisClient.ZAdd(ctx, tempSetKey, zs...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to add posters to temp set: %+v", err)
	}

	// Intersect the temp set with the active posters set
	intersection, err := c.RedisClient.ZInter(ctx, &redis.ZStore{
		Keys: []string{tempSetKey, activePostersKey},
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to intersect temp set with active posters set: %+v", err)
	}

	// Delete the temp set
	_, err = c.RedisClient.Del(ctx, tempSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to delete temp set: %+v", err)
	}

	return intersection, nil
}

// HandleCreateRecord handles a create record event from the firehose
func (c *Consumer) HandleCreateRecord(
	ctx context.Context,
	repo string,
	path string,
	rec typegen.CBORMarshaler,
) (*time.Time, error) {
	ctx, span := tracer.Start(ctx, "HandleCreateRecord")

	// collection := strings.Split(path, "/")[0]
	rkey := strings.Split(path, "/")[1]

	var recCreatedAt time.Time
	var parseError error
	var err error

	indexedAt := time.Now()

	// Unpack the record and process it
	switch rec := rec.(type) {
	case *bsky.FeedPost:
		span.SetAttributes(attribute.String("record_type", "feed_post"))
		recordsProcessedCounter.WithLabelValues("feed_post", c.SocketURL).Inc()

		// Check if we've already processed this record
		_, err = c.Store.Queries.GetPost(ctx, store_queries.GetPostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get post: %w", err)
			}
		} else {
			// We've already processed this record, so skip it
			return nil, nil
		}

		quoteActorDid := ""
		quoteActorRkey := ""
		if rec.Embed != nil && rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			quoteRepostsProcessedCounter.WithLabelValues(c.SocketURL).Inc()
			u, err := GetURI(rec.Embed.EmbedRecord.Record.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Quoted Record uri: %w", err)
			}
			quoteActorDid = u.Did
			quoteActorRkey = u.RKey

		}

		parentActorDid := ""
		parentActorRkey := ""
		if rec.Reply != nil && rec.Reply.Parent != nil {
			u, err := GetURI(rec.Reply.Parent.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Reply uri: %w", err)
			}
			parentActorDid = u.Did
			parentActorRkey = u.RKey
		}

		rootActorDid := ""
		rootActorRkey := ""
		if rec.Reply != nil && rec.Reply.Root != nil {
			u, err := GetURI(rec.Reply.Root.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Root uri: %w", err)
			}
			rootActorDid = u.Did
			rootActorRkey = u.RKey
		}

		hasMedia := false
		if rec.Embed != nil {
			if rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0 {
				hasMedia = true
			} else if rec.Embed.EmbedRecordWithMedia != nil &&
				rec.Embed.EmbedRecordWithMedia.Media != nil &&
				rec.Embed.EmbedRecordWithMedia.Media.EmbedImages != nil &&
				len(rec.Embed.EmbedRecordWithMedia.Media.EmbedImages.Images) > 0 {
				hasMedia = true
			}
		}

		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		createParams := store_queries.CreatePostParams{
			ActorDid:           repo,
			Rkey:               rkey,
			Content:            sql.NullString{String: rec.Text, Valid: true},
			ParentPostActorDid: sql.NullString{String: parentActorDid, Valid: parentActorDid != ""},
			ParentPostRkey:     sql.NullString{String: parentActorRkey, Valid: parentActorRkey != ""},
			QuotePostActorDid:  sql.NullString{String: quoteActorDid, Valid: quoteActorDid != ""},
			QuotePostRkey:      sql.NullString{String: quoteActorRkey, Valid: quoteActorRkey != ""},
			RootPostActorDid:   sql.NullString{String: rootActorDid, Valid: rootActorDid != ""},
			RootPostRkey:       sql.NullString{String: rootActorRkey, Valid: rootActorRkey != ""},
			HasEmbeddedMedia:   hasMedia,
			CreatedAt:          sql.NullTime{Time: recCreatedAt, Valid: true},
			Langs:              rec.Langs,
			InsertedAt:         indexedAt,
		}

		if rec.Facets != nil {
			facetsJSON := []byte{}
			facetsJSON, err = json.Marshal(rec.Facets)
			if err != nil {
				log.Errorf("failed to marshal facets: %+v", err)
			} else if len(facetsJSON) > 0 {
				createParams.Facets = pqtype.NullRawMessage{RawMessage: facetsJSON, Valid: true}
			}

			for _, facet := range rec.Facets {
				if facet.Features != nil {
					for _, feature := range facet.Features {
						if feature.RichtextFacet_Tag != nil {
							createParams.Tags = append(createParams.Tags, feature.RichtextFacet_Tag.Tag)
						}
					}
				}
			}
		}

		if rec.Embed != nil && rec.Embed.EmbedExternal != nil {
			embedJSON := []byte{}
			embedJSON, err = json.Marshal(rec.Embed.EmbedExternal)
			if err != nil {
				log.Errorf("failed to marshal embed external: %+v", err)
			} else if len(embedJSON) > 0 {
				createParams.Embed = pqtype.NullRawMessage{RawMessage: embedJSON, Valid: true}
			}
		}

		if len(rec.Tags) > 0 {
			createParams.Tags = append(createParams.Tags, rec.Tags...)
		}

		// Create the post subject
		subj, err := c.Store.Queries.CreateSubject(ctx, store_queries.CreateSubjectParams{
			ActorDid: repo,
			Rkey:     rkey,
			Col:      1, // Maps to app.bsky.feed.post
		})
		if err != nil {
			log.Errorf("failed to create subject: %+v", err)
		}

		createParams.SubjectID = sql.NullInt64{Int64: subj.ID, Valid: true}

		err = c.Store.Queries.CreatePost(ctx, createParams)
		if err != nil {
			log.Errorf("failed to create post: %+v", err)
		}

		err = c.Store.Queries.CreateRecentPost(ctx, store_queries.CreateRecentPostParams{
			ActorDid:           createParams.ActorDid,
			Rkey:               createParams.Rkey,
			Content:            createParams.Content,
			ParentPostActorDid: createParams.ParentPostActorDid,
			ParentPostRkey:     createParams.ParentPostRkey,
			QuotePostActorDid:  createParams.QuotePostActorDid,
			QuotePostRkey:      createParams.QuotePostRkey,
			RootPostActorDid:   createParams.RootPostActorDid,
			RootPostRkey:       createParams.RootPostRkey,
			HasEmbeddedMedia:   createParams.HasEmbeddedMedia,
			Facets:             createParams.Facets,
			Embed:              createParams.Embed,
			Langs:              createParams.Langs,
			Tags:               createParams.Tags,
			CreatedAt:          createParams.CreatedAt,
			SubjectID:          sql.NullInt64{Int64: subj.ID, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create recent post: %+v", err)
		}

		earliestTS := time.Now()
		if recCreatedAt.Before(earliestTS) {
			earliestTS = recCreatedAt
		}

		err = c.MarkPosterActive(ctx, repo, earliestTS)
		if err != nil {
			log.Errorf("failed to mark poster active: %+v", err)
		}

		// Create a Sentiment Job for the post
		err = c.Store.Queries.CreateSentimentJob(ctx, store_queries.CreateSentimentJobParams{
			ActorDid:  repo,
			Rkey:      rkey,
			CreatedAt: recCreatedAt,
		})
		if err != nil {
			log.Errorf("failed to create sentiment job: %+v", err)
		}

		// Create images for the post
		if rec.Embed != nil && rec.Embed.EmbedImages != nil {
			for _, img := range rec.Embed.EmbedImages.Images {
				if img.Image == nil {
					continue
				}
				err = c.Store.Queries.CreateImage(ctx, store_queries.CreateImageParams{
					Cid:          img.Image.Ref.String(),
					PostActorDid: repo,
					PostRkey:     rkey,
					AltText:      sql.NullString{String: img.Alt, Valid: img.Alt != ""},
					CreatedAt:    sql.NullTime{Time: recCreatedAt, Valid: true},
				})
				if err != nil {
					log.Errorf("failed to create image: %+v", err)
				}
			}
		}

		// Initialize the like count
		err = c.Store.Queries.CreateLikeCount(ctx, store_queries.CreateLikeCountParams{
			SubjectID:        subj.ID,
			NumLikes:         0,
			UpdatedAt:        time.Now(),
			SubjectCreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create like count: %+v", err)
		}

		if c.shardDB != nil {
			bucket, err := sharddb.GetBucketFromRKey(rkey)
			if err != nil {
				log.Errorf("failed to get bucket from rkey %q: %+v", rkey, err)
				break
			}

			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Errorf("failed to marshal record for insertion to sharddb: %+v", err)
				break
			}

			// Create Post in ShardDB
			shardDBPost := sharddb.Post{
				ActorDID:  repo,
				Rkey:      rkey,
				IndexedAt: indexedAt,
				Bucket:    bucket,
				Raw:       recBytes,
				Langs:     rec.Langs,
				Tags:      rec.Tags,
				HasMedia:  hasMedia,
				IsReply:   rec.Reply != nil,
			}

			err = c.shardDB.InsertPost(ctx, shardDBPost)
			if err != nil {
				log.Errorf("failed to insert post into sharddb: %+v", err)
			}
		}

		// Increment the tag use counts
		if createParams.Tags != nil {
			err = c.tags.IncrementTagUseCounts(ctx, repo, createParams.Tags)
			if err != nil {
				log.Errorf("failed to increment tag use counts: %+v", err)
			}
		}
	case *bsky.FeedLike:
		span.SetAttributes(attribute.String("record_type", "feed_like"))
		recordsProcessedCounter.WithLabelValues("feed_like", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		var subjectURI *URI
		if rec.Subject != nil {
			subjectURI, err = GetURI(rec.Subject.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Subject uri: %w", err)
			}
		}

		if subjectURI == nil {
			return nil, fmt.Errorf("invalid like subject: %+v", rec.Subject)
		}

		// Check if we've already processed this record
		_, err = c.Store.Queries.GetLike(ctx, store_queries.GetLikeParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get like: %w", err)
			}
		} else {
			// We've already processed this record, so skip it
			return nil, nil
		}

		err = c.Store.Queries.CreateLike(ctx, store_queries.CreateLikeParams{
			ActorDid:        repo,
			Rkey:            rkey,
			SubjectActorDid: subjectURI.Did,
			Collection:      subjectURI.Collection,
			SubjectRkey:     subjectURI.RKey,
			CreatedAt:       sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create like: %w", err)
		}

		// Increment the like count
		err = c.Store.Queries.IncrementLikeCountByN(ctx, store_queries.IncrementLikeCountByNParams{
			ActorDid:   subjectURI.Did,
			Collection: subjectURI.Collection,
			Rkey:       subjectURI.RKey,
			NumLikes:   1,
		})
		if err != nil {
			log.Errorf("failed to increment like count: %+v", err)
		}

		// Track the user in the likers bitmap
		hourlyLikeBMKey := fmt.Sprintf("likes_hourly:%s", recCreatedAt.Format("2006_01_02_15"))

		err = c.bitmapper.AddMember(ctx, hourlyLikeBMKey, repo)
		if err != nil {
			log.Errorf("failed to add member to likers bitmap: %+v", err)
		}
	case *bsky.FeedRepost:
		span.SetAttributes(attribute.String("record_type", "feed_repost"))
		recordsProcessedCounter.WithLabelValues("feed_repost", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)
		var subjectURI *URI
		if rec.Subject != nil {
			subjectURI, err = GetURI(rec.Subject.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Subject uri: %w", err)
			}
		}

		if subjectURI == nil {
			return nil, fmt.Errorf("invalid repost subject: %+v", rec.Subject)
		}

		// Check if we've already processed this record
		_, err = c.Store.Queries.GetRepost(ctx, store_queries.GetRepostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get repost: %w", err)
			}
		} else {
			// We've already processed this record, so skip it
			return nil, nil
		}

		err = c.Store.Queries.CreateRepost(ctx, store_queries.CreateRepostParams{
			ActorDid:        repo,
			Rkey:            rkey,
			SubjectActorDid: subjectURI.Did,
			Collection:      subjectURI.Collection,
			SubjectRkey:     subjectURI.RKey,
			CreatedAt:       sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create repost: %w", err)
		}

		// Increment the repost count
		err = c.Store.Queries.IncrementRepostCountByN(ctx, store_queries.IncrementRepostCountByNParams{
			ActorDid:   subjectURI.Did,
			Collection: subjectURI.Collection,
			Rkey:       subjectURI.RKey,
			NumReposts: 1,
		})
		if err != nil {
			log.Errorf("failed to increment repost count: %+v", err)
		}

	case *bsky.GraphBlock:
		span.SetAttributes(attribute.String("record_type", "graph_block"))
		recordsProcessedCounter.WithLabelValues("graph_block", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		// Check if we've already processed this record
		_, err = c.Store.Queries.GetBlock(ctx, store_queries.GetBlockParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get block: %w", err)
			}
		} else {
			// We've already processed this record, so skip it
			return nil, nil
		}

		err = c.Store.Queries.CreateBlock(ctx, store_queries.CreateBlockParams{
			ActorDid:  repo,
			Rkey:      rkey,
			TargetDid: rec.Subject,
			CreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create block: %+v", err)
		}
	case *bsky.GraphFollow:
		span.SetAttributes(attribute.String("record_type", "graph_follow"))
		recordsProcessedCounter.WithLabelValues("graph_follow", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		// Check if we've already processed this record
		_, err = c.Store.Queries.GetFollow(ctx, store_queries.GetFollowParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get follow: %w", err)
			}
		} else {
			// We've already processed this record, so skip it
			return nil, nil
		}

		err = c.Store.Queries.CreateFollow(ctx, store_queries.CreateFollowParams{
			ActorDid:  repo,
			Rkey:      rkey,
			TargetDid: rec.Subject,
			CreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create follow: %+v", err)
		}
		err = c.Store.Queries.IncrementFollowerCountByN(ctx, store_queries.IncrementFollowerCountByNParams{
			ActorDid:     rec.Subject,
			NumFollowers: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to increment follower count: %+v", err)
		}
		err = c.Store.Queries.IncrementFollowingCountByN(ctx, store_queries.IncrementFollowingCountByNParams{
			ActorDid:     repo,
			NumFollowing: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to increment following count: %+v", err)
		}

		if c.graphdClient != nil {
			err = c.graphdClient.Follow(ctx, repo, rec.Subject)
			if err != nil {
				log.Errorf("failed to propagate follow to GraphD: %+v", err)
			}
		}
	case *bsky.ActorProfile:
		span.SetAttributes(attribute.String("record_type", "actor_profile"))
		recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()

		upsertParams := store_queries.UpsertActorFromFirehoseParams{
			Did:       repo,
			Handle:    "",
			UpdatedAt: sql.NullTime{Time: time.Now(), Valid: true},
			CreatedAt: sql.NullTime{Time: time.Now(), Valid: true},
		}

		if rec.DisplayName != nil && *rec.DisplayName != "" {
			upsertParams.DisplayName = sql.NullString{String: *rec.DisplayName, Valid: true}
		}

		if rec.Description != nil && *rec.Description != "" {
			upsertParams.Bio = sql.NullString{String: *rec.Description, Valid: true}
		}

		if rec.Avatar != nil {
			upsertParams.ProPicCid = sql.NullString{String: rec.Avatar.Ref.String(), Valid: true}
		}

		if rec.Banner != nil {
			upsertParams.BannerCid = sql.NullString{String: rec.Banner.Ref.String(), Valid: true}
		}

		err := c.Store.Queries.UpsertActorFromFirehose(ctx, upsertParams)
		if err != nil {
			log.Errorf("failed to upsert actor from firehose: %+v", err)
		}
	case *bsky.FeedGenerator:
		span.SetAttributes(attribute.String("record_type", "feed_generator"))
		recordsProcessedCounter.WithLabelValues("feed_generator", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)
	case *bsky.GraphList:
		span.SetAttributes(attribute.String("record_type", "graph_list"))
		recordsProcessedCounter.WithLabelValues("graph_list", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)
	case *bsky.GraphListitem:
		span.SetAttributes(attribute.String("record_type", "graph_listitem"))
		recordsProcessedCounter.WithLabelValues("graph_listitem", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)
	case *bsky.FeedThreadgate:
		span.SetAttributes(attribute.String("record_type", "feed_threadgate"))
		recordsProcessedCounter.WithLabelValues("feed_threadgate", c.SocketURL).Inc()
	case *bsky.GraphListblock:
		span.SetAttributes(attribute.String("record_type", "graph_listblock"))
		recordsProcessedCounter.WithLabelValues("graph_listblock", c.SocketURL).Inc()
	default:
		span.SetAttributes(attribute.String("record_type", "unknown"))
		log.Warnf("unknown record type: %+v", rec)
	}
	if parseError != nil {
		return nil, fmt.Errorf("error parsing time: %w", parseError)
	}

	return &recCreatedAt, nil
}
