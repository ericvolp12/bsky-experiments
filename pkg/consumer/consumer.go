package consumer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
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
	"github.com/goccy/go-json"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
	"github.com/redis/go-redis/v9"
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
	ProgMux   sync.Mutex
	Logger    *zap.SugaredLogger

	RedisClient *redis.Client
	ProgressKey string

	Store *store.Store

	BackfillStatus map[string]*BackfillRepoStatus
	statusLock     sync.RWMutex
	SyncLimiter    *rate.Limiter
}

type BackfillRepoStatus struct {
	RepoDid     string
	Seq         int64
	State       string
	EventBuffer []*comatproto.SyncSubscribeRepos_Commit
	lk          sync.Mutex
}

// Progress is the cursor for the consumer
type Progress struct {
	LastSeq            int64     `json:"last_seq"`
	LastSeqProcessedAt time.Time `json:"last_seq_processed_at"`
}

var tracer = otel.Tracer("consumer")

// WriteCursor writes the cursor to redis
func (c *Consumer) WriteCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "WriteCursor")
	defer span.End()

	// Marshal the cursor JSON
	c.ProgMux.Lock()
	data, err := json.Marshal(c.Progress)
	c.ProgMux.Unlock()
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
	c.ProgMux.Lock()
	err = json.Unmarshal(data, c.Progress)
	c.ProgMux.Unlock()
	if err != nil {
		return fmt.Errorf("failed to unmarshal cursor JSON: %+v", err)
	}

	return nil
}

// NewConsumer creates a new consumer
func NewConsumer(ctx context.Context, logger *zap.SugaredLogger, redisClient *redis.Client, redisPrefix string, store *store.Store, socketURL string) (*Consumer, error) {
	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
		},
		Logger:      logger,
		ProgMux:     sync.Mutex{},
		RedisClient: redisClient,
		ProgressKey: fmt.Sprintf("%s:progress", redisPrefix),
		Store:       store,

		BackfillStatus: map[string]*BackfillRepoStatus{},
		SyncLimiter:    rate.NewLimiter(2, 1),
	}

	// Check to see if the cursor exists in redis
	err := c.ReadCursor(context.Background())
	if err != nil {
		if !strings.Contains(err.Error(), "redis: nil") {
			return nil, fmt.Errorf("failed to read cursor from redis: %+v", err)
		}
		logger.Warn("cursor not found in redis, starting from live")
	}

	// Populate the backfill status from the database
	backfillRecords, err := c.Store.Queries.GetRepoBackfillRecords(context.Background(), store_queries.GetRepoBackfillRecordsParams{
		Limit:  1000000,
		Offset: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list backfill records: %+v", err)
	}

	for _, backfillRecord := range backfillRecords {
		c.BackfillStatus[backfillRecord.Repo] = &BackfillRepoStatus{
			RepoDid:     backfillRecord.Repo,
			Seq:         backfillRecord.SeqStarted,
			State:       backfillRecord.State,
			EventBuffer: []*comatproto.SyncSubscribeRepos_Commit{},
		}
		if backfillRecord.State == "enqueued" {
			backfillJobsEnqueued.WithLabelValues(c.SocketURL).Inc()
		}
	}

	// Start the backfill processor
	go c.BackfillProcessor(ctx)

	return &c, nil
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
		c.ProgMux.Lock()
		c.Progress.LastSeq = xe.RepoHandle.Seq
		c.Progress.LastSeqProcessedAt = now
		c.ProgMux.Unlock()
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoHandle.Time)
		if err != nil {
			log.Errorf("error parsing time: %+v", err)
			return nil
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
		c.ProgMux.Lock()
		c.Progress.LastSeq = xe.RepoMigrate.Seq
		c.Progress.LastSeqProcessedAt = time.Now()
		c.ProgMux.Unlock()
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

// HandleRepoCommit handles a repo commit event from the firehose and processes the records
func (c *Consumer) HandleRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	ctx, span := tracer.Start(ctx, "HandleRepoCommit")
	defer span.End()

	processedAt := time.Now()

	c.ProgMux.Lock()
	c.Progress.LastSeq = evt.Seq
	c.Progress.LastSeqProcessedAt = processedAt
	c.ProgMux.Unlock()

	lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(evt.Seq))

	log := c.Logger.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit)

	c.statusLock.RLock()
	if _, ok := c.BackfillStatus[evt.Repo]; !ok {
		c.statusLock.RUnlock()
		c.statusLock.Lock()
		log.Infof("backfill not in progress, adding repo %s to queue", evt.Repo)
		c.BackfillStatus[evt.Repo] = &BackfillRepoStatus{
			RepoDid:     evt.Repo,
			Seq:         evt.Seq,
			State:       "enqueued",
			EventBuffer: []*comatproto.SyncSubscribeRepos_Commit{evt},
		}
		c.statusLock.Unlock()
		err := c.Store.Queries.CreateRepoBackfillRecord(ctx, store_queries.CreateRepoBackfillRecordParams{
			Repo:         evt.Repo,
			LastBackfill: time.Now(),
			SeqStarted:   evt.Seq,
			State:        "enqueued",
		})
		if err != nil {
			log.Errorf("failed to create repo backfill record: %+v", err)
		}
		backfillJobsEnqueued.WithLabelValues(c.SocketURL).Inc()
		return nil
	}
	backfill := c.BackfillStatus[evt.Repo]
	c.statusLock.RUnlock()

	backfill.lk.Lock()
	if backfill.State == "enqueued" || backfill.State == "in_progress" {
		log.Debugf("backfill scheduled for %s, buffering event", evt.Repo)
		backfill.EventBuffer = append(backfill.EventBuffer, evt)
		backfill.lk.Unlock()
		backfillEventsBuffered.WithLabelValues(c.SocketURL).Inc()
		return nil
	}
	backfill.lk.Unlock()

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

		opsProcessedCounter.WithLabelValues(op.Action, collection, c.SocketURL).Inc()

		// recordURI := "at://" + evt.Repo + "/" + op.Path
		span.SetAttributes(attribute.String("repo", evt.Repo))
		span.SetAttributes(attribute.String("collection", collection))
		span.SetAttributes(attribute.String("rkey", rkey))
		span.SetAttributes(attribute.Int64("seq", evt.Seq))
		span.SetAttributes(attribute.String("event_kind", op.Action))
		switch ek {
		case repomgr.EvtKindCreateRecord:
			// Grab the record from the merkel tree
			rc, rec, err := rr.GetRecord(ctx, op.Path)
			if err != nil {
				e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
				log.Errorf("failed to get a record from the event: %+v", e)
				break
			}

			// Verify that the record cid matches the cid in the event
			if lexutil.LexLink(rc) != *op.Cid {
				e := fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
				log.Errorf("failed to LexLink the record in the event: %+v", e)
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
			// For now we don't do anything with updates
		case repomgr.EvtKindDeleteRecord:
			recordType := strings.Split(op.Path, "/")[0]
			switch recordType {
			case "app.bsky.feed.post":
				span.SetAttributes(attribute.String("record_type", "feed_post"))
				err = c.Store.Queries.DeletePost(ctx, store_queries.DeletePostParams{
					ActorDid: evt.Repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to delete post: %+v", err)
				}
				// Delete images for the post
				err = c.Store.Queries.DeleteImagesForPost(ctx, store_queries.DeleteImagesForPostParams{
					PostActorDid: evt.Repo,
					PostRkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to delete images for post: %+v", err)
				}
			case "app.bsky.feed.like":
				span.SetAttributes(attribute.String("record_type", "feed_like"))
				// Get the like from the database to get the subject
				like, err := c.Store.Queries.GetLike(ctx, store_queries.GetLikeParams{
					ActorDid: evt.Repo,
					Rkey:     rkey,
				})
				if err != nil {
					if err == sql.ErrNoRows {
						log.Warnf("like not found, so we can't delete it: %+v", err)
						continue
					}
					log.Errorf("can't delete like: %+v", err)
					continue
				}

				// Delete the like from the database
				err = c.Store.Queries.DeleteLike(ctx, store_queries.DeleteLikeParams{
					ActorDid: evt.Repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to delete like: %+v", err)
					continue
				}

				// Decrement the like count
				err = c.Store.Queries.DecrementLikeCountByN(ctx, store_queries.DecrementLikeCountByNParams{
					ActorDid: like.SubjectActorDid,
					Ns:       like.SubjectNamespace,
					Rkey:     like.SubjectRkey,
					NumLikes: 1,
				})
				if err != nil {
					log.Warnf("failed to decrement like count: %+v", err)
				}
			case "app.bsky.graph.follow":
				span.SetAttributes(attribute.String("record_type", "graph_follow"))
				err = c.Store.Queries.DeleteFollow(ctx, store_queries.DeleteFollowParams{
					ActorDid: evt.Repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to delete follow: %+v", err)
				}
			case "app.bsky.graph.block":
				span.SetAttributes(attribute.String("record_type", "graph_block"))
				err = c.Store.Queries.DeleteBlock(ctx, store_queries.DeleteBlockParams{
					ActorDid: evt.Repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to delete block: %+v", err)
				}
			}
		default:
			log.Warnf("unknown event kind from op action: %+v", op.Action)
		}
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
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

	// Unpack the record and process it
	switch rec := rec.(type) {
	case *bsky.FeedPost:
		span.SetAttributes(attribute.String("record_type", "feed_post"))
		recordsProcessedCounter.WithLabelValues("feed_post", c.SocketURL).Inc()
		parentRelationship := ""
		parentActorDid := ""
		parentActorRkey := ""

		if rec.Embed != nil && rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			quoteRepostsProcessedCounter.WithLabelValues(c.SocketURL).Inc()
			u, err := getURI(rec.Embed.EmbedRecord.Record.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Quoted Record uri: %w", err)
			}
			parentActorDid = u.Did
			parentActorRkey = u.RKey
			parentRelationship = "q"
		}
		if rec.Reply != nil && rec.Reply.Parent != nil {
			u, err := getURI(rec.Reply.Parent.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Reply uri: %w", err)
			}
			parentActorDid = u.Did
			parentActorRkey = u.RKey
			parentRelationship = "r"
		}

		rootActorDid := ""
		rootActorRkey := ""
		if rec.Reply != nil && rec.Reply.Root != nil {
			u, err := getURI(rec.Reply.Root.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Root uri: %w", err)
			}
			rootActorDid = u.Did
			rootActorRkey = u.RKey
		}

		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		err = c.Store.Queries.CreatePost(ctx, store_queries.CreatePostParams{
			ActorDid:           repo,
			Rkey:               rkey,
			Content:            sql.NullString{String: rec.Text, Valid: true},
			ParentPostActorDid: sql.NullString{String: parentActorDid, Valid: parentActorDid != ""},
			ParentPostRkey:     sql.NullString{String: parentActorRkey, Valid: parentActorRkey != ""},
			ParentRelationship: sql.NullString{String: parentRelationship, Valid: parentRelationship != ""},
			RootPostActorDid:   sql.NullString{String: rootActorDid, Valid: rootActorDid != ""},
			RootPostRkey:       sql.NullString{String: rootActorRkey, Valid: rootActorRkey != ""},
			HasEmbeddedMedia:   rec.Embed != nil && rec.Embed.EmbedImages != nil,
			CreatedAt:          sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create post: %+v", err)
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

	case *bsky.FeedLike:
		span.SetAttributes(attribute.String("record_type", "feed_like"))
		recordsProcessedCounter.WithLabelValues("feed_like", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

		var subjectURI *URI
		if rec.Subject != nil {
			subjectURI, err = getURI(rec.Subject.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Subject uri: %w", err)
			}
		}

		if subjectURI == nil {
			return nil, fmt.Errorf("invalid like subject: %+v", rec.Subject)
		}

		err = c.Store.Queries.CreateLike(ctx, store_queries.CreateLikeParams{
			ActorDid:         repo,
			Rkey:             rkey,
			SubjectActorDid:  subjectURI.Did,
			SubjectNamespace: subjectURI.Namespace,
			SubjectRkey:      subjectURI.RKey,
			CreatedAt:        sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create like: %w", err)
		}

		// Increment the like count
		err = c.Store.Queries.IncrementLikeCountByN(ctx, store_queries.IncrementLikeCountByNParams{
			ActorDid: subjectURI.Did,
			Ns:       subjectURI.Namespace,
			Rkey:     subjectURI.RKey,
			NumLikes: 1,
		})

	case *bsky.FeedRepost:
		span.SetAttributes(attribute.String("record_type", "feed_repost"))
		recordsProcessedCounter.WithLabelValues("feed_repost", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)
	case *bsky.GraphBlock:
		span.SetAttributes(attribute.String("record_type", "graph_block"))
		recordsProcessedCounter.WithLabelValues("graph_block", c.SocketURL).Inc()
		recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

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

		err = c.Store.Queries.CreateFollow(ctx, store_queries.CreateFollowParams{
			ActorDid:  repo,
			Rkey:      rkey,
			TargetDid: rec.Subject,
			CreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create follow: %+v", err)
		}
	case *bsky.ActorProfile:
		span.SetAttributes(attribute.String("record_type", "actor_profile"))
		recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()
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
	default:
		span.SetAttributes(attribute.String("record_type", "unknown"))
		log.Warnf("unknown record type: %+v", rec)
	}
	if parseError != nil {
		return nil, fmt.Errorf("error parsing time: %w", parseError)
	}

	return &recCreatedAt, nil
}

func (c *Consumer) BackfillProcessor(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "BackfillProcessor")
	defer span.End()

	log := c.Logger.With("source", "backfill_main")
	log.Info("starting backfill processor")

	// Create a semaphore with a capacity of 50
	sem := make(chan struct{}, 50)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping backfill processor")
			return
		default:
		}

		// Get the next backfill
		var backfill *BackfillRepoStatus
		c.statusLock.RLock()
		for _, b := range c.BackfillStatus {
			b.lk.Lock()
			if b.State == "enqueued" {
				backfill = b
				b.State = "in_progress"
				b.lk.Unlock()
				break
			}
			b.lk.Unlock()
		}
		c.statusLock.RUnlock()

		if backfill == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		sem <- struct{}{} // Block until there is a slot in the semaphore
		go func(b *BackfillRepoStatus) {
			// Process the backfill
			c.ProcessBackfill(ctx, b.RepoDid)
			backfillJobsProcessed.WithLabelValues(c.SocketURL).Inc()
			<-sem // Release a slot in the semaphore when the goroutine finishes
		}(backfill)
	}
}

type RecordJob struct {
	RecordPath string
	NodeCid    cid.Cid
}

type RecordResult struct {
	RecordPath string
	Error      error
}

func (c *Consumer) ProcessBackfill(ctx context.Context, repoDID string) {
	ctx, span := tracer.Start(ctx, "ProcessBackfill")
	defer span.End()

	log := c.Logger.With("source", "backfill", "repo", repoDID)
	log.Infof("processing backfill for %s", repoDID)

	var url = "https://bsky.social/xrpc/com.atproto.sync.getCheckout?did=" + repoDID

	// GET and CAR decode the body
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   120 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Errorf("Error creating request: %v", err)
		return
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")

	c.SyncLimiter.Wait(ctx)

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Error sending request: %v", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Error response: %v", resp.StatusCode)
		return
	}

	r, err := repo.ReadRepoFromCar(ctx, resp.Body)
	if err != nil {
		log.Errorf("Error reading repo: %v", err)
		return
	}

	numRecords := 0
	numRoutines := 50
	recordJobs := make(chan RecordJob, numRoutines)
	recordResults := make(chan RecordResult, numRoutines)

	wg := sync.WaitGroup{}

	// Producer routine
	go func() {
		defer close(recordJobs)
		r.ForEach(ctx, "", func(recordPath string, nodeCid cid.Cid) error {
			numRecords++
			recordJobs <- RecordJob{RecordPath: recordPath, NodeCid: nodeCid}
			return nil
		})
	}()

	// Consumer routines
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range recordJobs {
				recordCid, rec, err := r.GetRecord(ctx, job.RecordPath)
				if err != nil {
					log.Errorf("Error getting record: %v", err)
					recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
					continue
				}

				// Verify that the record cid matches the cid in the event
				if recordCid != job.NodeCid {
					log.Errorf("mismatch in record and op cid: %s != %s", recordCid, job.NodeCid)
					recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
					continue
				}

				_, err = c.HandleCreateRecord(ctx, repoDID, job.RecordPath, rec)
				if err != nil {
					log.Errorf("failed to handle create record: %+v", err)
				}

				backfillRecordsProcessed.WithLabelValues(c.SocketURL).Inc()
				recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
			}
		}()
	}

	resultWG := sync.WaitGroup{}
	resultWG.Add(1)
	// Handle results
	go func() {
		defer resultWG.Done()
		for result := range recordResults {
			if result.Error != nil {
				log.Errorf("Error processing record %s: %v", result.RecordPath, result.Error)
			}
		}
	}()

	wg.Wait()
	close(recordResults)
	resultWG.Wait()

	log.Infof("processed %d records", numRecords)

	c.statusLock.RLock()
	bf := c.BackfillStatus[repoDID]
	c.statusLock.RUnlock()

	// Update the backfill record
	bf.lk.Lock()
	err = c.Store.Queries.UpdateRepoBackfillRecord(ctx, store_queries.UpdateRepoBackfillRecordParams{
		Repo:         repoDID,
		LastBackfill: time.Now(),
		SeqStarted:   bf.Seq,
		State:        "complete",
	})
	if err != nil {
		log.Errorf("failed to update repo backfill record: %+v", err)
	}

	// Update the backfill status
	bf.State = "complete"
	bf.lk.Unlock()

	bufferedEventsProcessed := 0
	log.Infof("processing %d buffered events", len(bf.EventBuffer))
	// Playback the buffered events
	for _, evt := range bf.EventBuffer {
		err = c.HandleRepoCommit(ctx, evt)
		if err != nil {
			log.Errorf("failed to handle repo commit: %+v", err)
		}
		backfillEventsBuffered.WithLabelValues(c.SocketURL).Dec()
		bufferedEventsProcessed++
	}

	log.Infof("processed %d buffered events", bufferedEventsProcessed)
}

type URI struct {
	Did       string
	RKey      string
	Namespace string
}

// URI: at://{did}/{namespace}/{rkey}
func getURI(uri string) (*URI, error) {
	trimmed := strings.TrimPrefix(uri, "at://")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid uri: %s", uri)
	}
	return &URI{
		Did:       parts[0],
		Namespace: parts[1],
		RKey:      parts[2],
	}, nil
}
