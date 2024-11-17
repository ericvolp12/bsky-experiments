package consumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	_ "github.com/bluesky-social/indigo/api/chat" // Register chat types
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	graphdclient "github.com/ericvolp12/bsky-experiments/pkg/graphd/client"
	"github.com/ericvolp12/bsky-experiments/pkg/sharddb"
	"github.com/goccy/go-json"
	"github.com/labstack/gommon/log"
	"github.com/redis/go-redis/v9"

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

	graphdClient *graphdclient.Client
	shardDB      *sharddb.ShardDB

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
	// return c.bitmapper.Shutdown()
	return nil
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
	graphdRoot string,
	shardDBNodes []string,
) (*Consumer, error) {
	// h := http.Client{
	// 	Transport: otelhttp.NewTransport(&http.Transport{
	// 		MaxConnsPerHost:     100,
	// 		MaxIdleConnsPerHost: 100,
	// 	}),
	// 	Timeout: time.Millisecond * 250,
	// }

	var shardDB *sharddb.ShardDB
	var err error
	// if len(shardDBNodes) > 0 {
	// 	shardDB, err = sharddb.NewShardDB(ctx, shardDBNodes, slog.Default())
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to create sharddb: %+v", err)
	// 	}

	// 	err := shardDB.CreatePostTable(ctx)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to create post table: %+v", err)
	// 	}
	// }

	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
		},
		Logger:      logger,
		RedisClient: redisClient,
		ProgressKey: fmt.Sprintf("%s:progress", redisPrefix),
		Store:       store,

		shardDB: shardDB,
	}

	// if graphdRoot != "" {
	// 	c.graphdClient = graphdclient.NewClient(graphdRoot, &h)
	// }

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

	// Trim the label feeds
	oldestRkey := syntax.NewTIDFromTime(time.Now().Add(-maxAge*2), 0).String()
	err = c.Store.Queries.TrimMPLS(ctx, oldestRkey)
	if err != nil {
		c.Logger.Error("failed to trim MPLS feed", "error", err)
	}
	err = c.Store.Queries.TrimTQSP(ctx, oldestRkey)
	if err != nil {
		c.Logger.Error("failed to trim TQSP feed", "error", err)
	}
	err = c.Store.Queries.TrimRecentPostLabels(ctx, oldestRkey)
	if err != nil {
		c.Logger.Error("failed to trim recent post labels", "error", err)
	}

	postsTrimmed.WithLabelValues(c.SocketURL).Add(float64(numDeleted))

	c.Logger.Infow("trimmed recent posts", "num_deleted", numDeleted, "duration", time.Since(start).Seconds())

	return nil
}

// OnEvent handles a stream event from the Jetstream firehose
func (c *Consumer) OnEvent(ctx context.Context, evt *models.Event) error {
	ctx, span := tracer.Start(ctx, "HandleStreamEvent")
	defer span.End()

	if evt.Identity != nil {
		now := time.Now()
		c.Progress.Update(evt.TimeUS, now)
		t := time.UnixMicro(evt.TimeUS)
		if evt.Identity.Handle == nil {
			c.Logger.Error("unexpected missing handle in identity event", "repo", evt.Identity.Did, "seq", evt.TimeUS)
			return nil
		}
		eventsProcessedCounter.WithLabelValues("repo_identity", c.SocketURL).Inc()
		err := c.Store.Queries.UpsertActor(ctx, store_queries.UpsertActorParams{
			Did:       evt.Identity.Did,
			Handle:    *evt.Identity.Handle,
			CreatedAt: sql.NullTime{Time: t, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to upsert actor: %+v", err)
		}
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(evt.TimeUS))
		return nil
	}

	if evt.Commit != nil {
		// Process the commit
		err := c.OnCommit(ctx, evt)
		if err != nil {
			return fmt.Errorf("failed to process commit: %+v", err)
		}
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
func (c *Consumer) OnCommit(ctx context.Context, evt *models.Event) error {
	ctx, span := tracer.Start(ctx, "OnEvent")
	defer span.End()

	processedAt := time.Now()

	c.Progress.Update(evt.TimeUS, processedAt)

	lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(evt.TimeUS))

	if evt.Commit == nil {
		c.Logger.Error("got commit with empty 'commit' field on it", "repo", evt.Did, "seq", evt.TimeUS)
		return nil
	}

	log := c.Logger.With("repo", evt.Did, "seq", evt.TimeUS, "commit", evt.Commit, "action", evt.Commit.Operation, "collection", evt.Commit.Collection)

	// Parse time from the event time string
	evtCreatedAt := time.UnixMicro(evt.TimeUS)

	lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.UnixNano()))
	lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.UnixNano()))
	lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(evtCreatedAt).Seconds()))

	metricCollection := evt.Commit.Collection
	if _, ok := knownCollections[evt.Commit.Collection]; !ok {
		metricCollection = "unknown"
	}
	opsProcessedCounter.WithLabelValues(evt.Commit.Operation, metricCollection, c.SocketURL).Inc()

	// recordURI := "at://" + evt.Repo + "/" + op.Path
	span.SetAttributes(attribute.String("repo", evt.Did))
	span.SetAttributes(attribute.String("collection", evt.Commit.Collection))
	span.SetAttributes(attribute.String("rkey", evt.Commit.RKey))
	span.SetAttributes(attribute.Int64("seq", evt.TimeUS))
	span.SetAttributes(attribute.String("event_kind", evt.Commit.Operation))
	switch evt.Commit.Operation {
	case models.CommitOperationCreate:
		recCreatedAt, err := c.HandleCreateRecord(ctx, evt.Did, evt.Commit.Collection, evt.Commit.RKey, evt.Commit.Record)
		if err != nil {
			log.Errorf("failed to handle create record: %+v", err)
		}

		if recCreatedAt != nil && !recCreatedAt.IsZero() {
			lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(recCreatedAt.UnixNano()))
			lastEvtCreatedRecordCreatedGapGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.Sub(*recCreatedAt).Seconds()))
			lastRecordCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(*recCreatedAt).Seconds()))
		}
	case models.CommitOperationUpdate:
		// Unpack the record and process it
		switch evt.Commit.Collection {
		case "app.bsky.actor.profile":
			// Process profile updates
			span.SetAttributes(attribute.String("record_type", "actor_profile"))
			recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()

			upsertParams := store_queries.UpsertActorFromFirehoseParams{
				Did:       evt.Did,
				Handle:    "",
				UpdatedAt: sql.NullTime{Time: time.Now(), Valid: true},
				CreatedAt: sql.NullTime{Time: time.Now(), Valid: true},
			}

			var rec bsky.ActorProfile
			err := json.Unmarshal(evt.Commit.Record, &rec)
			if err != nil {
				log.Errorf("failed to unmarshal actor profile: %+v", err)
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

			err = c.Store.Queries.UpsertActorFromFirehose(ctx, upsertParams)
			if err != nil {
				log.Errorf("failed to upsert actor from firehose: %+v", err)
			}
		}
	case models.CommitOperationDelete:
		err := c.HandleDeleteRecord(ctx, evt.Did, evt.Commit.Collection, evt.Commit.RKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				log.Warn("record not found, so we can't delete it")
			} else {
				log.Errorf("failed to handle delete record: %+v", err)
			}
		}
	default:
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
}

// HandleDeleteRecord handles a delete record event from the firehose
func (c *Consumer) HandleDeleteRecord(
	ctx context.Context,
	repo string,
	collection string,
	rkey string,
) error {
	ctx, span := tracer.Start(ctx, "HandleDeleteRecord")
	defer span.End()

	switch collection {
	case "app.bsky.feed.post":
		err := c.HandleDeletePost(ctx, repo, rkey)
		if err != nil {
			return fmt.Errorf("failed to handle delete post: %w", err)
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
				return fmt.Errorf("like not found, so we can't delete it: %w", err)
			}
			return fmt.Errorf("can't delete like: %w", err)
		}

		// Delete the like from the database
		err = c.Store.Queries.DeleteLike(ctx, store_queries.DeleteLikeParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete like: %w", err)
		}

		// Decrement the like count
		err = c.Store.Queries.DecrementLikeCountByN(ctx, store_queries.DecrementLikeCountByNParams{
			ActorDid:   like.SubjectActorDid,
			Collection: like.SubjectNamespace,
			Rkey:       like.SubjectRkey,
			NumLikes:   1,
		})
		if err != nil {
			return fmt.Errorf("failed to decrement like count: %w", err)
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
				return fmt.Errorf("repost not found, so we can't delete it: %w", err)
			}
			return fmt.Errorf("can't delete repost: %w", err)
		}

		// Delete the repost from the database
		err = c.Store.Queries.DeleteRepost(ctx, store_queries.DeleteRepostParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete repost: %w", err)
		}

		// Decrement the repost count
		err = c.Store.Queries.DecrementRepostCountByN(ctx, store_queries.DecrementRepostCountByNParams{
			ActorDid:   repost.SubjectActorDid,
			Collection: repost.SubjectNamespace,
			Rkey:       repost.SubjectRkey,
			NumReposts: 1,
		})
		if err != nil {
			return fmt.Errorf("failed to decrement repost count: %w", err)
		}
	case "app.bsky.graph.follow":
		span.SetAttributes(attribute.String("record_type", "graph_follow"))
		follow, err := c.Store.Queries.GetFollow(ctx, store_queries.GetFollowParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("follow not found, so we can't delete it: %w", err)
			}
			return fmt.Errorf("can't delete follow: %w", err)
		}

		if c.graphdClient != nil {
			err = c.graphdClient.Unfollow(ctx, repo, follow.TargetDid)
			if err != nil {
				log.Errorf("failed to propagate unfollow to GraphD: %w", err)
			}
		}

		err = c.Store.Queries.DeleteFollow(ctx, store_queries.DeleteFollowParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete follow: %w", err)
		}
		err = c.Store.Queries.DecrementFollowerCountByN(ctx, store_queries.DecrementFollowerCountByNParams{
			ActorDid:     follow.TargetDid,
			NumFollowers: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to decrement follower count: %w", err)
			// Don't return an error here, because we still want to try to decrement the following count
		}
		err = c.Store.Queries.DecrementFollowingCountByN(ctx, store_queries.DecrementFollowingCountByNParams{
			ActorDid:     repo,
			NumFollowing: 1,
			UpdatedAt:    time.Now(),
		})
		if err != nil {
			log.Errorf("failed to decrement following count: %w", err)
		}

	case "app.bsky.graph.block":
		span.SetAttributes(attribute.String("record_type", "graph_block"))
		block, err := c.Store.Queries.GetBlock(ctx, store_queries.GetBlockParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("block not found, so we can't delete it: %w", err)
			}
			return fmt.Errorf("can't delete block: %w", err)
		}
		err = c.Store.Queries.DeleteBlock(ctx, store_queries.DeleteBlockParams{
			ActorDid: block.ActorDid,
			Rkey:     rkey,
		})
		if err != nil {
			return fmt.Errorf("failed to delete block: %w", err)
		}

	}

	return nil
}

// HandleCreateRecord handles a create record event from the firehose
func (c *Consumer) HandleCreateRecord(
	ctx context.Context,
	repo string,
	collection string,
	rkey string,
	rec json.RawMessage,
) (*time.Time, error) {
	ctx, span := tracer.Start(ctx, "HandleCreateRecord")
	defer span.End()

	var recCreatedAt time.Time
	var parseError error

	indexedAt := time.Now()

	// Unpack the record and process it
	switch collection {
	case "app.bsky.feed.post":
		var post bsky.FeedPost
		err := json.Unmarshal(rec, &post)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal post: %w", err)
		}
		err = c.HandleCreatePost(ctx, repo, rkey, indexedAt, &post)
		if err != nil {
			return nil, fmt.Errorf("failed to handle post: %w", err)
		}
	case "app.bsky.feed.like":
		span.SetAttributes(attribute.String("record_type", "feed_like"))
		recordsProcessedCounter.WithLabelValues("feed_like", c.SocketURL).Inc()

		var like bsky.FeedLike
		err := json.Unmarshal(rec, &like)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal like: %w", err)
		}

		recCreatedAt, parseError = dateparse.ParseAny(like.CreatedAt)

		var subjectURI *URI
		if like.Subject != nil {
			subjectURI, err = GetURI(like.Subject.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Subject uri: %w", err)
			}
		}

		if subjectURI == nil {
			return nil, fmt.Errorf("invalid like subject: %+v", like.Subject)
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
	case "app.bsky.feed.repost":
		span.SetAttributes(attribute.String("record_type", "feed_repost"))
		recordsProcessedCounter.WithLabelValues("feed_repost", c.SocketURL).Inc()

		var repost bsky.FeedRepost
		err := json.Unmarshal(rec, &repost)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal repost: %w", err)
		}

		recCreatedAt, parseError = dateparse.ParseAny(repost.CreatedAt)
		var subjectURI *URI
		if repost.Subject != nil {
			subjectURI, err = GetURI(repost.Subject.Uri)
			if err != nil {
				return nil, fmt.Errorf("failed to get Subject uri: %w", err)
			}
		}

		if subjectURI == nil {
			return nil, fmt.Errorf("invalid repost subject: %+v", repost.Subject)
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

		// Track the user in the reposters bitmap
		hourlyRepostBMKey := fmt.Sprintf("reposts_hourly:%s", recCreatedAt.Format("2006_01_02_15"))

		err = c.bitmapper.AddMember(ctx, hourlyRepostBMKey, repo)
		if err != nil {
			log.Errorf("failed to add member to reposters bitmap: %+v", err)
		}
	case "app.bsky.graph.block":
		span.SetAttributes(attribute.String("record_type", "graph_block"))
		recordsProcessedCounter.WithLabelValues("graph_block", c.SocketURL).Inc()

		var block bsky.GraphBlock
		err := json.Unmarshal(rec, &block)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal block: %w", err)
		}

		recCreatedAt, parseError = dateparse.ParseAny(block.CreatedAt)

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
			TargetDid: block.Subject,
			CreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create block: %+v", err)
		}

		// Track the user in the blockers bitmap
		hourlyBlocksBMKey := fmt.Sprintf("blocks_hourly:%s", recCreatedAt.Format("2006_01_02_15"))

		err = c.bitmapper.AddMember(ctx, hourlyBlocksBMKey, repo)
		if err != nil {
			log.Errorf("failed to add member to blockers bitmap: %+v", err)
		}
	case "app.bsky.graph.follow":
		span.SetAttributes(attribute.String("record_type", "graph_follow"))
		recordsProcessedCounter.WithLabelValues("graph_follow", c.SocketURL).Inc()

		var follow bsky.GraphFollow
		err := json.Unmarshal(rec, &follow)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal follow: %w", err)
		}

		recCreatedAt, parseError = dateparse.ParseAny(follow.CreatedAt)

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
			TargetDid: follow.Subject,
			CreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create follow: %+v", err)
		}
		err = c.Store.Queries.IncrementFollowerCountByN(ctx, store_queries.IncrementFollowerCountByNParams{
			ActorDid:     follow.Subject,
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
			err = c.graphdClient.Follow(ctx, repo, follow.Subject)
			if err != nil {
				log.Errorf("failed to propagate follow to GraphD: %+v", err)
			}
		}

		// Track the user in the followers bitmap
		hourlyFollowsBMKey := fmt.Sprintf("follows_hourly:%s", recCreatedAt.Format("2006_01_02_15"))

		err = c.bitmapper.AddMember(ctx, hourlyFollowsBMKey, repo)
		if err != nil {
			log.Errorf("failed to add member to followers bitmap: %+v", err)
		}
	case "app.bsky.actor.profile":
		span.SetAttributes(attribute.String("record_type", "actor_profile"))
		recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()

		var profile bsky.ActorProfile
		err := json.Unmarshal(rec, &profile)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal actor profile: %w", err)
		}

		upsertParams := store_queries.UpsertActorFromFirehoseParams{
			Did:       repo,
			Handle:    "",
			UpdatedAt: sql.NullTime{Time: time.Now(), Valid: true},
			CreatedAt: sql.NullTime{Time: time.Now(), Valid: true},
		}

		if profile.DisplayName != nil && *profile.DisplayName != "" {
			upsertParams.DisplayName = sql.NullString{String: *profile.DisplayName, Valid: true}
		}

		if profile.Description != nil && *profile.Description != "" {
			upsertParams.Bio = sql.NullString{String: *profile.Description, Valid: true}
		}

		if profile.Avatar != nil {
			upsertParams.ProPicCid = sql.NullString{String: profile.Avatar.Ref.String(), Valid: true}
		}

		if profile.Banner != nil {
			upsertParams.BannerCid = sql.NullString{String: profile.Banner.Ref.String(), Valid: true}
		}

		err = c.Store.Queries.UpsertActorFromFirehose(ctx, upsertParams)
		if err != nil {
			log.Errorf("failed to upsert actor from firehose: %+v", err)
		}
	default:
		span.SetAttributes(attribute.String("record_type", "other"))
	}

	if parseError != nil {
		return nil, fmt.Errorf("failed to parse created at time: %w", parseError)
	}

	return &recCreatedAt, nil
}
