package consumer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
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
	"github.com/labstack/gommon/log"
	"github.com/redis/go-redis/v9"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
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
func NewConsumer(logger *zap.SugaredLogger, redisClient *redis.Client, redisPrefix string, store *store.Store, socketURL string) (*Consumer, error) {
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
	}

	// Check to see if the cursor exists in redis
	err := c.ReadCursor(context.Background())
	if err != nil {
		if !strings.Contains(err.Error(), "redis: nil") {
			return nil, fmt.Errorf("failed to read cursor from redis: %+v", err)
		}
		logger.Warn("cursor not found in redis, starting from live")
	}

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
		case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
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

			var recCreatedAt time.Time
			var parseError error

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
						log.Errorf("failed to get Quoted Record uri: %+v", err)
						continue
					}
					parentActorDid = u.Did
					parentActorRkey = u.RKey
					parentRelationship = "q"
				}
				if rec.Reply != nil && rec.Reply.Parent != nil {
					u, err := getURI(rec.Reply.Parent.Uri)
					if err != nil {
						log.Errorf("failed to get Reply uri: %+v", err)
						continue
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
						log.Errorf("failed to get Root uri: %+v", err)
						continue
					}
					rootActorDid = u.Did
					rootActorRkey = u.RKey
				}

				recCreatedAt, parseError = dateparse.ParseAny(rec.CreatedAt)

				err = c.Store.Queries.CreatePost(ctx, store_queries.CreatePostParams{
					ActorDid:           evt.Repo,
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
							PostActorDid: evt.Repo,
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
						log.Errorf("failed to get Subject uri: %+v", err)
						continue
					}
				}

				if subjectURI == nil {
					log.Errorf("invalid like subject: %+v", rec.Subject)
					continue
				}

				err = c.Store.Queries.CreateLike(ctx, store_queries.CreateLikeParams{
					ActorDid:         evt.Repo,
					Rkey:             rkey,
					SubjectActorDid:  subjectURI.Did,
					SubjectNamespace: subjectURI.Namespace,
					SubjectRkey:      subjectURI.RKey,
					CreatedAt:        sql.NullTime{Time: recCreatedAt, Valid: true},
				})
				if err != nil {
					log.Errorf("failed to create like: %+v", err)
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
					ActorDid:  evt.Repo,
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
					ActorDid:  evt.Repo,
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
				log.Errorf("error parsing time: %+v", parseError)
				continue
			}
			if !recCreatedAt.IsZero() {
				lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(recCreatedAt.UnixNano()))
				lastEvtCreatedRecordCreatedGapGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.Sub(recCreatedAt).Seconds()))
				lastRecordCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(recCreatedAt).Seconds()))
			}

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
