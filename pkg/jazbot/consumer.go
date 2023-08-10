package jazbot

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	typegen "github.com/whyrusleeping/cbor-gen"

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

	Jazbot *Jazbot
}

// Progress is the cursor for the consumer
type Progress struct {
	LastSeq            int64     `json:"last_seq"`
	LastSeqProcessedAt time.Time `json:"last_seq_processed_at"`
}

var tracer = otel.Tracer("jazbot")

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
func NewConsumer(
	ctx context.Context,
	logger *zap.SugaredLogger,
	redisClient *redis.Client,
	redisPrefix string,
	jazbot *Jazbot,
	socketURL string,
) (*Consumer, error) {
	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
		},
		Logger:      logger,
		ProgMux:     sync.Mutex{},
		RedisClient: redisClient,
		ProgressKey: fmt.Sprintf("%s:progress", redisPrefix),
		Jazbot:      jazbot,
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
		return nil
	}

	// Parse time from the event time string
	evtCreatedAt, err := time.Parse(time.RFC3339, evt.Time)
	if err != nil {
		log.Errorf("error parsing time: %+v", err)
		return nil
	}

	lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(evtCreatedAt).Seconds()))

	for _, op := range evt.Ops {
		collection := strings.Split(op.Path, "/")[0]
		rkey := strings.Split(op.Path, "/")[1]

		ek := repomgr.EventKind(op.Action)
		log = log.With("action", op.Action, "collection", collection)

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
			recCreatedAt, err := c.HandleCreateRecord(ctx, evt.Repo, op.Path, rec, op.Cid)
			if err != nil {
				log.Errorf("failed to handle create record: %+v", err)
			}

			if recCreatedAt != nil && !recCreatedAt.IsZero() {
				lastEvtCreatedRecordCreatedGapGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.Sub(*recCreatedAt).Seconds()))
				lastRecordCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(*recCreatedAt).Seconds()))
			}
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
	cid *lexutil.LexLink,
) (*time.Time, error) {
	ctx, span := tracer.Start(ctx, "HandleCreateRecord")

	var recCreatedAt time.Time
	var parseError error

	rkey := strings.Split(path, "/")[1]

	// Unpack the record and process it
	switch rec := rec.(type) {
	case *bsky.FeedPost:
		span.SetAttributes(attribute.String("record_type", "feed_post"))
		recordsProcessedCounter.WithLabelValues("feed_post", c.SocketURL).Inc()

		var parentURI string
		if rec.Reply != nil && rec.Reply.Parent != nil {
			parentURI = rec.Reply.Parent.Uri
		}

		// Check if the record text starts with `!jazbot`
		if strings.HasPrefix(rec.Text, "!jazbot") {
			err := c.Jazbot.HandleRequest(ctx, repo, rkey, rec.Text, cid, &parentURI)
			if err != nil {
				return nil, fmt.Errorf("jazbot failed to handle request: %+v", err)
			}
		}
	default:
		return nil, nil
	}
	if parseError != nil {
		return nil, fmt.Errorf("error parsing time: %w", parseError)
	}

	return &recCreatedAt, nil
}
