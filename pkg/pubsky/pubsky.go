package pubsky

import (
	"context"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

type Pubsky struct {
	Store     *store.Store
	PLCMirror string

	redisClient     *redis.Client
	postCacheTTL    time.Duration
	profileCacheTTL time.Duration
}

type PubskyOptions struct {
	PostCacheTTL    time.Duration
	ProfileCacheTTL time.Duration
}

var tracer = otel.Tracer("pubsky")

func DefaultPubskyOptions() *PubskyOptions {
	return &PubskyOptions{
		PostCacheTTL:    time.Minute * 5,
		ProfileCacheTTL: time.Minute * 5,
	}
}

func NewPubsky(ctx context.Context, store *store.Store, redisClient *redis.Client, plcMirror string, opts *PubskyOptions) *Pubsky {
	if opts == nil {
		opts = DefaultPubskyOptions()
	}

	return &Pubsky{
		Store:           store,
		PLCMirror:       plcMirror,
		redisClient:     redisClient,
		postCacheTTL:    opts.PostCacheTTL,
		profileCacheTTL: opts.ProfileCacheTTL,
	}
}
