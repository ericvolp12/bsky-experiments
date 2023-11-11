package endpoints

import (
	"fmt"
	"sync"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/clusters"
	"golang.org/x/time/rate"

	"github.com/ericvolp12/bsky-experiments/pkg/usercount"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"go.opentelemetry.io/otel"
)

type API struct {
	PostRegistry *search.PostRegistry
	UserCount    *usercount.UserCount

	Store *store.Store

	ClusterManager *clusters.ClusterManager

	LayoutServiceHost string

	ThreadViewCacheTTL time.Duration
	ThreadViewCache    *lru.ARCCache[string, ThreadViewCacheEntry]
	LayoutCacheTTL     time.Duration
	LayoutCache        *lru.ARCCache[string, LayoutCacheEntry]
	StatsCacheTTL      time.Duration
	StatsCache         *StatsCacheEntry
	StatsCacheRWMux    *sync.RWMutex

	CheckoutLimiter *rate.Limiter
}

var tracer = otel.Tracer("search-api")

func NewAPI(
	postRegistry *search.PostRegistry,
	store *store.Store,
	userCount *usercount.UserCount,
	graphJSONUrl string,
	layoutServiceHost string,
	threadViewCacheTTL time.Duration,
	layoutCacheTTL time.Duration,
	statsCacheTTL time.Duration,
) (*API, error) {
	// Hellthread is around 300KB right now so 1000 worst-case threads should be around 300MB
	threadViewCache, err := lru.NewARC[string, ThreadViewCacheEntry](1000)
	if err != nil {
		return nil, fmt.Errorf("error initializing thread view cache: %w", err)
	}

	layoutCache, err := lru.NewARC[string, LayoutCacheEntry](500)
	if err != nil {
		return nil, fmt.Errorf("error initializing layout cache: %w", err)
	}

	clusterManager, err := clusters.NewClusterManager(graphJSONUrl)
	if err != nil {
		return nil, fmt.Errorf("error initializing cluster manager: %w", err)
	}

	return &API{
		PostRegistry:       postRegistry,
		UserCount:          userCount,
		Store:              store,
		ClusterManager:     clusterManager,
		LayoutServiceHost:  layoutServiceHost,
		ThreadViewCacheTTL: threadViewCacheTTL,
		ThreadViewCache:    threadViewCache,
		LayoutCacheTTL:     layoutCacheTTL,
		LayoutCache:        layoutCache,
		StatsCacheTTL:      statsCacheTTL,
		StatsCacheRWMux:    &sync.RWMutex{},
		CheckoutLimiter:    rate.NewLimiter(rate.Every(2*time.Second), 1),
	}, nil
}
