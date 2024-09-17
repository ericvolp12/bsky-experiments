package endpoints

import (
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"golang.org/x/time/rate"

	"github.com/ericvolp12/bsky-experiments/pkg/usercount"
	"go.opentelemetry.io/otel"
)

type API struct {
	UserCount *usercount.UserCount

	Store     *store.Store
	Directory identity.Directory

	StatsCacheTTL   time.Duration
	StatsCache      *StatsCacheEntry
	StatsCacheRWMux *sync.RWMutex

	CheckoutLimiter *rate.Limiter
	MagicHeaderVal  string

	Bitmapper *consumer.Bitmapper
}

var tracer = otel.Tracer("search-api")

func NewAPI(
	store *store.Store,
	userCount *usercount.UserCount,
	MagicHeaderVal string,
	statsCacheTTL time.Duration,
) (*API, error) {
	dir := identity.DefaultDirectory()

	bitmapper, err := consumer.NewBitmapper(store)
	if err != nil {
		return nil, fmt.Errorf("error initializing bitmapper: %w", err)
	}

	return &API{
		UserCount:       userCount,
		Store:           store,
		Directory:       dir,
		MagicHeaderVal:  MagicHeaderVal,
		StatsCacheTTL:   statsCacheTTL,
		StatsCacheRWMux: &sync.RWMutex{},
		CheckoutLimiter: rate.NewLimiter(rate.Every(2*time.Second), 1),
		Bitmapper:       bitmapper,
	}, nil
}
