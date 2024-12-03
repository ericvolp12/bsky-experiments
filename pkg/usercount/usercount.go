package usercount

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"golang.org/x/time/rate"
)

type UserCount struct {
	CurrentUserCount int

	RedisClient *redis.Client
	Prefix      string

	PDSs []*PDS
}

func NewUserCount(ctx context.Context, redisClient *redis.Client) *UserCount {
	prefix := "usercount"

	// Check for a prior cursor in redis
	pdsList, err := redisClient.HGetAll(ctx, prefix+":pdslist").Result()
	if err != nil {
		if err != redis.Nil {
			log.Printf("error getting last pds from redis: %s\n", err)
		}
		pdsList = map[string]string{}
	}

	// pdsList is a map of host -> last cursor, last page size, last user count
	// We need to convert it to a slice of PDS structs
	pdsSlice := []*PDS{}
	for host, pdsString := range pdsList {
		pds := NewPDS(host, 25)
		_, err := fmt.Sscanf(pdsString, "%d|%d|%s", &pds.UserCount, &pds.LastPageSize, &pds.LastCursor)
		if err != nil {
			_, err := fmt.Sscanf(pdsString, "%d|%d|", &pds.UserCount, &pds.LastPageSize)
			if err != nil {
				log.Printf("error parsing pds string: %s\n", err)
				continue
			}
			pds.LastCursor = ""
		}
		pdsSlice = append(pdsSlice, pds)
	}

	// If there are no PDSs in redis, add the default list
	if len(pdsSlice) == 0 {
		for _, host := range PDSHostList {
			pdsSlice = append(pdsSlice, NewPDS(host, 25))
		}
	}

	lastUserCount, err := redisClient.Get(ctx, prefix+":last_user_count").Int()
	if err != nil {
		if err != redis.Nil {
			log.Printf("error getting last user count from redis: %s\n", err)
		}
		lastUserCount = 0
	}

	return &UserCount{
		RedisClient:      redisClient,
		Prefix:           prefix,
		CurrentUserCount: lastUserCount,
		PDSs:             pdsSlice,
	}
}

var PDSHostList = []string{
	"https://morel.us-east.host.bsky.network",
	"https://puffball.us-east.host.bsky.network",
	"https://inkcap.us-east.host.bsky.network",
	"https://oyster.us-east.host.bsky.network",
	"https://enoki.us-east.host.bsky.network",
	"https://porcini.us-east.host.bsky.network",
	"https://shimeji.us-east.host.bsky.network",
	"https://amanita.us-east.host.bsky.network",
	"https://lionsmane.us-east.host.bsky.network",
	"https://shiitake.us-east.host.bsky.network",
	"https://blewit.us-west.host.bsky.network",
	"https://conocybe.us-west.host.bsky.network",
	"https://boletus.us-west.host.bsky.network",
	"https://lepista.us-west.host.bsky.network",
	"https://chaga.us-west.host.bsky.network",
	"https://agaric.us-west.host.bsky.network",
	"https://maitake.us-west.host.bsky.network",
	"https://verpa.us-west.host.bsky.network",
	"https://russula.us-west.host.bsky.network",
	"https://hydnum.us-west.host.bsky.network",
	"https://coral.us-east.host.bsky.network",
	"https://lobster.us-east.host.bsky.network",
	"https://magic.us-west.host.bsky.network",
	"https://woodear.us-west.host.bsky.network",
	"https://cordyceps.us-west.host.bsky.network",
	"https://ganoderma.us-west.host.bsky.network",
	"https://bracket.us-west.host.bsky.network",
	"https://button.us-west.host.bsky.network",
	"https://chanterelle.us-west.host.bsky.network",
	"https://cremini.us-west.host.bsky.network",
	"https://gomphus.us-west.host.bsky.network",
	"https://matsutake.us-west.host.bsky.network",
	"https://milkcap.us-west.host.bsky.network",
	"https://shaggymane.us-west.host.bsky.network",
	"https://stinkhorn.us-west.host.bsky.network",
	"https://witchesbutter.us-west.host.bsky.network",
	"https://earthstar.us-east.host.bsky.network",
	"https://meadow.us-east.host.bsky.network",
	"https://parasol.us-east.host.bsky.network",
	"https://reishi.us-east.host.bsky.network",
	"https://scarletina.us-east.host.bsky.network",
	"https://splitgill.us-east.host.bsky.network",
	"https://truffle.us-east.host.bsky.network",
	"https://velvetfoot.us-east.host.bsky.network",
	"https://helvella.us-east.host.bsky.network",
	"https://panthercap.us-east.host.bsky.network",
	"https://yellowfoot.us-west.host.bsky.network",
	"https://blusher.us-east.host.bsky.network",
	"https://brittlegill.us-west.host.bsky.network",
	"https://fibercap.us-west.host.bsky.network",
	"https://poisonpie.us-west.host.bsky.network",
	"https://oysterling.us-west.host.bsky.network",
	"https://woodtuft.us-west.host.bsky.network",
	"https://scalycap.us-west.host.bsky.network",
}

type PDS struct {
	Host         string
	UserCount    int
	LastCursor   string
	LastPageSize int
	Limiter      *rate.Limiter
	Client       *xrpc.Client
}

func NewPDS(host string, rps int) *PDS {
	instrumentedTransport := otelhttp.NewTransport(&http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// Create the XRPC Client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: instrumentedTransport,
		},
		Host: host,
	}

	return &PDS{
		Host:    host,
		Client:  &client,
		Limiter: rate.NewLimiter(rate.Limit(rps), 1),
	}
}

// GetUserCount returns the number of users of BSky from the Repo Sync API
// It uses a rate limiter to limit requests to 5 per second
// It does not implement any caching, so it will make a series of requests to the API every time it is called
// Caching should be implemented one layer up in the application
func (uc *UserCount) GetUserCount(ctx context.Context) (int, error) {
	ctx, span := otel.Tracer("usercount").Start(ctx, "GetUserCount")
	defer span.End()
	var wg sync.WaitGroup
	resultCh := make(chan int, len(uc.PDSs))
	errorCh := make(chan error, len(uc.PDSs))

	slog.Info("refreshing user counts")

	for _, pds := range uc.PDSs {
		wg.Add(1)
		go func(pds *PDS) {
			defer wg.Done()

			// Reset the cursor and counts every time
			// pds.LastCursor = ""
			// pds.UserCount = 0
			// pds.LastPageSize = 0

			for {
				err := pds.Limiter.Wait(ctx)
				if err != nil {
					errorCh <- fmt.Errorf("error waiting for rate limiter: %w", err)
					return
				}

				repoOutput, err := comatproto.SyncListRepos(ctx, pds.Client, pds.LastCursor, 1000)
				if err != nil {
					errorCh <- fmt.Errorf("error listing repos: %w", err)
					return
				}

				numActive := 0
				for _, repo := range repoOutput.Repos {
					if repo.Active != nil && *repo.Active {
						numActive++
					}
				}

				pds.UserCount += numActive
				pds.LastPageSize = len(repoOutput.Repos)

				if repoOutput.Cursor == nil {
					resultCh <- pds.UserCount
					slog.Info("Finished counting users for PDS", "host", pds.Host, "count", pds.UserCount)
					return
				}

				pds.LastCursor = *repoOutput.Cursor
			}
		}(pds)
	}

	go func() {
		wg.Wait()
		close(resultCh)
		close(errorCh)
	}()

	var totalUserCount int
	for count := range resultCh {
		totalUserCount += count
	}

	select {
	case err := <-errorCh:
		if err != nil {
			return -1, err
		}
	default:
		// No error
	}

	uc.CurrentUserCount = totalUserCount

	// Store the PDS list in redis
	pdsList := map[string]interface{}{}
	for _, pds := range uc.PDSs {
		pdsList[pds.Host] = fmt.Sprintf("%d|%d|%s", pds.UserCount, pds.LastPageSize, pds.LastCursor)
	}

	err := uc.RedisClient.HSet(ctx, uc.Prefix+":pdslist", pdsList).Err()
	if err != nil {
		log.Printf("error setting pds list in redis: %s\n", err)
	}

	// Store the last user count in redis
	err = uc.RedisClient.Set(ctx, uc.Prefix+":last_user_count", uc.CurrentUserCount, 0).Err()
	if err != nil {
		log.Printf("error setting last user count in redis: %s\n", err)
	}

	return uc.CurrentUserCount, nil
}
