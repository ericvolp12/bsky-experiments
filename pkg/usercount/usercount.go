package usercount

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"

	"golang.org/x/time/rate"
)

type UserCount struct {
	Client           *xrpc.Client
	ClientMux        *sync.RWMutex
	CurrentUserCount int
	RateLimiter      *rate.Limiter
	LastCursor       string
	LastPageSize     int

	RedisClient *redis.Client
	Prefix      string
}

func NewUserCount(ctx context.Context, client *xrpc.Client, redisClient *redis.Client) *UserCount {
	clientMux := &sync.RWMutex{}

	// Run a routine that refreshes the auth token every 10 minutes
	authTicker := time.NewTicker(10 * time.Minute)
	quit := make(chan struct{})
	go func() {
		log.Println("starting auth refresh routine...")
		for {
			select {
			case <-authTicker.C:
				log.Println("refreshing auth token...")
				err := intXRPC.RefreshAuth(ctx, client, clientMux)
				if err != nil {
					log.Printf("error refreshing auth token: %s\n", err)
				} else {
					log.Println("successfully refreshed auth token")
				}
			case <-quit:
				authTicker.Stop()
				return
			}
		}
	}()

	prefix := "usercount"

	// Check for a prior cursor in redis

	lastCursor, err := redisClient.Get(ctx, prefix+":last_cursor").Result()
	if err != nil {
		if err != redis.Nil {
			log.Printf("error getting last cursor from redis: %s\n", err)
		}
		lastCursor = ""
	}

	lastPageSize, err := redisClient.Get(ctx, prefix+":last_page_size").Int()
	if err != nil {
		if err != redis.Nil {
			log.Printf("error getting last page size from redis: %s\n", err)
		}
		lastPageSize = 0
	}

	lastUserCount, err := redisClient.Get(ctx, prefix+":last_user_count").Int()
	if err != nil {
		if err != redis.Nil {
			log.Printf("error getting last user count from redis: %s\n", err)
		}
		lastUserCount = 0
	}

	// Set up a rate limiter to limit requests to 50 per second
	limiter := rate.NewLimiter(rate.Limit(50), 1)

	return &UserCount{
		Client:           client,
		RateLimiter:      limiter,
		ClientMux:        clientMux,
		RedisClient:      redisClient,
		Prefix:           "usercount",
		LastCursor:       lastCursor,
		LastPageSize:     lastPageSize,
		CurrentUserCount: lastUserCount,
	}
}

// GetUserCount returns the number of users of BSky from the Repo Sync API
// It uses a rate limiter to limit requests to 5 per second
// It does not implement any caching, so it will make a series of requests to the API every time it is called
// Caching should be implemented one layer up in the application
func (uc *UserCount) GetUserCount(ctx context.Context) (int, error) {
	ctx, span := otel.Tracer("usercount").Start(ctx, "GetUserCount")
	defer span.End()

	for {
		// Use rate limiter before each request
		err := uc.RateLimiter.Wait(ctx)
		if err != nil {
			fmt.Printf("error waiting for rate limiter: %v", err)
			return -1, fmt.Errorf("error waiting for rate limiter: %w", err)
		}

		span.AddEvent("AcquireClientRLock")
		uc.ClientMux.RLock()
		span.AddEvent("ClientRLockAcquired")
		repoOutput, err := comatproto.SyncListRepos(ctx, uc.Client, uc.LastCursor, 1000)
		if err != nil {
			fmt.Printf("error listing repos: %s\n", err)
			span.AddEvent("ReleaseClientRLock")
			uc.ClientMux.RUnlock()
			return -1, fmt.Errorf("error listing repos: %w", err)
		}
		span.AddEvent("ReleaseClientRLock")
		uc.ClientMux.RUnlock()

		// On the last page, the cursor will be nil and the repo list will be empty

		uc.CurrentUserCount += len(repoOutput.Repos)

		if repoOutput.Cursor == nil {
			break
		}

		uc.LastCursor = *repoOutput.Cursor
	}

	// Store the last cursor in redis
	err := uc.RedisClient.Set(ctx, uc.Prefix+":last_cursor", uc.LastCursor, 0).Err()
	if err != nil {
		log.Printf("error setting last cursor in redis: %s\n", err)
	}

	// Store the last page size in redis
	err = uc.RedisClient.Set(ctx, uc.Prefix+":last_page_size", uc.LastPageSize, 0).Err()
	if err != nil {
		log.Printf("error setting last page size in redis: %s\n", err)
	}

	// Store the last user count in redis
	err = uc.RedisClient.Set(ctx, uc.Prefix+":last_user_count", uc.CurrentUserCount, 0).Err()
	if err != nil {
		log.Printf("error setting last user count in redis: %s\n", err)
	}

	return uc.CurrentUserCount, nil
}
