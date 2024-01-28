package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/redis/go-redis/v9"
	"golang.org/x/exp/slog"
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	store, err := store.NewStore("postgresql://")
	if err != nil {
		panic(err)
	}

	start := time.Now()
	page := 0
	totalFollows := 0
	lastInsertedAt := time.Time{}

	uidMap := map[string]uint64{}

	nextUID := uint64(0)

	for {
		slog.Info("getting page", "page", page)
		follows, err := store.Queries.GetFollowPage(ctx, store_queries.GetFollowPageParams{
			InsertedAt: lastInsertedAt,
			Limit:      400000,
		})

		if err != nil {
			slog.Error("failed to get page", "err", err)
			return
		}

		if len(follows) == 0 {
			slog.Info("no more follows")
			break
		}

		totalFollows += len(follows)
		lastInsertedAt = follows[len(follows)-1].InsertedAt

		pipeline := redisClient.Pipeline()
		for _, follow := range follows {
			actorUID, ok := uidMap[follow.ActorDid]
			if !ok {
				actorUID = nextUID
				nextUID++
				uidMap[follow.ActorDid] = actorUID
				pipeline.Set(ctx, fmt.Sprintf("cg:dtu:%s", follow.ActorDid), actorUID, 0)
				pipeline.Set(ctx, fmt.Sprintf("cg:utd:%d", actorUID), follow.ActorDid, 0)
			}
			targetUID, ok := uidMap[follow.TargetDid]
			if !ok {
				targetUID = nextUID
				nextUID++
				uidMap[follow.TargetDid] = targetUID
				pipeline.Set(ctx, fmt.Sprintf("cg:dtu:%s", follow.TargetDid), targetUID, 0)
				pipeline.Set(ctx, fmt.Sprintf("cg:utd:%d", targetUID), follow.TargetDid, 0)
			}
			pipeline.SAdd(ctx, fmt.Sprintf("cg:%d:following", actorUID), targetUID)
			pipeline.SAdd(ctx, fmt.Sprintf("cg:%d:followers", targetUID), actorUID)
		}

		_, err = pipeline.Exec(ctx)
		if err != nil {
			slog.Error("failed to add follows to redis", "err", err)
			return
		}
		slog.Info("added follows to redis", "total", totalFollows)
		page++
	}

	slog.Info("total follows", "total", totalFollows, "duration", time.Since(start))

	// Let the redis instance settle (not sure if this is necessary)
	time.Sleep(10 * time.Second)

	queryStart := time.Now()

	uid, err := redisClient.Get(ctx, "cg:dtu:did:plc:z72i7hdynmk6r22z27h6tvur").Result()
	if err != nil {
		slog.Error("failed to get uid", "err", err)
	} else {
		uidDone := time.Now()
		followers, err := redisClient.SMembers(ctx, fmt.Sprintf("cg:%s:followers", uid)).Result()
		if err != nil {
			slog.Error("failed to get followers", "err", err)
		}
		membersDone := time.Now()

		// Lookup the dids
		pipeline := redisClient.Pipeline()
		const chunkSize = 5000
		// Break the followers into chunks of 5k
		for i := 0; i < len(followers); i += chunkSize {
			end := i + chunkSize
			if end > len(followers) {
				end = len(followers)
			}
			chunkedFollowers := followers[i:end]

			keys := make([]string, len(chunkedFollowers))
			for j, follower := range chunkedFollowers {
				keys[j] = fmt.Sprintf("cg:utd:%s", follower)
			}

			pipeline.MGet(ctx, keys...)
		}

		_, err = pipeline.Exec(ctx)
		if err != nil {
			slog.Error("failed to get dids", "err", err)
		}
		didsDone := time.Now()
		slog.Info("got followers",
			"followers", len(followers),
			"uid", uid,
			"uidDuration", uidDone.Sub(queryStart),
			"membersDuration", membersDone.Sub(uidDone),
			"didsDuration", didsDone.Sub(membersDone),
			"totalDuration", didsDone.Sub(queryStart),
		)
	}
}
