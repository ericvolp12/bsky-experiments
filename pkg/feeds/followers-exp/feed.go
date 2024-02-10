package followersexp

import (
	"context"
	"fmt"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	graphdclient "github.com/ericvolp12/bsky-experiments/pkg/graphd/client"
	"github.com/ericvolp12/bsky-experiments/pkg/sharddb"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type FollowersFeed struct {
	FeedActorDID string
	GraphD       *graphdclient.Client
	Redis        *redis.Client
	ShardDB      *sharddb.ShardDB
	Store        *store.Store

	spamFollowers map[string]struct{}
	sfLk          sync.RWMutex
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"my-followers-ex", "my-followers"}

var tracer = otel.Tracer("my-followers-ex")

func NewFollowersFeed(ctx context.Context, feedActorDID string, gClient *graphdclient.Client, rClient *redis.Client, shardDB *sharddb.ShardDB, store *store.Store) (*FollowersFeed, []string, error) {
	f := FollowersFeed{
		FeedActorDID:  feedActorDID,
		GraphD:        gClient,
		Redis:         rClient,
		ShardDB:       shardDB,
		Store:         store,
		spamFollowers: map[string]struct{}{},
	}

	// Refresh the spam followers
	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				ctx := context.Background()
				err := f.refreshSpamFollowers(ctx)
				if err != nil {
					fmt.Printf("error refreshing spam followers: %v\n", err)
				}
			}
		}
	}()

	return &f, supportedFeeds, nil
}

var activePostersKey = "consumer:active_posters"

func (f *FollowersFeed) intersectActivePosters(
	ctx context.Context,
	dids []string,
) (map[string]struct{}, error) {
	ctx, span := tracer.Start(ctx, "IntersectActivePosters")
	defer span.End()

	// Intersect the active posters sorted set and the given set of posters
	// to find the posters that are both active and in the given set

	zs := make([]redis.Z, len(dids))
	for i, did := range dids {
		zs[i] = redis.Z{
			Score:  1,
			Member: did,
		}
	}

	// Add the posters to a temporary set
	tempSetKey := fmt.Sprintf("consumer:active_posters:temp:%s", uuid.New().String())
	_, err := f.Redis.ZAdd(ctx, tempSetKey, zs...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to add posters to temp set: %+v", err)
	}

	// Intersect the temp set with the active posters set
	intersection, err := f.Redis.ZInter(ctx, &redis.ZStore{
		Keys: []string{tempSetKey, activePostersKey},
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to intersect temp set with active posters set: %+v", err)
	}

	// Delete the temp set
	_, err = f.Redis.Del(ctx, tempSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to delete temp set: %+v", err)
	}

	// Convert the intersection to a map for easy lookup
	intersectionMap := map[string]struct{}{}
	for _, did := range intersection {
		intersectionMap[did] = struct{}{}
	}

	return intersectionMap, nil
}

func (f *FollowersFeed) refreshSpamFollowers(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "RefreshSpamFollowers")
	defer span.End()

	// Get the spam followers from the store
	spamFollowers, err := f.Store.Queries.GetSpamFollowers(ctx)
	if err != nil {
		return fmt.Errorf("failed to get spam followers: %w", err)
	}

	// Convert the spam followers to a map for easy lookup
	spamFollowersMap := map[string]struct{}{}
	for _, did := range spamFollowers {
		spamFollowersMap[did] = struct{}{}
	}

	// Update the spam followers
	f.sfLk.Lock()
	f.spamFollowers = spamFollowersMap
	f.sfLk.Unlock()

	return nil
}

func (f *FollowersFeed) getSpamFollowers(ctx context.Context) map[string]struct{} {
	f.sfLk.RLock()
	defer f.sfLk.RUnlock()

	return f.spamFollowers
}

func (f *FollowersFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	if userDID == "" {
		return nil, nil, fmt.Errorf("feed %s requires authentication", feed)
	}

	var err error
	createdAt := time.Now()
	var authorDID string
	var rkey string

	createdAt, authorDID, rkey, err = ParseCursor(cursor)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	span.SetAttributes(attribute.String("createdAt", createdAt.Format(time.RFC3339)))
	span.SetAttributes(attribute.String("authorDID", authorDID))
	span.SetAttributes(attribute.String("rkey", rkey))

	nonMoots, err := f.GraphD.GetFollowersNotFollowing(ctx, userDID)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting non-moots: %w", err)
	}

	span.SetAttributes(attribute.Int("nonMoots", len(nonMoots)))

	// Get the intersection of the non-moots and the active posters
	nonMootMap, err := f.intersectActivePosters(ctx, nonMoots)
	if err != nil {
		return nil, nil, fmt.Errorf("error intersecting active posters: %w", err)
	}

	span.SetAttributes(attribute.Int("nonMootMap", len(nonMootMap)))

	// Get the spam followers
	spamFollowers := f.getSpamFollowers(ctx)

	// Remove the spam followers from the non-moots
	for did := range spamFollowers {
		delete(nonMootMap, did)
	}

	span.SetAttributes(attribute.Int("nonMootMapAfterSpam", len(nonMootMap)))

	filteredPostURIs := []string{}
	newRkey := ""

	// Use the store to get the posts
	if len(nonMootMap) < 1000 {
		rawPosts, err := f.Store.Queries.GetRecentPostsFromNonSpamUsers(ctx, store_queries.GetRecentPostsFromNonSpamUsersParams{
			Dids:            nonMoots,
			Limit:           int32(limit),
			CursorCreatedAt: createdAt,
			CursorActorDid:  authorDID,
			CursorRkey:      rkey,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error getting posts: %w", err)
		}

		// Add URIs to the filteredPostURIs
		for _, post := range rawPosts {
			filteredPostURIs = append(filteredPostURIs, fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey))
		}

		// Set the cursor to the last post
		if len(rawPosts) > 0 {
			lastPost := rawPosts[len(rawPosts)-1]
			createdAt = lastPost.CreatedAt.Time
			authorDID = lastPost.ActorDid
			newRkey = lastPost.Rkey
		}
	} else {
		// Otherwise use the sharddb to get the posts (high recent hit-rate)
		bucket, err := sharddb.GetBucketFromRKey(rkey)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting bucket from rkey: %w", err)
		}

		maxPages := 50
		pageSize := 5000

		metaPageCursor := createdAt
		for i := 0; i < maxPages; i++ {
			// Walk posts in reverse chronological order
			postMetas, nextCursor, err := f.ShardDB.GetPostMetas(ctx, bucket, pageSize, metaPageCursor)
			if err != nil {
				return nil, nil, fmt.Errorf("error getting post metas: %w", err)
			}

			// Pick out the posts from the non-moots
			for _, post := range postMetas {
				if _, ok := nonMootMap[post.ActorDID]; ok {
					filteredPostURIs = append(filteredPostURIs, fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDID, post.Rkey))
				}

				createdAt = post.IndexedAt
				authorDID = post.ActorDID
				newRkey = post.Rkey

				if len(filteredPostURIs) >= int(limit) {
					break
				}
			}

			if len(filteredPostURIs) >= int(limit) {
				break
			}

			metaPageCursor = nextCursor
			if nextCursor.IsZero() {
				bucket = bucket - 1
				metaPageCursor = time.Now()
				if bucket < 0 {
					break
				}
			}
		}

		if newRkey == "" {
			// Set the rkey cursor to the next bucket
			newRkey = sharddb.GetHighestRKeyForBucket(bucket - 1)
		}
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, uri := range filteredPostURIs {
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{Post: uri})
	}

	newCursor := AssembleCursor(createdAt, authorDID, newRkey)

	return posts, &newCursor, nil
}

func (f *FollowersFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}

	for _, feed := range supportedFeeds {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + feed,
		})
	}

	return feeds, nil
}
