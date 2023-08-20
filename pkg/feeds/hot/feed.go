package hot

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

const (
	hotCacheKey = "whats-hot"
	hotCacheTTL = 1 * time.Minute
	maxPosts    = 3000
)

type HotFeed struct {
	FeedActorDID string
	Store        *store.Store
	Redis        *redis.Client
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"whats-hot"}

var tracer = otel.Tracer("hot-feed")

type postRef struct {
	ActorDid string `json:"did"`
	Rkey     string `json:"rkey"`
}

func NewHotFeed(ctx context.Context, feedActorDID string, store *store.Store, redis *redis.Client) (*HotFeed, []string, error) {
	return &HotFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
		Redis:        redis,
	}, supportedFeeds, nil
}

func (f *HotFeed) fetchAndCachePosts(ctx context.Context) ([]postRef, error) {
	rawPosts, err := f.Store.Queries.GetHotPage(ctx, store_queries.GetHotPageParams{
		Limit: int32(maxPosts),
		Score: sql.NullFloat64{
			Float64: 0,
			Valid:   false,
		},
	})
	if err != nil {
		return nil, err
	}

	p := f.Redis.Pipeline()

	postRefs := []postRef{}
	for _, post := range rawPosts {
		postRef := postRef{
			ActorDid: post.ActorDid,
			Rkey:     post.Rkey,
		}
		cacheValue, err := json.Marshal(postRef)
		if err != nil {
			return nil, fmt.Errorf("error marshalling post: %w", err)
		}
		p.RPush(ctx, hotCacheKey, cacheValue)
		postRefs = append(postRefs, postRef)
	}

	p.Expire(ctx, hotCacheKey, hotCacheTTL)

	_, err = p.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error caching posts for feed (whats-hot): %w", err)
	}

	return postRefs, nil
}

func (f *HotFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	offset := int64(0)
	var err error

	if cursor != "" {
		offset, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	var posts []postRef

	cached, err := f.Redis.LRange(ctx, hotCacheKey, offset, offset+limit-1).Result()

	if err == redis.Nil || len(cached) == 0 {
		posts, err = f.fetchAndCachePosts(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
		// Truncate the posts to the limit and offset
		if len(posts) > int(limit) {
			posts = posts[offset : offset+limit]
		}
	} else if err == nil {
		posts = make([]postRef, len(cached))
		for i, cachedValue := range cached {
			json.Unmarshal([]byte(cachedValue), &posts[i])
		}
	} else {
		return nil, nil, fmt.Errorf("error getting posts from cache for feed (%s): %w", feed, err)
	}

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		feedPosts = append(feedPosts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
	}

	if int64(len(posts)) < limit {
		return feedPosts, nil, nil
	}

	newCursor := strconv.FormatInt(offset+limit, 10)
	return feedPosts, &newCursor, nil
}

func (f *HotFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
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
