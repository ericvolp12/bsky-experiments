package hot

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
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

var supportedFeeds = []string{"whats-hot", "wh-ja", "wh-ja-txt"}

var tracer = otel.Tracer("hot-feed")

type postRef struct {
	ActorDid string   `json:"did"`
	Rkey     string   `json:"rkey"`
	Langs    []string `json:"langs"`
	HasMedia bool     `json:"has_media"`
}

func NewHotFeed(ctx context.Context, feedActorDID string, store *store.Store, redis *redis.Client) (*HotFeed, []string, error) {
	f := HotFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
		Redis:        redis,
	}

	_, err := f.fetchAndCachePosts(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching and caching posts for feed (whats-hot): %w", err)
	}

	go func() {
		t := time.NewTicker(hotCacheTTL)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()
				_, err := f.fetchAndCachePosts(ctx)
				if err != nil {
					fmt.Printf("error fetching and caching posts for feed (whats-hot): %v\n", err)
				}
			}
		}
	}()

	return &f, supportedFeeds, nil
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
			Langs:    post.Langs,
			HasMedia: post.HasEmbeddedMedia,
		}
		cacheValue, err := json.Marshal(postRef)
		if err != nil {
			return nil, fmt.Errorf("error marshalling post: %w", err)
		}
		p.RPush(ctx, hotCacheKey, cacheValue)
		postRefs = append(postRefs, postRef)
	}

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

	lang := ""
	if strings.HasPrefix(feed, "wh-") {
		lang = strings.TrimPrefix(feed, "wh-")
		lang = strings.TrimSuffix(lang, "-txt")
	}

	textOnly := false
	if strings.HasSuffix(feed, "-txt") {
		textOnly = true
	}

	cached, err := f.Redis.LRange(ctx, hotCacheKey, offset, offset+(limit*3)).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts from cache for feed (%s): %w", feed, err)
	}

	posts := make([]postRef, len(cached))
	for i, cachedValue := range cached {
		json.Unmarshal([]byte(cachedValue), &posts[i])
	}

	postsSeen := int64(len(posts))

	// If the feed is a language feed, filter out posts that don't match the language
	lastPostAdded := int(offset) + len(posts) - 1
	if lang != "" {
		filteredPosts := []postRef{}
		for i, post := range posts {
			if textOnly && post.HasMedia {
				continue
			}

			if slices.Contains(post.Langs, lang) {
				filteredPosts = append(filteredPosts, post)
				lastPostAdded = int(offset) + i
				if int64(len(filteredPosts)) >= limit {
					break
				}
			}
		}
		posts = filteredPosts
	}

	feedPosts := make([]*appbsky.FeedDefs_SkeletonFeedPost, len(posts))
	for i, post := range posts {
		feedPosts[i] = &appbsky.FeedDefs_SkeletonFeedPost{
			Post: fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey),
		}
	}

	if postsSeen < limit {
		return feedPosts, nil, nil
	}

	newCursor := strconv.FormatInt(int64(lastPostAdded)+1, 10)
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
