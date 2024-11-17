package bangers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"encoding/json"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

const (
	atBangersKey = "at-bangers"
	atBangersTTL = 30 * time.Minute
	userTTL      = 10 * time.Minute
	maxPosts     = 3000
)

type Feed struct {
	FeedActorDID string
	Store        *store.Store
	Redis        *redis.Client
}

type NotFoundError struct {
	error
}

func NewFeed(ctx context.Context, feedActorDID string, store *store.Store, redis *redis.Client) (*Feed, []string, error) {
	return &Feed{
		FeedActorDID: feedActorDID,
		Store:        store,
		Redis:        redis,
	}, []string{"bangers", "at-bangers"}, nil
}

var tracer = otel.Tracer("bangers-feed")

type postRef struct {
	ActorDid string `json:"did"`
	Rkey     string `json:"rkey"`
}

var pinnedPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3lb45ahcquc2n"

func (f *Feed) fetchAndCachePosts(ctx context.Context, userDID string, feed string) ([]postRef, error) {
	var posts []store_queries.Post
	var err error

	if feed == "bangers" {
		posts, err = f.Store.Queries.GetTopPostsForActor(ctx, store_queries.GetTopPostsForActorParams{
			ActorDid: userDID,
			Limit:    maxPosts,
			Offset:   0,
		})
	} else {
		posts, err = f.Store.Queries.GetTopPosts(ctx, store_queries.GetTopPostsParams{
			Limit:  maxPosts,
			Offset: 0,
		})
	}

	if err != nil {
		return nil, err
	}

	cacheKey := feed
	if feed == "bangers" {
		cacheKey += ":" + userDID
	}

	p := f.Redis.Pipeline()

	postRefs := []postRef{}
	for _, post := range posts {
		postRef := postRef{
			ActorDid: post.ActorDid,
			Rkey:     post.Rkey,
		}
		cacheValue, err := json.Marshal(postRef)
		if err != nil {
			return nil, fmt.Errorf("error marshalling post: %w", err)
		}
		p.RPush(ctx, cacheKey, cacheValue)
		postRefs = append(postRefs, postRef)
	}

	ttl := atBangersTTL
	if feed == "bangers" {
		ttl = userTTL
	}

	p.Expire(ctx, cacheKey, ttl)

	_, err = p.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error caching posts for feed (%s): %w", feed, err)
	}

	return postRefs, nil
}

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
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

	cacheKey := feed
	if feed == "bangers" {
		feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{
			{
				Post: pinnedPost,
				Reason: &appbsky.FeedDefs_SkeletonFeedPost_Reason{
					FeedDefs_SkeletonReasonPin: &appbsky.FeedDefs_SkeletonReasonPin{},
				},
			},
		}
		return feedPosts, nil, nil
		// if userDID == "" {
		// 	return nil, nil, fmt.Errorf("authorization required for feed: %s", feed)
		// }
		// cacheKey += ":" + userDID
	}

	cached, err := f.Redis.LRange(ctx, cacheKey, offset, offset+limit-1).Result()

	if err == redis.Nil || len(cached) == 0 {
		posts, err = f.fetchAndCachePosts(ctx, userDID, feed)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
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

	// Serve posts from the cache if within the 3k window
	if offset+limit > maxPosts {
		var dbPosts []store_queries.Post
		switch feed {
		case "bangers":
			dbPosts, err = f.Store.Queries.GetTopPostsForActor(ctx, store_queries.GetTopPostsForActorParams{
				ActorDid: userDID,
				Limit:    int32(limit),
				Offset:   int32(offset),
			})
		case "at-bangers":
			// Fetch posts beyond the 3k window from the database
			dbPosts, err = f.Store.Queries.GetTopPosts(ctx, store_queries.GetTopPostsParams{
				Limit:  int32(limit),
				Offset: int32(offset),
			})
		}
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
		posts = []postRef{}
		for _, post := range dbPosts {
			posts = append(posts, postRef{
				ActorDid: post.ActorDid,
				Rkey:     post.Rkey,
			})
		}
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

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "bangers",
		},
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "at-bangers",
		},
	}

	return feeds, nil
}
