package followers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	graphdclient "github.com/ericvolp12/bsky-experiments/pkg/graphd/client"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type FollowersFeed struct {
	FeedActorDID string
	Store        *store.Store
	GraphD       *graphdclient.Client
	Redis        *redis.Client
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"my-followers"}

var tracer = otel.Tracer("my-followers")

func NewFollowersFeed(ctx context.Context, feedActorDID string, store *store.Store, gClient *graphdclient.Client, rClient *redis.Client) (*FollowersFeed, []string, error) {
	return &FollowersFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
		GraphD:       gClient,
		Redis:        rClient,
	}, supportedFeeds, nil
}

var activePostersKey = "consumer:active_posters"

func (f *FollowersFeed) intersectActivePosters(
	ctx context.Context,
	dids []string,
) ([]string, error) {
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

	return intersection, nil
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

	if cursor != "" {
		createdAt, authorDID, rkey, err = ParseCursor(cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	span.SetAttributes(attribute.String("createdAt", createdAt.Format(time.RFC3339)))
	span.SetAttributes(attribute.String("authorDID", authorDID))
	span.SetAttributes(attribute.String("rkey", rkey))

	var rawPosts []store_queries.RecentPost

	nonMoots, err := f.GraphD.GetFollowersNotFollowing(ctx, userDID)
	if err == nil {
		// Get the intersection of the non-moots and the active posters
		nonMoots, err = f.intersectActivePosters(ctx, nonMoots)
		if err != nil {
			return nil, nil, fmt.Errorf("error intersecting active posters: %w", err)
		}

		rawPosts, err = f.Store.Queries.GetRecentPostsFromNonSpamUsers(ctx, store_queries.GetRecentPostsFromNonSpamUsersParams{
			Dids:            nonMoots,
			Limit:           int32(limit),
			CursorCreatedAt: createdAt,
			CursorActorDid:  authorDID,
			CursorRkey:      rkey,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error getting posts: %w", err)
		}
	} else {
		span.SetAttributes(attribute.Bool("fallback", true))
		// Fallback to old query
		slog.Error("error getting non-moots, falling back to old query", "error", err)
		rawPosts, err = f.Store.Queries.GetRecentPostsFromNonMoots(ctx, store_queries.GetRecentPostsFromNonMootsParams{
			ActorDid:        userDID,
			Limit:           int32(limit),
			CursorCreatedAt: createdAt,
			CursorActorDid:  authorDID,
			CursorRkey:      rkey,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error getting posts: %w", err)
		}
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, post := range rawPosts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})

		if post.CreatedAt.Valid {
			createdAt = post.CreatedAt.Time
		}
		authorDID = post.ActorDid
		rkey = post.Rkey
	}

	// If we got less than the limit, we're at the end of the feed
	if int64(len(posts)) < limit {
		return posts, nil, nil
	}

	// Otherwise, we need to return a cursor to the lowest score of the posts we got
	newCursor := AssembleCursor(createdAt, authorDID, rkey)

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
