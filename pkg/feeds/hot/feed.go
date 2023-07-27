package hot

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
)

type HotFeed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"whats-hot"}

var tracer = otel.Tracer("hot-feed")

func NewHotFeed(ctx context.Context, feedActorDID string, store *store.Store) (*HotFeed, []string, error) {
	return &HotFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, supportedFeeds, nil
}

func (f *HotFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	// For this feed, the cursor is a score
	score := float64(0)
	var err error

	if cursor != "" {
		score, err = strconv.ParseFloat(cursor, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	rawPosts, err := f.Store.Queries.GetHotPage(ctx, store_queries.GetHotPageParams{
		Limit: int32(limit),
		Score: sql.NullFloat64{
			Float64: score,
			Valid:   score > 0,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting hot page: %w", err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, post := range rawPosts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		score = post.Score
	}

	// If we got less than the limit, we're at the end of the feed
	if int64(len(posts)) < limit {
		return posts, nil, nil
	}

	// Otherwise, we need to return a cursor to the lowest score of the posts we got
	newCursor := strconv.FormatFloat(score, 'f', -1, 64)

	return posts, &newCursor, nil
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
