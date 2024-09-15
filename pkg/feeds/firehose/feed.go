package firehose

import (
	"context"
	"fmt"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
)

type FirehoseFeed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

func NewFirehoseFeed(ctx context.Context, feedActorDID string, store *store.Store) (*FirehoseFeed, []string, error) {
	return &FirehoseFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, []string{"firehose"}, nil
}

func (f *FirehoseFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("firehose-feed")
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	insertedAt := time.Now()
	var err error
	if cursor != "" {
		insertedAt, err = time.Parse(time.RFC3339, cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	posts, err := f.Store.Queries.ListRecentPosts(ctx, store_queries.ListRecentPostsParams{
		InsertedAt: insertedAt,
		Limit:      int32(limit),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts: %w", err)
	}

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newCursor := ""
	for _, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		feedPosts = append(feedPosts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		newCursor = post.InsertedAt.Format(time.RFC3339)
	}

	if int64(len(posts)) < limit {
		return feedPosts, nil, nil
	}

	return feedPosts, &newCursor, nil
}

func (f *FirehoseFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	tracer := otel.Tracer("firehose-feed")
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "firehose",
		},
	}

	return feeds, nil
}
