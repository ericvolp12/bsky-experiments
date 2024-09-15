package pins

import (
	"context"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Feed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

func NewFeed(ctx context.Context, feedActorDID string, store *store.Store) (*Feed, []string, error) {
	return &Feed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, []string{"my-pins"}, nil
}

var tracer = otel.Tracer("pins-feed")

type postRef struct {
	ActorDid string `json:"did"`
	Rkey     string `json:"rkey"`
}

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()
	var err error

	if cursor == "" {
		cursor = "~"
	}

	span.SetAttributes(attribute.String("cursor", cursor), attribute.Int64("limit", limit))

	posts, err := f.Store.Queries.ListPinsByActor(ctx, store_queries.ListPinsByActorParams{
		ActorDid: userDID,
		Rkey:     cursor,
		Limit:    int32(limit),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting pinned posts: %w", err)
	}

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newCursor := ""
	for _, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		feedPosts = append(feedPosts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		newCursor = post.Rkey
	}

	if int64(len(posts)) < limit {
		return feedPosts, nil, nil
	}

	return feedPosts, &newCursor, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "my-pins",
		},
	}

	return feeds, nil
}
