package pins

import (
	"context"
	"fmt"
	"strconv"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type PinsFeed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

func NewPinsFeed(ctx context.Context, feedActorDID string, store *store.Store) (*PinsFeed, []string, error) {
	return &PinsFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, []string{"my-pins"}, nil
}

var tracer = otel.Tracer("pins-feed")

type postRef struct {
	ActorDid string `json:"did"`
	Rkey     string `json:"rkey"`
}

func (f *PinsFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
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

	span.SetAttributes(attribute.Int64("offset", offset), attribute.Int64("limit", limit))

	posts, err := f.Store.Queries.GetPinnedPostsByActor(ctx, store_queries.GetPinnedPostsByActorParams{
		ActorDid: userDID,
		Limit:    int32(limit),
		Offset:   int32(offset),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting pinned posts: %w", err)
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

func (f *PinsFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "my-pins",
		},
	}

	return feeds, nil
}
