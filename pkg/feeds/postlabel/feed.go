package postlabel

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type Feed struct {
	FeedActorDID string
	PostRegistry *search.PostRegistry
}

var feedAliases = map[string]string{
	"positivifeed": "sentiment:pos",
	"negativifeed": "sentiment:neg",
}

var deprecatedLabels = map[string]string{
	"hellthread":      "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3k3jf5lgbsw24",
	"hellthread:pics": "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3k3jf5lgbsw24",
}

type NotFoundError struct {
	error
}

var tracer = otel.Tracer("post-label-feed")

func NewFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*Feed, []string, error) {
	labels, err := postRegistry.GetUniquePostLabels(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting unique post labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	return &Feed{
		FeedActorDID: feedActorDID,
		PostRegistry: postRegistry,
	}, labels, nil
}

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	// Check if the feed is an alias
	if alias, ok := feedAliases[feed]; ok {
		feed = alias
	}

	if pin, ok := deprecatedLabels[feed]; ok {
		posts := []*appbsky.FeedDefs_SkeletonFeedPost{
			{
				Post: pin,
			},
		}
		return posts, nil, nil
	}

	var err error

	hotness := float64(-1)
	if cursor != "" {
		hotness, err = strconv.ParseFloat(cursor, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	postsFromRegistry, err := f.PostRegistry.GetPostsPageForPostLabelByHotness(ctx, feed, int32(limit), hotness)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
		}
		return nil, nil, fmt.Errorf("error getting posts for feed (%s): %w", feed, err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newHotness := -1.0
	for _, post := range postsFromRegistry {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		if post.Hotness != nil {
			newHotness = *post.Hotness
		}
	}

	newCursor := ""
	if newHotness > 0 {
		newCursor = fmt.Sprintf("%0.10f", newHotness)
	}

	return posts, &newCursor, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	labels, err := f.PostRegistry.GetUniquePostLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting unique post labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, label := range labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + label,
		})
	}

	return feeds, nil
}
