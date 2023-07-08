package firehose

import (
	"context"
	"errors"
	"fmt"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type FirehoseFeed struct {
	FeedActorDID                 string
	PostRegistry                 *search.PostRegistry
	BloomFilterSize              uint
	BloomFilterFalsePositiveRate float64
}

type NotFoundError struct {
	error
}

func NewFirehoseFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*FirehoseFeed, []string, error) {
	return &FirehoseFeed{
		FeedActorDID:                 feedActorDID,
		PostRegistry:                 postRegistry,
		BloomFilterSize:              1_000,
		BloomFilterFalsePositiveRate: 0.01,
	}, []string{"firehose"}, nil
}

func (plf *FirehoseFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("firehose-feed")
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	cursorCreatedAt, cursorBloomFilter, _, err := feeds.ParseTimebasedCursor(cursor, plf.BloomFilterSize, plf.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	postsFromRegistry, err := plf.PostRegistry.GetPostPageCursor(ctx, int32(limit), cursorCreatedAt)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
		}
		return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newHotness := -1.0
	lastPostCreatedAt := time.Unix(0, 0)
	for _, post := range postsFromRegistry {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		if post.Hotness != nil {
			newHotness = *post.Hotness
		}
		lastPostCreatedAt = post.CreatedAt
	}

	// Get the cursor for the next page
	newCursor, err := feeds.AssembleTimebasedCursor(lastPostCreatedAt, cursorBloomFilter, newHotness)
	if err != nil {
		return nil, nil, fmt.Errorf("error assembling cursor: %w", err)
	}

	return posts, &newCursor, nil
}

func (plf *FirehoseFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	tracer := otel.Tracer("firehose-feed")
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + "firehose",
		},
	}

	return feeds, nil
}
