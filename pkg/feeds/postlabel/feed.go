package postlabel

import (
	"context"
	"errors"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type PostLabelFeed struct {
	FeedActorDID                 string
	PostRegistry                 *search.PostRegistry
	BloomFilterSize              uint
	BloomFilterFalsePositiveRate float64
}

// Internal constants
var animalLabels = []string{
	"cv:bird",
	"cv:cat",
	"cv:dog",
	"cv:horse",
	"cv:sheep",
	"cv:cow",
	"cv:elephant",
	"cv:bear",
	"cv:zebra",
	"cv:giraffe",
}

var foodLabels = []string{
	"cv:banana",
	"cv:apple",
	"cv:sandwich",
	"cv:orange",
	"cv:broccoli",
	"cv:carrot",
	"cv:hot dog",
	"cv:pizza",
	"cv:donut",
	"cv:cake",
}

var feedAliases = map[string]string{
	"positivifeed": "sentiment:pos",
	"negativifeed": "sentiment:neg",
}

type NotFoundError struct {
	error
}

func NewPostLabelFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*PostLabelFeed, []string, error) {
	labels, err := postRegistry.GetUniquePostLabels(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting unique post labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	labels = append(labels, "animals", "food")

	return &PostLabelFeed{
		FeedActorDID:                 feedActorDID,
		PostRegistry:                 postRegistry,
		BloomFilterSize:              1_000,
		BloomFilterFalsePositiveRate: 0.01,
	}, labels, nil
}

func (plf *PostLabelFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("post-label-feed")
	ctx, span := tracer.Start(ctx, "PostLabelFeed:GetPage")
	defer span.End()

	_, cursorBloomFilter, cursorHotness, err := feeds.ParseCursor(cursor, plf.BloomFilterSize, plf.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	// Check if the feed is an alias
	if alias, ok := feedAliases[feed]; ok {
		feed = alias
	}

	var postsFromRegistry []*search.Post

	switch feed {
	case "animals":
		postsFromRegistry, err = plf.PostRegistry.GetPostsPageForPostLabelsByHotness(ctx, animalLabels, int32(limit), cursorHotness)
	case "food":
		postsFromRegistry, err = plf.PostRegistry.GetPostsPageForPostLabelsByHotness(ctx, foodLabels, int32(limit), cursorHotness)
	default:
		postsFromRegistry, err = plf.PostRegistry.GetPostsPageForPostLabelByHotness(ctx, feed, int32(limit), cursorHotness)
	}

	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
		}
		return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newHotness := -1.0
	lastPostID := ""
	for _, post := range postsFromRegistry {
		// Check if the post is in the bloom filter
		if !cursorBloomFilter.TestString(post.ID) {
			postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
			posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
				Post: postAtURL,
			})
			if post.Hotness != nil {
				newHotness = *post.Hotness
			}
			cursorBloomFilter.AddString(post.ID)
			lastPostID = post.ID
		}
	}

	// Get the cursor for the next page
	newCursor, err := feeds.AssembleCursor(lastPostID, cursorBloomFilter, newHotness)
	if err != nil {
		return nil, nil, fmt.Errorf("error assembling cursor: %w", err)
	}

	return posts, &newCursor, nil
}

func (plf *PostLabelFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	tracer := otel.Tracer("post-label-feed")
	ctx, span := tracer.Start(ctx, "PostLabelFeed:Describe")
	defer span.End()

	labels, err := plf.PostRegistry.GetUniquePostLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting unique post labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	labels = append(labels, "animals", "food")

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, label := range labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + label,
		})
	}

	return feeds, nil
}
