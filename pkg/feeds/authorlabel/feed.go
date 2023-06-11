package authorlabel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type AuthorLabelFeed struct {
	FeedActorDID                 string
	PostRegistry                 *search.PostRegistry
	BloomFilterSize              uint
	BloomFilterFalsePositiveRate float64
	DefaultLookbackHours         int32
}

var feedAliases = map[string]string{}

type NotFoundError struct {
	error
}

func NewAuthorLabelFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*AuthorLabelFeed, []string, error) {
	labels, err := postRegistry.GetUniqueAuthorLabels(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting unique author labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	return &AuthorLabelFeed{
		FeedActorDID:                 feedActorDID,
		PostRegistry:                 postRegistry,
		BloomFilterSize:              1_000,
		BloomFilterFalsePositiveRate: 0.01,
		DefaultLookbackHours:         24,
	}, labels, nil
}

func (alf *AuthorLabelFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("author-label-feed")
	ctx, span := tracer.Start(ctx, "AuthorLabelFeed:GetPage")
	defer span.End()

	postID, cursorBloomFilter, _, err := feeds.ParseCursor(cursor, alf.BloomFilterSize, alf.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	// Check if the feed is an alias
	if alias, ok := feedAliases[feed]; ok {
		feed = alias
	}

	// Get the author label from the feed
	authorLabel := strings.TrimPrefix(feed, "a:")

	postsFromRegistry, err := alf.PostRegistry.GetPostsPageForAuthorLabel(ctx, authorLabel, alf.DefaultLookbackHours, int32(limit), postID)
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

func (plf *AuthorLabelFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	labels, err := plf.PostRegistry.GetUniqueAuthorLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting unique author labels: %w", err)
	}

	for alias := range feedAliases {
		labels = append(labels, alias)
	}

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, label := range labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + label,
		})
	}

	return feeds, nil
}
