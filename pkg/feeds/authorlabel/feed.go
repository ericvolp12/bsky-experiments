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
	"go.opentelemetry.io/otel/attribute"
)

type AuthorLabelFeed struct {
	FeedActorDID                 string
	PostRegistry                 *search.PostRegistry
	BloomFilterSize              uint
	BloomFilterFalsePositiveRate float64
	DefaultLookbackHours         int32
}

type FeedEntity struct {
	Name      string
	IsPrivate bool
}

var feedAliases = map[string]FeedEntity{
	"cl-tpot": {
		Name:      "a:tpot",
		IsPrivate: false,
	},
}

var privateFeedInstructionsPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3jwvwlajglc2w"
var unauthorizedResponse = []*appbsky.FeedDefs_SkeletonFeedPost{{Post: privateFeedInstructionsPost}}

type NotFoundError struct {
	error
}

func NewAuthorLabelFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*AuthorLabelFeed, []string, error) {
	labelsFromRegistry, err := postRegistry.GetAllLabels(ctx, 10000, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting author labels: %w", err)
	}

	// Author labels are prefixed with "a:" to distinguish them from other feeds
	labels := []string{}
	for _, label := range labelsFromRegistry {
		labels = append(labels, "a:"+label.LookupAlias)
		labels = append(labels, "a-"+label.LookupAlias)
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

var tracer = otel.Tracer("author-label-feed")

func (alf *AuthorLabelFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	if userDID == "" {
		span.SetAttributes(attribute.Bool("feed.author.not_authorized", true))
		return unauthorizedResponse, nil, nil
	}

	postID, cursorBloomFilter, _, err := feeds.ParseCursor(cursor, alf.BloomFilterSize, alf.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	isPrivate := true

	// Check if the feed is an alias
	if alias, ok := feedAliases[feed]; ok {
		feed = alias.Name
		isPrivate = alias.IsPrivate
	}

	// Get the author label from the feed
	authorLabel := strings.TrimPrefix(feed, "a:")
	if strings.HasPrefix(feed, "a-") {
		authorLabel = strings.TrimPrefix(feed, "a-")
	}

	includeReplies := true
	sort := "hotness"

	// Author Label feeds are private feeds for now, so we need to check that the user is assigned to the label
	if isPrivate {
		labels, err := alf.PostRegistry.GetLabelsForAuthor(ctx, userDID)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.author.label_lookup.error", true))
			return nil, nil, fmt.Errorf("error getting labels for author: %w", err)
		}

		// Check that the author is assigned to this label
		found := false
		for _, label := range labels {
			if label.LookupAlias == authorLabel {
				found = true
				break
			}
		}

		if !found {
			span.SetAttributes(attribute.Bool("feed.author.not_assigned_label", true))
			return unauthorizedResponse, nil, nil
		}

		includeReplies = false
		sort = "revchron"
	}

	span.SetAttributes(attribute.String("sort", sort), attribute.Bool("include_replies", includeReplies))

	var postsFromRegistry []*search.Post

	if sort == "hotness" {
		postsFromRegistry, err = alf.PostRegistry.GetPostsPageForAuthorLabelFromView(ctx, authorLabel, alf.DefaultLookbackHours, int32(limit), postID, includeReplies)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
	} else if sort == "revchron" {
		postsFromRegistry, err = alf.PostRegistry.GetPostsPageForAuthorLabel(ctx, authorLabel, alf.DefaultLookbackHours, int32(limit), postID, includeReplies)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
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
	labelsFromRegistry, err := plf.PostRegistry.GetAllLabels(ctx, 10000, 0)
	if err != nil {
		return nil, fmt.Errorf("error getting author labels: %w", err)
	}

	// Author labels are prefixed with "a:" to distinguish them from other feeds
	labels := []string{}
	for _, label := range labelsFromRegistry {
		labels = append(labels, "a:"+label.LookupAlias)
		labels = append(labels, "a-"+label.LookupAlias)
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
