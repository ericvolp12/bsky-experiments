package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/feeds"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type ClusterFeed struct {
	FeedActorDID                 string
	PostRegistry                 *search.PostRegistry
	BloomFilterSize              uint
	BloomFilterFalsePositiveRate float64
	DefaultLookbackHours         int32
}

type NotFoundError struct {
	error
}

var feedAliases = map[string]string{
	"cl-web3":      "cluster-web3",
	"cl-tqsp":      "cluster-tq-shitposters",
	"cl-eng":       "cluster-eng",
	"cl-wrestling": "cluster-wrestling",
	"cl-turkish":   "cluster-turkish",
	"cl-japanese":  "cluster-japanese",
	"cl-brasil":    "cluster-brasil",
	"cl-korean":    "cluster-korean",
	"cl-tpot":      "cluster-tpot",
	"cl-persian":   "cluster-persian",
	"cl-ukraine":   "cluster-ukraine",
}

func NewClusterFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*ClusterFeed, []string, error) {
	clusters, err := postRegistry.GetClusters(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting clusters: %w", err)
	}

	clusterFeeds := make([]string, len(clusters))
	for i, cluster := range clusters {
		clusterFeeds[i] = "cluster-" + cluster.LookupAlias
	}

	for alias := range feedAliases {
		clusterFeeds = append(clusterFeeds, alias)
	}

	return &ClusterFeed{
		FeedActorDID:                 feedActorDID,
		PostRegistry:                 postRegistry,
		BloomFilterSize:              1_000,
		BloomFilterFalsePositiveRate: 0.01,
		DefaultLookbackHours:         24,
	}, clusterFeeds, nil
}

func (cf *ClusterFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("cluster-feed")
	ctx, span := tracer.Start(ctx, "ClusterFeed:GetPage")
	defer span.End()

	createdAt, cursorBloomFilter, _, err := feeds.ParseTimebasedCursor(cursor, cf.BloomFilterSize, cf.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	}

	// Try to find the feed in the aliases map
	if alias, ok := feedAliases[feed]; ok {
		feed = alias
	}

	// Slice the cluster feed prefix off the feed name
	clusterName := strings.TrimPrefix(feed, "cluster-")

	postsFromRegistry, err := cf.PostRegistry.GetPostsPageForCluster(ctx, clusterName, cf.DefaultLookbackHours, int32(limit), createdAt)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
		}
		return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newHotness := -1.0
	var lastPostCreatedAt time.Time
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
			lastPostCreatedAt = post.CreatedAt
		}
	}

	// Get the cursor for the next page
	newCursor, err := feeds.AssembleTimebasedCursor(lastPostCreatedAt, cursorBloomFilter, newHotness)
	if err != nil {
		return nil, nil, fmt.Errorf("error assembling cursor: %w", err)
	}

	return posts, &newCursor, nil
}

func (cf *ClusterFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	clusters, err := cf.PostRegistry.GetClusters(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting clusters: %w", err)
	}

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, cluster := range clusters {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + cf.FeedActorDID + "/app.bsky.feed.generator/" + "cluster-" + cluster.LookupAlias,
		})
	}

	for alias := range feedAliases {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + cf.FeedActorDID + "/app.bsky.feed.generator/" + alias,
		})
	}

	return feeds, nil
}
