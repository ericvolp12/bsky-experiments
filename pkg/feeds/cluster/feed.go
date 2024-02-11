package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type ClusterFeed struct {
	FeedActorDID         string
	PostRegistry         *search.PostRegistry
	DefaultLookbackHours int32
	Store                *store.Store
}

type NotFoundError struct {
	error
}

var langAliases = map[string]string{
	"japanese": "ja",
	"turkish":  "tr",
	"brasil":   "pt",
	"korean":   "ko",
	"persian":  "fa",
	"ukraine":  "uk",
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
	// "cl-tpot":      "cluster-tpot",
	"cl-persian": "cluster-persian",
	"cl-ukraine": "cluster-ukraine",
}

func NewClusterFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry, store *store.Store) (*ClusterFeed, []string, error) {
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
		FeedActorDID:         feedActorDID,
		PostRegistry:         postRegistry,
		Store:                store,
		DefaultLookbackHours: 24,
	}, clusterFeeds, nil
}

var tracer = otel.Tracer("cluster-feed")

func (cf *ClusterFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	createdAt, actorDid, rkey, err := ParseCursor(cursor)
	if err != nil {
		slog.Error("failed to parse cursor, using defaults", "err", err)
	}

	// Try to find the feed in the aliases map
	if alias, ok := feedAliases[feed]; ok {
		feed = alias
	}

	// Slice the cluster feed prefix off the feed name
	clusterName := strings.TrimPrefix(feed, "cluster-")

	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}

	// Check if this is a language feed
	if lang, ok := langAliases[clusterName]; ok {
		span.SetAttributes(
			attribute.String("lang", lang),
			attribute.String("cluster", clusterName),
			attribute.String("createdAt", createdAt.String()),
			attribute.String("actorDid", actorDid),
			attribute.String("rkey", rkey),
			attribute.Int64("limit", limit),
		)

		postsFromStore, err := cf.Store.Queries.GetPopularRecentPostsByLanguage(ctx, store_queries.GetPopularRecentPostsByLanguageParams{
			MinFollowers: 500,
			// MinLikes:        5,
			Lang:            lang,
			CursorCreatedAt: createdAt,
			CursorActorDid:  actorDid,
			CursorRkey:      rkey,
			Limit:           int32(limit),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("error getting posts from store for feed (%s): %w", feed, err)
		}

		for _, post := range postsFromStore {
			posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
				Post: fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey),
			})
			createdAt = post.CreatedAt.Time
			actorDid = post.ActorDid
			rkey = post.Rkey
		}
	} else {
		postsFromRegistry, err := cf.PostRegistry.GetPostsPageForCluster(ctx, clusterName, cf.DefaultLookbackHours, int32(limit), createdAt)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}

		for _, post := range postsFromRegistry {
			posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
				Post: fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID),
			})
			createdAt = post.CreatedAt
			actorDid = post.AuthorDID
			rkey = post.ID
		}
	}

	newCursor := AssembleCursor(createdAt, actorDid, rkey)

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
