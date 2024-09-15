package language

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type LanguageFeed struct {
	FeedActorDID         string
	DefaultLookbackHours int32
	Store                *store.Store
}

type NotFoundError struct {
	error
}

var langAliases = map[string]string{
	"japanese": "ja",
	"brasil":   "pt",
}

var feedAliases = map[string]string{
	"cl-japanese": "cluster-japanese",
	"cl-brasil":   "cluster-brasil",
}

func NewLanguageFeed(ctx context.Context, feedActorDID string, store *store.Store) (*LanguageFeed, []string, error) {
	feeds := []string{}
	for alias := range feedAliases {
		feeds = append(feeds, alias)
	}

	return &LanguageFeed{
		FeedActorDID:         feedActorDID,
		Store:                store,
		DefaultLookbackHours: 24,
	}, feeds, nil
}

var tracer = otel.Tracer("cluster-feed")

func (f *LanguageFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
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

		postsFromStore, err := f.Store.Queries.GetPopularRecentPostsByLanguage(ctx, store_queries.GetPopularRecentPostsByLanguageParams{
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
		// Not supported
		return nil, nil, NotFoundError{errors.New("feed not found")}
	}

	newCursor := AssembleCursor(createdAt, actorDid, rkey)

	return posts, &newCursor, nil
}

func (f *LanguageFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for alias := range feedAliases {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + alias,
		})
	}

	return feeds, nil
}
