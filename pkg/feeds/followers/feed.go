package followers

import (
	"context"
	"fmt"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type FollowersFeed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"my-followers"}

var tracer = otel.Tracer("my-followers")

func NewFollowersFeed(ctx context.Context, feedActorDID string, store *store.Store) (*FollowersFeed, []string, error) {
	return &FollowersFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, supportedFeeds, nil
}

func (f *FollowersFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	if userDID == "" {
		return nil, nil, fmt.Errorf("feed %s requires authentication", feed)
	}

	var err error
	createdAt := time.Now()
	var authorDID string
	var rkey string

	if cursor != "" {
		createdAt, authorDID, rkey, err = ParseCursor(cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	span.SetAttributes(attribute.String("createdAt", createdAt.Format(time.RFC3339)))
	span.SetAttributes(attribute.String("authorDID", authorDID))
	span.SetAttributes(attribute.String("rkey", rkey))

	rawPosts, err := f.Store.Queries.GetPostsFromNonMoots(ctx, store_queries.GetPostsFromNonMootsParams{
		ActorDid:        userDID,
		Limit:           int32(limit),
		CursorCreatedAt: createdAt,
		CursorActorDid:  authorDID,
		CursorRkey:      rkey,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts: %w", err)
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, post := range rawPosts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})

		if post.CreatedAt.Valid {
			createdAt = post.CreatedAt.Time
		}
		authorDID = post.ActorDid
		rkey = post.Rkey
	}

	// If we got less than the limit, we're at the end of the feed
	if int64(len(posts)) < limit {
		return posts, nil, nil
	}

	// Otherwise, we need to return a cursor to the lowest score of the posts we got
	newCursor := AssembleCursor(createdAt, authorDID, rkey)

	return posts, &newCursor, nil
}

func (f *FollowersFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}

	for _, feed := range supportedFeeds {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + feed,
		})
	}

	return feeds, nil
}
