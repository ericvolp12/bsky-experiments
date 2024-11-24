package postlabel

import (
	"context"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"go.opentelemetry.io/otel"
)

type Feed struct {
	FeedActorDID string
	Store        *store.Store
}

type NotFoundError struct {
	error
}

var labels = []string{
	"cv:cat",
	"cv:dog",
	"cv:bird",
}

var tracer = otel.Tracer("post-label-feed")

var pinnedPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3lbi35q4vp22j"

func NewFeed(ctx context.Context, feedActorDID string, store *store.Store) (*Feed, []string, error) {
	return &Feed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}, labels, nil
}

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	// Temporary hardcoded pinned post
	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{
		{
			Post: pinnedPost,
			Reason: &appbsky.FeedDefs_SkeletonFeedPost_Reason{
				FeedDefs_SkeletonReasonPin: &appbsky.FeedDefs_SkeletonReasonPin{},
			},
		},
	}

	return feedPosts, nil, nil

	// var err error
	// score := float64(-1)
	// if cursor != "" {
	// 	score, err = strconv.ParseFloat(cursor, 64)
	// 	if err != nil {
	// 		return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
	// 	}
	// }

	// dbPosts, err := f.Store.Queries.ListRecentPostsByLabelHot(ctx, store_queries.ListRecentPostsByLabelHotParams{
	// 	Label: feed,
	// 	Limit: int32(limit),
	// 	Score: sql.NullFloat64{Float64: score, Valid: score > 0},
	// })

	// // Convert to appbsky.FeedDefs_SkeletonFeedPost
	// posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	// lastScore := -1.0
	// for _, post := range dbPosts {
	// 	postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
	// 	posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{Post: postAtURL})
	// 	lastScore = post.Score
	// }

	// if len(posts) < int(limit) {
	// 	return posts, nil, nil
	// }

	// newCursor := ""
	// if lastScore > 0 {
	// 	newCursor = fmt.Sprintf("%0.10f", lastScore)
	// }

	// return posts, &newCursor, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, label := range labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + label,
		})
	}

	return feeds, nil
}
