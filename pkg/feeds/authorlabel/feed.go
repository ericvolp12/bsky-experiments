package authorlabel

import (
	"context"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Feed struct {
	FeedActorDID string
	Store        *store.Store
	Labels       map[string]LabelFeed
}

type LabelFeed struct {
	Label   string
	Private bool
	GetPage func(ctx context.Context, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error)
}

var privateFeedInstructionsPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3jwvwlajglc2w"
var unauthorizedResponse = []*appbsky.FeedDefs_SkeletonFeedPost{{Post: privateFeedInstructionsPost}}

type NotFoundError struct {
	error
}

func NewFeed(ctx context.Context, feedActorDID string, store *store.Store) (*Feed, []string, error) {
	alf := Feed{
		FeedActorDID: feedActorDID,
		Store:        store,
	}

	labelFeeds := map[string]LabelFeed{
		"a-mpls": {
			Label:   "mpls",
			Private: true,
			GetPage: alf.GetMPLSPage,
		},
		"cl-tqsp": {
			Label:   "tqsp",
			Private: false,
			GetPage: alf.GetTQSPPage,
		},
	}

	labels := make([]string, len(labelFeeds))
	for name := range labelFeeds {
		labels = append(labels, name)
	}

	alf.Labels = labelFeeds
	return &alf, labels, nil
}

var tracer = otel.Tracer("author-label-feed")

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	span.SetAttributes(
		attribute.String("feed", feed),
		attribute.String("user_did", userDID),
		attribute.Int64("limit", limit),
		attribute.String("cursor", cursor),
	)

	if userDID == "" {
		span.SetAttributes(attribute.Bool("feed.author.not_authorized", true))
		return unauthorizedResponse, nil, nil
	}

	labelFeed, ok := f.Labels[feed]
	if !ok {
		span.SetAttributes(attribute.String("invalid_label", feed))
		return nil, nil, NotFoundError{fmt.Errorf("feed not found")}
	}

	if labelFeed.Private {
		// Ensure the user is assigned to the label before letting them view the feed
		authorized, err := f.Store.Queries.ActorHasLabel(ctx, store_queries.ActorHasLabelParams{
			ActorDid: userDID,
			Label:    labelFeed.Label,
		})
		if err != nil {
			span.SetAttributes(attribute.String("label_lookup_error", err.Error()))
			return nil, nil, fmt.Errorf("error getting labels for actor: %w", err)
		}

		if !authorized {
			span.SetAttributes(attribute.Bool("actor_not_authorized", true))
			return unauthorizedResponse, nil, nil
		}
	}

	posts, newCursor, err := labelFeed.GetPage(ctx, userDID, limit, cursor)
	if err != nil {
		span.SetAttributes(attribute.String("error", err.Error()))
		return nil, nil, fmt.Errorf("error getting posts for feed (%s): %w", feed, err)
	}

	return posts, newCursor, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for name := range f.Labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + name,
		})
	}

	return feeds, nil
}

func (f *Feed) GetMPLSPage(ctx context.Context, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetMPLSPage")
	defer span.End()

	if userDID == "" {
		return unauthorizedResponse, nil, nil
	}

	if cursor == "" {
		cursor = "~"
	}

	posts, err := f.Store.Queries.ListMPLS(ctx, store_queries.ListMPLSParams{
		Rkey:  cursor,
		Limit: int32(limit),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts from DB for feed (%s): %w", "mpls", err)
	}

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newCursor := ""
	for _, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		feedPosts = append(feedPosts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		newCursor = post.Rkey
	}

	if int64(len(posts)) < limit {
		return feedPosts, nil, nil
	}

	return feedPosts, &newCursor, nil
}

func (f *Feed) GetTQSPPage(ctx context.Context, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetTQSPPage")
	defer span.End()

	if cursor == "" {
		cursor = "~"
	}

	posts, err := f.Store.Queries.ListTQSP(ctx, store_queries.ListTQSPParams{
		Rkey:  cursor,
		Limit: int32(limit),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts from DB for feed (%s): %w", "tqsp", err)
	}

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	newCursor := ""
	for _, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey)
		feedPosts = append(feedPosts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
		newCursor = post.Rkey
	}

	if int64(len(posts)) < limit {
		return feedPosts, nil, nil
	}

	return feedPosts, &newCursor, nil
}
