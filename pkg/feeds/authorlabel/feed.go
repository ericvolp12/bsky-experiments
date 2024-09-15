package authorlabel

import (
	"context"
	"fmt"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type AuthorLabelFeed struct {
	FeedActorDID string
	Store        *store.Store
	Labels       []string
}

var privateFeedInstructionsPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3jwvwlajglc2w"
var unauthorizedResponse = []*appbsky.FeedDefs_SkeletonFeedPost{{Post: privateFeedInstructionsPost}}

type NotFoundError struct {
	error
}

func NewAuthorLabelFeed(ctx context.Context, feedActorDID string, store *store.Store) (*AuthorLabelFeed, []string, error) {
	labels := []string{
		"a-mpls",
	}
	return &AuthorLabelFeed{
		FeedActorDID: feedActorDID,
		Store:        store,
		Labels:       labels,
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

	// Get the actor label from the feed
	actorLabel := strings.TrimPrefix(feed, "a-")

	if actorLabel != "mpls" {
		span.SetAttributes(attribute.String("invalid_label", actorLabel))
		return nil, nil, NotFoundError{fmt.Errorf("feed not found")}
	}

	// Ensure the user is assigned to the label before letting them view the feed
	authorized, err := alf.Store.Queries.ActorHasLabel(ctx, store_queries.ActorHasLabelParams{
		ActorDid: userDID,
		Label:    actorLabel,
	})
	if err != nil {
		span.SetAttributes(attribute.String("label_lookup_error", err.Error()))
		return nil, nil, fmt.Errorf("error getting labels for actor: %w", err)
	}

	if !authorized {
		span.SetAttributes(attribute.Bool("actor_not_authorized", true))
		return unauthorizedResponse, nil, nil
	}

	if cursor == "" {
		cursor = "~"
	}

	// PostsFromDB
	posts, err := alf.Store.Queries.ListMPLS(ctx, store_queries.ListMPLSParams{
		Rkey:  cursor,
		Limit: int32(limit),
	})
	if err != nil {
		span.SetAttributes(attribute.String("post_load_error", err.Error()))
		return nil, nil, fmt.Errorf("error getting posts from DB for feed (%s): %w", feed, err)
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

func (plf *AuthorLabelFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}
	for _, label := range plf.Labels {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + label,
		})
	}

	return feeds, nil
}
