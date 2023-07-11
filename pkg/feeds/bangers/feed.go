package bangers

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
)

type BangersFeed struct {
	FeedActorDID string
	PostRegistry *search.PostRegistry
}

type NotFoundError struct {
	error
}

func NewBangersFeed(ctx context.Context, feedActorDID string, postRegistry *search.PostRegistry) (*BangersFeed, []string, error) {
	return &BangersFeed{
		FeedActorDID: feedActorDID,
		PostRegistry: postRegistry,
	}, []string{"bangers", "at-bangers"}, nil
}

func (plf *BangersFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	tracer := otel.Tracer("bangers-feed")
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	// For this feed, the cursor is a simple offset
	offset := int64(0)
	var err error

	if cursor != "" {
		offset, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	var postsFromRegistry []*search.Post

	switch feed {
	case "bangers":
		postsFromRegistry, err = plf.PostRegistry.GetBangerPostsForAuthor(ctx, userDID, int32(limit), int32(offset))
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
	case "at-bangers":
		postsFromRegistry, err = plf.PostRegistry.GetAllTimeBangers(ctx, int32(limit), int32(offset))
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				return nil, nil, NotFoundError{fmt.Errorf("posts not found for feed %s", feed)}
			}
			return nil, nil, fmt.Errorf("error getting posts from registry for feed (%s): %w", feed, err)
		}
	}

	// Convert to appbsky.FeedDefs_SkeletonFeedPost
	posts := []*appbsky.FeedDefs_SkeletonFeedPost{}
	for _, post := range postsFromRegistry {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
		posts = append(posts, &appbsky.FeedDefs_SkeletonFeedPost{
			Post: postAtURL,
		})
	}

	// If we got less than the limit, we're at the end of the feed
	if int64(len(posts)) < limit {
		return posts, nil, nil
	}

	// Otherwise, we need to return a cursor
	newCursor := strconv.FormatInt(offset+limit, 10)

	return posts, &newCursor, nil
}

func (plf *BangersFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	tracer := otel.Tracer("bangers-feed")
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + "bangers",
		},
		{
			Uri: "at://" + plf.FeedActorDID + "/app.bsky.feed.generator/" + "at-bangers",
		},
	}

	return feeds, nil
}
