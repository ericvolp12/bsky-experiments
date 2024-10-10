package rss

import (
	"context"
	"fmt"
	"io"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/lex/util"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

type PostArgs struct {
	Text      string
	Tags      []string
	Labels    []string
	Languages []string
	Embed     *bsky.FeedPost_Embed
	CreatedAt time.Time
}

// CreatePost creates a new post
func (c *Client) CreatePost(ctx context.Context, args PostArgs) (*syntax.ATURI, error) {
	post := bsky.FeedPost{
		Text: args.Text,
		Tags: args.Tags,
	}

	if args.CreatedAt.IsZero() {
		post.CreatedAt = time.Now().Format(time.RFC3339Nano)
	} else {
		post.CreatedAt = args.CreatedAt.Format(time.RFC3339Nano)
	}

	if args.Languages == nil {
		args.Languages = []string{"en"}
	}

	post.Langs = args.Languages

	if args.Labels != nil {
		labels := []*atproto.LabelDefs_SelfLabel{}
		for _, label := range args.Labels {
			labels = append(labels, &atproto.LabelDefs_SelfLabel{
				Val: label,
			})
		}
		post.Labels = &bsky.FeedPost_Labels{
			LabelDefs_SelfLabels: &atproto.LabelDefs_SelfLabels{
				Values: labels,
			},
		}
	}

	if args.Embed != nil {
		post.Embed = args.Embed
	}

	if c.rl != nil {
		c.rl.Wait(ctx)
	}
	c.clientMux.RLock()
	resp, err := comatproto.RepoCreateRecord(ctx, c.xrpcc, &comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.feed.post",
		Repo:       c.ActorDID.String(),
		Record:     &util.LexiconTypeDecoder{Val: &post},
	})
	c.clientMux.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("failed to create post: %w", err)
	}

	postURI, err := syntax.ParseATURI(resp.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse post URI from creation response: %w", err)
	}

	return &postURI, nil
}

// UploadImage uploads an image to the PDS
func (c *Client) UploadImage(ctx context.Context, image io.Reader) (*util.LexBlob, error) {
	if c.rl != nil {
		c.rl.Wait(ctx)
	}
	c.clientMux.RLock()
	blob, err := comatproto.RepoUploadBlob(ctx, c.xrpcc, image)
	c.clientMux.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("failed to upload image: %w", err)
	}

	return blob.Blob, nil
}
