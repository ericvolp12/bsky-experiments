package pubsky

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/gin-gonic/gin"
)

type Image struct {
	Cid string `json:"cid"`
	URL string `json:"url"`
	Alt string `json:"alt"`
}

type Post struct {
	ActorDid           string    `json:"actor_did"`
	ActorHandle        string    `json:"actor_handle,omitempty"`
	ActorProPicURL     string    `json:"actor_propic_url,omitempty"`
	Rkey               string    `json:"rkey"`
	Content            string    `json:"content"`
	ParentPostActorDid string    `json:"parent_post_actor_did,omitempty"`
	ParentPostRkey     string    `json:"parent_post_rkey,omitempty"`
	RootPostActorDid   string    `json:"root_post_actor_did,omitempty"`
	RootPostRkey       string    `json:"root_post_rkey,omitempty"`
	QuotePostActorDid  string    `json:"quote_post_actor_did,omitempty"`
	QuotePostRkey      string    `json:"quote_post_rkey,omitempty"`
	HasEmbeddedMedia   bool      `json:"has_embedded_media"`
	CreatedAt          time.Time `json:"created_at"`
	InsertedAt         time.Time `json:"inserted_at"`
	LikeCount          int64     `json:"like_count"`
	Images             []Image   `json:"images,omitempty"`
	Replies            []*Post   `json:"replies,omitempty"`
}

func (p *Pubsky) HandleGetPost(c *gin.Context) {
	ctx, span := tracer.Start(c.Request.Context(), "HandleGetPost")
	defer span.End()

	actorDidOrHandle := c.Param("handle_or_did")
	rkey := c.Param("rkey")

	if actorDidOrHandle == "" {
		c.JSON(400, gin.H{"error": "missing actor did or handle"})
		return
	}

	if rkey == "" {
		c.JSON(400, gin.H{"error": "missing rkey"})
		return
	}

	did, handle, err := ResolveHandleOrDid(ctx, p.PLCMirror, actorDidOrHandle)
	if err != nil {
		if err == ActorNotFound {
			c.JSON(404, gin.H{"error": "actor not found"})
			return
		}
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	dbPosts, err := p.Store.Queries.GetPostWithReplies(ctx, store_queries.GetPostWithRepliesParams{
		ActorDid: did,
		Rkey:     rkey,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(404, gin.H{"error": "post not found"})
			return
		}
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	if len(dbPosts) == 0 {
		c.JSON(404, gin.H{"error": "post not found"})
		return
	}

	rootDBPost := dbPosts[0]
	dbPosts = dbPosts[1:]

	var actorPropicURL string
	if rootDBPost.ProPicCid.Valid {
		actorPropicURL = fmt.Sprintf("https://av-cdn.bsky.app/img/avatar/plain/%s/%s@jpeg", did, rootDBPost.ProPicCid.String)
	}

	rootPost := Post{
		ActorDid:           rootDBPost.ActorDid,
		ActorHandle:        handle,
		ActorProPicURL:     actorPropicURL,
		Rkey:               rootDBPost.Rkey,
		Content:            rootDBPost.Content.String,
		ParentPostActorDid: rootDBPost.ParentPostActorDid.String,
		ParentPostRkey:     rootDBPost.ParentPostRkey.String,
		RootPostActorDid:   rootDBPost.RootPostActorDid.String,
		RootPostRkey:       rootDBPost.RootPostRkey.String,
		QuotePostActorDid:  rootDBPost.QuotePostActorDid.String,
		QuotePostRkey:      rootDBPost.QuotePostRkey.String,
		HasEmbeddedMedia:   rootDBPost.HasEmbeddedMedia,
		LikeCount:          rootDBPost.LikeCount.Int64,
		CreatedAt:          rootDBPost.CreatedAt.Time,
		InsertedAt:         rootDBPost.InsertedAt,
	}

	// Construct image URLs for the root post
	for i, cid := range rootDBPost.ImageCids {
		if cid == "" {
			continue
		}
		alt := rootDBPost.ImageAlts[i]

		url := fmt.Sprintf("https://av-cdn.bsky.app/img/feed_fullsize/plain/%s/%s@jpeg", did, cid)
		rootPost.Images = append(rootPost.Images, Image{
			Cid: cid,
			URL: url,
			Alt: alt,
		})
	}

	if len(dbPosts) > 0 {

		didsToLookup := []string{}

		for i := range dbPosts {
			didsToLookup = append(didsToLookup, dbPosts[i].ActorDid)
		}

		lookups, err := BatchResolveDids(ctx, p.PLCMirror, didsToLookup)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		for i := range dbPosts {
			dbPost := dbPosts[i]

			handle := ""
			for _, lookup := range lookups {
				if lookup.Did == dbPost.ActorDid {
					handle = lookup.Handle
					break
				}
			}

			var actorPropicURL string
			if dbPost.ProPicCid.Valid {
				actorPropicURL = fmt.Sprintf("https://av-cdn.bsky.app/img/avatar/plain/%s/%s@jpeg", dbPost.ActorDid, dbPost.ProPicCid.String)
			}

			post := Post{
				ActorDid:           dbPost.ActorDid,
				ActorHandle:        handle,
				ActorProPicURL:     actorPropicURL,
				Rkey:               dbPost.Rkey,
				Content:            dbPost.Content.String,
				ParentPostActorDid: dbPost.ParentPostActorDid.String,
				ParentPostRkey:     dbPost.ParentPostRkey.String,
				RootPostActorDid:   dbPost.RootPostActorDid.String,
				RootPostRkey:       dbPost.RootPostRkey.String,
				QuotePostActorDid:  dbPost.QuotePostActorDid.String,
				QuotePostRkey:      dbPost.QuotePostRkey.String,
				HasEmbeddedMedia:   dbPost.HasEmbeddedMedia,
				LikeCount:          dbPost.LikeCount.Int64,
				CreatedAt:          dbPost.CreatedAt.Time,
				InsertedAt:         dbPost.InsertedAt,
			}

			// Construct image URLs for the post
			for i, cid := range dbPost.ImageCids {
				if cid == "" {
					continue
				}

				alt := dbPost.ImageAlts[i]

				url := fmt.Sprintf("https://av-cdn.bsky.app/img/feed_fullsize/plain/%s/%s@jpeg", dbPost.ActorDid, cid)
				post.Images = append(post.Images, Image{
					Cid: cid,
					URL: url,
					Alt: alt,
				})
			}

			rootPost.Replies = append(rootPost.Replies, &post)
		}

		// Sort replies by like count, then created_at
		sort.Slice(rootPost.Replies, func(i, j int) bool {
			if rootPost.Replies[i].LikeCount == rootPost.Replies[j].LikeCount {
				return rootPost.Replies[i].CreatedAt.After(rootPost.Replies[j].CreatedAt)
			}
			return rootPost.Replies[i].LikeCount > rootPost.Replies[j].LikeCount
		})
	}

	c.JSON(200, gin.H{
		"post": rootPost,
	})
}
