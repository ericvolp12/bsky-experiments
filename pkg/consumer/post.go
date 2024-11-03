package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/sharddb"

	"github.com/goccy/go-json"
	"github.com/sqlc-dev/pqtype"
	"go.opentelemetry.io/otel/attribute"
)

func (c *Consumer) HandleCreatePost(ctx context.Context, repo, rkey string, indexedAt time.Time, rec *appbsky.FeedPost) error {
	ctx, span := tracer.Start(ctx, "HandleCreatePost")
	defer span.End()

	span.SetAttributes(attribute.String("record_type", "feed_post"))
	recordsProcessedCounter.WithLabelValues("feed_post", c.SocketURL).Inc()

	log := c.Logger.With("repo", repo, "rkey", rkey, "method", "HandleCreatePost")

	// Check if we've already processed this record
	_, err := c.Store.Queries.GetPost(ctx, store_queries.GetPostParams{
		ActorDid: repo,
		Rkey:     rkey,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("failed to get post: %w", err)
		}
	} else {
		// We've already processed this record, so skip it
		return nil
	}

	quoteActorDid := ""
	quoteActorRkey := ""
	if rec.Embed != nil && rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
		quoteRepostsProcessedCounter.WithLabelValues(c.SocketURL).Inc()
		u, err := GetURI(rec.Embed.EmbedRecord.Record.Uri)
		if err != nil {
			return fmt.Errorf("failed to get Quoted Record uri: %w", err)
		}
		quoteActorDid = u.Did
		quoteActorRkey = u.RKey

	}

	parentActorDid := ""
	parentActorRkey := ""
	if rec.Reply != nil && rec.Reply.Parent != nil {
		u, err := GetURI(rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("failed to get Reply uri: %w", err)
		}
		parentActorDid = u.Did
		parentActorRkey = u.RKey
	}

	rootActorDid := ""
	rootActorRkey := ""
	if rec.Reply != nil && rec.Reply.Root != nil {
		u, err := GetURI(rec.Reply.Root.Uri)
		if err != nil {
			return fmt.Errorf("failed to get Root uri: %w", err)
		}
		rootActorDid = u.Did
		rootActorRkey = u.RKey
	}

	hasMedia := false
	if rec.Embed != nil {
		if rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0 {
			hasMedia = true
		} else if rec.Embed.EmbedRecordWithMedia != nil &&
			rec.Embed.EmbedRecordWithMedia.Media != nil &&
			rec.Embed.EmbedRecordWithMedia.Media.EmbedImages != nil &&
			len(rec.Embed.EmbedRecordWithMedia.Media.EmbedImages.Images) > 0 {
			hasMedia = true
		}
	}

	recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
	if parseError != nil {
		log.Warnf("failed to parse CreatedAt: %+v", parseError)
		recCreatedAt = indexedAt
	}

	createParams := store_queries.CreatePostParams{
		ActorDid:           repo,
		Rkey:               rkey,
		Content:            sql.NullString{String: rec.Text, Valid: true},
		ParentPostActorDid: sql.NullString{String: parentActorDid, Valid: parentActorDid != ""},
		ParentPostRkey:     sql.NullString{String: parentActorRkey, Valid: parentActorRkey != ""},
		QuotePostActorDid:  sql.NullString{String: quoteActorDid, Valid: quoteActorDid != ""},
		QuotePostRkey:      sql.NullString{String: quoteActorRkey, Valid: quoteActorRkey != ""},
		RootPostActorDid:   sql.NullString{String: rootActorDid, Valid: rootActorDid != ""},
		RootPostRkey:       sql.NullString{String: rootActorRkey, Valid: rootActorRkey != ""},
		HasEmbeddedMedia:   hasMedia,
		CreatedAt:          sql.NullTime{Time: recCreatedAt, Valid: true},
		Langs:              rec.Langs,
		InsertedAt:         indexedAt,
	}

	if rec.Facets != nil {
		facetsJSON := []byte{}
		facetsJSON, err = json.Marshal(rec.Facets)
		if err != nil {
			log.Errorf("failed to marshal facets: %+v", err)
		} else if len(facetsJSON) > 0 {
			createParams.Facets = pqtype.NullRawMessage{RawMessage: facetsJSON, Valid: true}
		}

		for _, facet := range rec.Facets {
			if facet.Features != nil {
				for _, feature := range facet.Features {
					if feature.RichtextFacet_Tag != nil {
						createParams.Tags = append(createParams.Tags, feature.RichtextFacet_Tag.Tag)
					}
				}
			}
		}
	}

	if rec.Embed != nil && rec.Embed.EmbedExternal != nil {
		embedJSON := []byte{}
		embedJSON, err = json.Marshal(rec.Embed.EmbedExternal)
		if err != nil {
			log.Errorf("failed to marshal embed external: %+v", err)
		} else if len(embedJSON) > 0 {
			createParams.Embed = pqtype.NullRawMessage{RawMessage: embedJSON, Valid: true}
		}
	}

	if len(rec.Tags) > 0 {
		createParams.Tags = append(createParams.Tags, rec.Tags...)
	}

	// Create the post subject
	subj, err := c.Store.Queries.CreateSubject(ctx, store_queries.CreateSubjectParams{
		ActorDid: repo,
		Rkey:     rkey,
		Col:      1, // Maps to app.bsky.feed.post
	})
	if err != nil {
		log.Errorf("failed to create subject: %+v", err)
	}

	createParams.SubjectID = sql.NullInt64{Int64: subj.ID, Valid: true}

	err = c.Store.Queries.CreatePost(ctx, createParams)
	if err != nil {
		log.Errorf("failed to create post: %+v", err)
	}

	err = c.Store.Queries.CreateRecentPost(ctx, store_queries.CreateRecentPostParams{
		ActorDid:           createParams.ActorDid,
		Rkey:               createParams.Rkey,
		Content:            createParams.Content,
		ParentPostActorDid: createParams.ParentPostActorDid,
		ParentPostRkey:     createParams.ParentPostRkey,
		QuotePostActorDid:  createParams.QuotePostActorDid,
		QuotePostRkey:      createParams.QuotePostRkey,
		RootPostActorDid:   createParams.RootPostActorDid,
		RootPostRkey:       createParams.RootPostRkey,
		HasEmbeddedMedia:   createParams.HasEmbeddedMedia,
		Facets:             createParams.Facets,
		Embed:              createParams.Embed,
		Langs:              createParams.Langs,
		Tags:               createParams.Tags,
		CreatedAt:          createParams.CreatedAt,
		SubjectID:          sql.NullInt64{Int64: subj.ID, Valid: true},
	})
	if err != nil {
		log.Errorf("failed to create recent post: %+v", err)
	}

	earliestTS := time.Now()
	if recCreatedAt.Before(earliestTS) {
		earliestTS = recCreatedAt
	}

	// Track pins if it's a pinned post
	if rec.Reply != nil && (strings.Contains(rec.Text, "📌") || strings.Contains(rec.Text, "🔖")) {
		err = c.Store.Queries.CreatePin(ctx, store_queries.CreatePinParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
		if err != nil {
			log.Errorf("failed to create pin: %+v", err)
		}
	}

	// If the actor has a label, update the label feed (for root posts only)
	// Filter out posts with a createdAt very far in the past
	if rec.Reply == nil && recCreatedAt.After(time.Now().Add(-time.Hour*24*7)) {
		// Fetch the labels for the actor
		labels, err := c.Store.Queries.ListActorLabels(ctx, repo)
		if err != nil {
			log.Errorf("failed to get labels for actor: %+v", err)
		} else if len(labels) > 0 {
			labelMap := map[string]struct{}{}
			for _, label := range labels {
				labelMap[label] = struct{}{}
			}

			if _, ok := labelMap["mpls"]; ok {
				err = c.Store.Queries.CreateMPLS(ctx, store_queries.CreateMPLSParams{
					ActorDid: repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to create mpls feed post: %+v", err)
				}
			}
			if _, ok := labelMap["tqsp"]; ok {
				err = c.Store.Queries.CreateTQSP(ctx, store_queries.CreateTQSPParams{
					ActorDid: repo,
					Rkey:     rkey,
				})
				if err != nil {
					log.Errorf("failed to create tqsp feed post: %+v", err)
				}
			}
		}
	}

	// Create images for the post
	if rec.Embed != nil && rec.Embed.EmbedImages != nil {
		for _, img := range rec.Embed.EmbedImages.Images {
			if img.Image == nil {
				continue
			}
			err = c.Store.Queries.CreateImage(ctx, store_queries.CreateImageParams{
				Cid:          img.Image.Ref.String(),
				PostActorDid: repo,
				PostRkey:     rkey,
				AltText:      sql.NullString{String: img.Alt, Valid: img.Alt != ""},
				CreatedAt:    sql.NullTime{Time: recCreatedAt, Valid: true},
			})
			if err != nil {
				log.Errorf("failed to create image: %+v", err)
			}
			err = c.Store.Queries.EnqueueImage(ctx, store_queries.EnqueueImageParams{
				Cid:          img.Image.Ref.String(),
				PostActorDid: repo,
				PostRkey:     rkey,
				SubjectID:    subj.ID,
				AltText:      sql.NullString{String: img.Alt, Valid: img.Alt != ""},
			})
			if err != nil {
				log.Errorf("failed to enqueue image: %+v", err)
			}
		}
	}

	// Create an "image" for video thumbnails
	if rec.Embed != nil && rec.Embed.EmbedVideo != nil && rec.Embed.EmbedVideo.Video != nil {
		alt := ""
		if rec.Embed.EmbedVideo.Alt != nil {
			alt = *rec.Embed.EmbedVideo.Alt
		}
		err = c.Store.Queries.CreateImage(ctx, store_queries.CreateImageParams{
			Cid:          rec.Embed.EmbedVideo.Video.Ref.String(),
			PostActorDid: repo,
			PostRkey:     rkey,
			IsVideo:      true,
			AltText:      sql.NullString{String: alt, Valid: alt != ""},
			CreatedAt:    sql.NullTime{Time: recCreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("failed to create video thumbnail: %+v", err)
		}
		err = c.Store.Queries.EnqueueImage(ctx, store_queries.EnqueueImageParams{
			Cid:          rec.Embed.EmbedVideo.Video.Ref.String(),
			PostActorDid: repo,
			PostRkey:     rkey,
			IsVideo:      true,
			SubjectID:    subj.ID,
			AltText:      sql.NullString{String: alt, Valid: alt != ""},
		})
		if err != nil {
			log.Errorf("failed to enqueue video thumbnail: %+v", err)
		}
	}

	// Initialize the like count
	err = c.Store.Queries.CreateLikeCount(ctx, store_queries.CreateLikeCountParams{
		SubjectID:        subj.ID,
		NumLikes:         0,
		UpdatedAt:        time.Now(),
		SubjectCreatedAt: sql.NullTime{Time: recCreatedAt, Valid: true},
	})
	if err != nil {
		log.Errorf("failed to create like count: %+v", err)
	}

	if c.shardDB != nil {
		bucket, err := sharddb.GetBucketFromRKey(rkey)
		if err != nil {
			log.Errorf("failed to get bucket from rkey %q: %+v", rkey, err)
			return nil
		}

		recBytes, err := json.Marshal(rec)
		if err != nil {
			log.Errorf("failed to marshal record for insertion to sharddb: %+v", err)
			return nil
		}

		// Create Post in ShardDB
		shardDBPost := sharddb.Post{
			ActorDID:  repo,
			Rkey:      rkey,
			IndexedAt: indexedAt,
			Bucket:    bucket,
			Raw:       recBytes,
			Langs:     rec.Langs,
			Tags:      rec.Tags,
			HasMedia:  hasMedia,
			IsReply:   rec.Reply != nil,
		}

		err = c.shardDB.InsertPost(ctx, shardDBPost)
		if err != nil {
			log.Errorf("failed to insert post into sharddb: %+v", err)
		}
	}

	// Track the user in the posters bitmap
	hourlyPostBMKey := fmt.Sprintf("posts_hourly:%s", recCreatedAt.Format("2006_01_02_15"))

	err = c.bitmapper.AddMember(ctx, hourlyPostBMKey, repo)
	if err != nil {
		log.Errorf("failed to add member to posters bitmap: %+v", err)
	}

	return nil
}

func (c *Consumer) HandleDeletePost(ctx context.Context, repo, rkey string) error {
	ctx, span := tracer.Start(ctx, "HandleDeletePost")
	defer span.End()

	log := c.Logger.With("repo", repo, "rkey", rkey, "method", "HandleDeletePost")

	span.SetAttributes(attribute.String("record_type", "feed_post"))
	err := c.Store.Queries.DeletePost(ctx, store_queries.DeletePostParams{
		ActorDid: repo,
		Rkey:     rkey,
	})
	if err != nil {
		log.Errorf("failed to delete post: %+v", err)
		// Don't return an error here, because we still want to try to delete the images
	}

	err = c.Store.Queries.DeleteRecentPost(ctx, store_queries.DeleteRecentPostParams{
		ActorDid: repo,
		Rkey:     rkey,
	})

	// Clean up pinned post
	_, err = c.Store.Queries.GetPin(ctx, store_queries.GetPinParams{
		ActorDid: repo,
		Rkey:     rkey,
	})
	if err == nil {
		err = c.Store.Queries.DeletePin(ctx, store_queries.DeletePinParams{
			ActorDid: repo,
			Rkey:     rkey,
		})
	}

	// Delete images for the post
	err = c.Store.Queries.DeleteImagesForPost(ctx, store_queries.DeleteImagesForPostParams{
		PostActorDid: repo,
		PostRkey:     rkey,
	})
	if err != nil {
		return fmt.Errorf("failed to delete images for post: %+v", err)
	}

	return nil
}
