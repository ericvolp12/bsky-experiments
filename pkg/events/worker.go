package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type Worker struct {
	WorkerID int
	Logger   *zap.SugaredLogger
}

type ImageMeta struct {
	CID      string `json:"cid"`
	MimeType string `json:"mime_type"`
	AltText  string `json:"alt_text"`
}

func (bsky *BSky) worker(ctx context.Context, workerID int) {
	// Create a logger for this worker
	rawlog, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("failed to create logger: %+v\n", err)
	}

	log := rawlog.Sugar().With("worker_id", workerID)

	bsky.Workers[workerID].Logger = log

	defer func() {
		log.Infof("shutting down worker %d...", workerID)
		err := log.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v\n", err.Error())
		}
	}()

	log.Infof("starting worker %d\n", workerID)

	// Pull from the work queue and process records as they come in
	for {
		select {
		case record, ok := <-bsky.RepoRecordQueue:
			if !ok {
				log.Infof("worker %d terminating: RepoRecordQueue has been closed\n", workerID)
				return
			}

			err := bsky.ProcessRepoRecord(
				record.ctx,
				record.seq,
				record.pst,
				record.opPath,
				record.repoName,
				record.eventTime,
				workerID,
			)
			if err != nil {
				log.Errorf("failed to process record: %v\n", err)
			}
		case <-ctx.Done():
			log.Infof("worker %d terminating: context was cancelled\n", workerID)
			return
		}
	}

}

func (bsky *BSky) ProcessRepoRecord(
	ctx context.Context,
	seq int64,
	pst appbsky.FeedPost,
	opPath string,
	authorDID string,
	eventTime string,
	workerID int,
) error {
	ctx, span := tracer.Start(ctx, "ProcessRepoRecord")
	defer span.End()
	start := time.Now()

	if pst.LexiconTypeID != "app.bsky.feed.post" {
		return nil
	}

	log := bsky.Workers[workerID].Logger

	span.SetAttributes(attribute.String("repo.name", authorDID))
	span.SetAttributes(attribute.String("record.type.id", pst.LexiconTypeID))

	log = log.With(
		"repo_name", authorDID,
		"record_type_id", pst.LexiconTypeID,
		"trace_id", span.SpanContext().TraceID().String(),
	)

	span.SetAttributes(attribute.String("author.did", authorDID))

	span.AddEvent("HandleRepoCommit:DecodeFacets")
	mentions, links, err := bsky.DecodeFacets(ctx, pst.Facets)
	if err != nil {
		log.Errorf("error decoding post facets: %+v\n", err)
	}

	// Parse time from the event time string
	t, err := time.Parse(time.RFC3339, eventTime)
	if err != nil {
		log.Errorf("error parsing time: %+v\n", err)
		return nil
	}

	postBody := strings.ReplaceAll(pst.Text, "\n", "\n\t")

	// Grab Post, Parent, and Root ID from the Path
	pathParts := strings.Split(opPath, "/")
	postID := pathParts[len(pathParts)-1]

	var parentID string
	var rootID string
	parentRelationsip := ""
	quotingDID := ""
	replyingToDID := ""

	// Handle direct replies
	if pst.Reply != nil {
		if pst.Reply.Parent != nil {
			u, err := syntax.ParseATURI(pst.Reply.Parent.Uri)
			if err != nil {
				log.Errorf("error parsing reply parent URI: %+v\n", err)
				return nil
			}
			replyingToDID = u.Authority().String()
			// Increment the reply count metric
			replyCounter.Inc()
			// Set the parent relationship to reply and the parent ID to the reply's ID
			parentRelationsip = search.ReplyRelationship
			parentID = u.RecordKey().String()
		}
		if pst.Reply.Root != nil {
			u, err := syntax.ParseATURI(pst.Reply.Root.Uri)
			if err != nil {
				log.Errorf("error parsing reply root URI: %+v\n", err)
				return nil
			}
			rootID = u.RecordKey().String()
		}
	}

	// Handle quote reposts
	if pst.Embed != nil && pst.Embed.EmbedRecord != nil && pst.Embed.EmbedRecord.Record != nil {
		u, err := syntax.ParseATURI(pst.Embed.EmbedRecord.Record.Uri)
		if err != nil {
			log.Errorf("error parsing quoting URI: %+v\n", err)
			return nil
		}
		quotingDID = u.Authority().String()
		// Increment the quote count metric
		quoteCounter.Inc()
		// Set the parent relationship to quote and the parent ID to the quote post ID
		parentRelationsip = search.QuoteRelationship
		parentID = u.RecordKey().String()
	}

	// Extract any embedded images
	images := []ImageMeta{}

	if pst.Embed != nil && pst.Embed.EmbedImages != nil && pst.Embed.EmbedImages.Images != nil {
		// Fetch the post with metadata from the BSky API (this includes signed URLs for the images)
		for _, image := range pst.Embed.EmbedImages.Images {
			imageCID := image.Image.Ref.String()
			images = append(images, ImageMeta{
				CID:      imageCID,
				MimeType: image.Image.MimeType,
				AltText:  image.Alt,
			})
		}

	}

	postLink := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", authorDID, postID)

	// Write the post to the Post Registry if enabled
	if bsky.PostRegistryEnabled {
		author := search.Author{
			DID: authorDID,
		}

		post := search.Post{
			ID:        postID,
			Text:      pst.Text,
			AuthorDID: authorDID,
			CreatedAt: t,
		}

		if parentID != "" {
			post.ParentPostID = &parentID
		}
		if rootID != "" {
			post.RootPostID = &rootID
		}

		if pst.Embed != nil && pst.Embed.EmbedImages != nil {
			post.HasEmbeddedMedia = true
		}
		if parentRelationsip != "" {
			post.ParentRelationship = &parentRelationsip
		}

		err = bsky.PostRegistry.AddAuthor(ctx, &author)
		if err != nil {
			log.Errorf("error writing author to registry: %+v\n", err)
		}

		err = bsky.PostRegistry.AddPost(ctx, &post)
		if err != nil {
			log.Errorf("error writing post to registry: %+v\n", err)
		}

		// If there are images, write them to the registry
		if len(images) > 0 {
			for _, image := range images {
				altText := image.AltText
				registryImage := search.Image{
					CID:       image.CID,
					PostID:    postID,
					AuthorDID: authorDID,
					MimeType:  image.MimeType,
					AltText:   &altText,
					CreatedAt: t,
				}
				span.AddEvent("AddImageToRegistry")
				err = bsky.PostRegistry.AddImage(ctx, &registryImage)
				if err != nil {
					log.Errorf("error writing image to registry: %+v\n", err)
				}
			}
		}
	}

	span.AddEvent("LogResult")
	log.Infow("post processed",
		"post_id", postID,
		"post_link", postLink,
		"post_body", postBody,
		"mentions", mentions,
		"image_count", len(images),
		"links", links,
		"author_did", authorDID,
		"quoting_did", quotingDID,
		"replying_to_did", replyingToDID,
		"parent_id", parentID,
		"root_id", rootID,
		"parent_relationship", parentRelationsip,
		"created_at", t,
		"created_at_fmt", t.UTC().Format(time.RFC3339Nano),
	)

	// Record the time to process and the count
	postsProcessedCounter.Inc()
	postProcessingDurationHistogram.Observe(time.Since(start).Seconds())

	return nil
}
