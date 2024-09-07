package events

import (
	"context"
	"fmt"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
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
		log.Info("worker teardown")
		err := log.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v\n", err.Error())
		}
	}()

	log.Infow("worker started")

	// Pull from the work queue and process posts as they come in
	for {
		select {
		case postEvent, ok := <-bsky.PostQueue:
			if !ok {
				log.Info("worker shutting down on channel close")
				return
			}

			err := bsky.ProcessPost(postEvent.ctx, postEvent.repo, postEvent.rkey, postEvent.post, postEvent.workerID)
			if err != nil {
				log.Errorw("error processing post", "error", err)
			}
		case <-ctx.Done():
			log.Info("worker shutting down on context done")
			return
		}
	}

}

// ProcessPost processes a post from the Jetstream Firehose
func (bsky *BSky) ProcessPost(ctx context.Context, repo, rkey string, post *appbsky.FeedPost, workerID int) error {
	ctx, span := tracer.Start(ctx, "ProcessPost")
	defer span.End()
	start := time.Now()

	log := bsky.Workers[workerID].Logger

	log = log.With("did", repo)

	indexedAt := time.Now()

	var parentRkey string
	var rootRkey string
	parentRelationship := ""

	// Handle direct replies
	if post.Reply != nil {
		if post.Reply.Parent != nil {
			u, err := syntax.ParseATURI(post.Reply.Parent.Uri)
			if err != nil {
				log.Errorf("error parsing reply parent URI: %+v\n", err)
				return nil
			}
			// Increment the reply count metric
			replyCounter.Inc()
			// Set the parent relationship to reply and the parent ID to the reply's ID
			parentRelationship = search.ReplyRelationship
			parentRkey = u.RecordKey().String()
		}
		if post.Reply.Root != nil {
			u, err := syntax.ParseATURI(post.Reply.Root.Uri)
			if err != nil {
				log.Errorf("error parsing reply root URI: %+v\n", err)
				return nil
			}
			rootRkey = u.RecordKey().String()
		}
	}

	// Handle quote reposts
	if post.Embed != nil && post.Embed.EmbedRecord != nil && post.Embed.EmbedRecord.Record != nil {
		u, err := syntax.ParseATURI(post.Embed.EmbedRecord.Record.Uri)
		if err != nil {
			log.Errorf("error parsing quoting URI: %+v\n", err)
			return nil
		}
		// Increment the quote count metric
		quoteCounter.Inc()
		// Set the parent relationship to quote and the parent ID to the quote post ID
		parentRelationship = search.QuoteRelationship
		parentRkey = u.RecordKey().String()
	}

	// Extract any embedded images
	images := []ImageMeta{}

	if post.Embed != nil && post.Embed.EmbedImages != nil && post.Embed.EmbedImages.Images != nil {
		// Fetch the post with metadata from the BSky API (this includes signed URLs for the images)
		for _, image := range post.Embed.EmbedImages.Images {
			imageCID := image.Image.Ref.String()
			images = append(images, ImageMeta{
				CID:      imageCID,
				MimeType: image.Image.MimeType,
				AltText:  image.Alt,
			})
		}

	}

	createdAt := indexedAt
	createdAtDT, err := syntax.ParseDatetimeLenient(post.CreatedAt)
	if err != nil {
		log.Errorf("error parsing created at time: %+v\n", err)
	} else {
		createdAt = createdAtDT.Time()
	}

	// Write the post to the Post Registry if enabled
	if bsky.PostRegistryEnabled {
		author := search.Author{
			DID: repo,
		}

		dbPost := search.Post{
			ID:        rkey,
			Text:      post.Text,
			AuthorDID: repo,
			CreatedAt: createdAt,
		}

		if parentRkey != "" {
			dbPost.ParentPostID = &parentRkey
		}
		if rootRkey != "" {
			dbPost.RootPostID = &rootRkey
		}

		if post.Embed != nil && post.Embed.EmbedImages != nil {
			dbPost.HasEmbeddedMedia = true
		}
		if parentRelationship != "" {
			dbPost.ParentRelationship = &parentRelationship
		}

		err = bsky.PostRegistry.AddAuthor(ctx, &author)
		if err != nil {
			log.Errorf("error writing author to registry: %+v\n", err)
		}

		err = bsky.PostRegistry.AddPost(ctx, &dbPost)
		if err != nil {
			log.Errorf("error writing post to registry: %+v\n", err)
		}

		// If there are images, write them to the registry
		if len(images) > 0 {
			for _, image := range images {
				altText := image.AltText
				registryImage := search.Image{
					CID:       image.CID,
					PostID:    rkey,
					AuthorDID: repo,
					MimeType:  image.MimeType,
					AltText:   &altText,
					CreatedAt: indexedAt,
				}
				span.AddEvent("AddImageToRegistry")
				err = bsky.PostRegistry.AddImage(ctx, &registryImage)
				if err != nil {
					log.Errorf("error writing image to registry: %+v\n", err)
				}
			}
		}
	}

	// Record the time to process and the count
	postsProcessedCounter.Inc()
	postProcessingDurationHistogram.Observe(time.Since(start).Seconds())

	return nil
}
