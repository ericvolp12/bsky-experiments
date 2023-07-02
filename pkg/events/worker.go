package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type Worker struct {
	WorkerID  int
	Client    *xrpc.Client
	ClientMux sync.RWMutex
	Logger    *zap.SugaredLogger
}

type ImageMeta struct {
	CID          string `json:"cid"`
	MimeType     string `json:"mime_type"`
	AltText      string `json:"alt_text"`
	FullsizeURL  string `json:"fullsize_url"`
	ThumbnailURL string `json:"thumbnail_url"`
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

	// Run a routine that refreshes the auth token every 10 minutes
	authTicker := time.NewTicker(10 * time.Minute)
	quit := make(chan struct{})
	go func() {
		log.Info("starting auth refresh routine...")
		for {
			select {
			case <-authTicker.C:
				log.Info("refreshing auth token...")
				err := bsky.RefreshAuthToken(ctx, workerID)
				if err != nil {
					log.Error("error refreshing auth token: %s\n", err)
				} else {
					log.Info("successfully refreshed auth token")
				}
			case <-quit:
				authTicker.Stop()
				return
			}
		}
	}()

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
	tracer := otel.Tracer("graph-builder")
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

	span.AddEvent("HandleRepoCommit:ResolveDid")
	authorHandle, err := bsky.ResolveDID(ctx, authorDID)
	if err != nil {
		log.Errorf("error getting DID for %s: %+v\n", authorDID, err)
		return nil
	}

	span.SetAttributes(attribute.String("author.did", authorDID))
	span.SetAttributes(attribute.String("author.handle", authorHandle))

	span.AddEvent("HandleRepoCommit:DecodeFacets")
	mentions, links, err := bsky.DecodeFacets(ctx, authorDID, authorHandle, pst.Facets, workerID)
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
	quotingHandle := ""
	replyingToHandle := ""

	// Handle direct replies
	if pst.Reply != nil && pst.Reply.Parent != nil {
		replyingToURI := pst.Reply.Parent.Uri
		_, parentAuthorHandle, err := bsky.ProcessRelation(ctx, authorDID, authorHandle, replyingToURI, workerID)
		if err != nil {
			log.Errorf("error processing reply relation: %+v\n", err)
			return nil
		}
		replyingToHandle = parentAuthorHandle
		// Increment the reply count metric
		replyCounter.Inc()
		// Set the parent relationship to reply and the parent ID to the reply's ID
		parentRelationsip = search.ReplyRelationship
		parentParts := strings.Split(replyingToURI, "/")
		parentID = parentParts[len(parentParts)-1]
		if pst.Reply.Root != nil {
			// Set the root ID to the root post ID
			rootParts := strings.Split(pst.Reply.Root.Uri, "/")
			rootID = rootParts[len(rootParts)-1]
		}
	}

	// Handle quote reposts
	if pst.Embed != nil && pst.Embed.EmbedRecord != nil && pst.Embed.EmbedRecord.Record != nil {
		quotingURI := pst.Embed.EmbedRecord.Record.Uri
		_, parentAuthorHandle, err := bsky.ProcessRelation(ctx, authorDID, authorHandle, quotingURI, workerID)
		if err != nil {
			log.Errorf("error processing quote relation: %+v\n", err)
			return nil
		}
		quotingHandle = parentAuthorHandle
		// Increment the quote count metric
		quoteCounter.Inc()
		// Set the parent relationship to quote and the parent ID to the quote post ID
		parentRelationsip = search.QuoteRelationship
		parentParts := strings.Split(quotingURI, "/")
		parentID = parentParts[len(parentParts)-1]
	}

	// Extract any embedded images
	images := []ImageMeta{}

	if pst.Embed != nil && pst.Embed.EmbedImages != nil && pst.Embed.EmbedImages.Images != nil {
		// Fetch the post with metadata from the BSky API (this includes signed URLs for the images)
		postPath := fmt.Sprintf("at://%s/%s", authorDID, opPath)
		postMeta, err := bsky.ResolvePost(ctx, postPath, workerID)
		if err != nil {
			log.Errorf("error fetching post with metadata: %+v\n", err)
		} else if postMeta == nil {
			log.Error("post with metadata not found")
		} else if postMeta.Embed != nil &&
			postMeta.Embed.EmbedImages_View != nil &&
			postMeta.Embed.EmbedImages_View.Images != nil &&
			len(postMeta.Embed.EmbedImages_View.Images) == len(pst.Embed.EmbedImages.Images) {
			// Iterate through the images and add them to the list with enriched metadata
			for idx, image := range pst.Embed.EmbedImages.Images {
				imageMeta := postMeta.Embed.EmbedImages_View.Images[idx]
				if image.Image != nil && imageMeta != nil {
					imageCID := image.Image.Ref.String()
					images = append(images, ImageMeta{
						CID:          imageCID,
						MimeType:     image.Image.MimeType,
						AltText:      image.Alt,
						FullsizeURL:  imageMeta.Fullsize,
						ThumbnailURL: imageMeta.Thumb,
					})
				}
			}
		}
	}

	postLink := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", authorHandle, postID)

	// Write the post to the Post Registry if enabled
	if bsky.PostRegistryEnabled {
		author := search.Author{
			DID:    authorDID,
			Handle: authorHandle,
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
					CID:          image.CID,
					PostID:       postID,
					AuthorDID:    authorDID,
					MimeType:     image.MimeType,
					AltText:      &altText,
					FullsizeURL:  image.FullsizeURL,
					ThumbnailURL: image.ThumbnailURL,
					CreatedAt:    t,
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
		"author_handle", authorHandle,
		"author_did", authorDID,
		"quoting_handle", quotingHandle,
		"replying_to_handle", replyingToHandle,
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

// ProcessRelation handles a quote or reply relation
// It returns the parent author DID and handle after resolving the parent post
// It also updates the graph with the relation by incrementing the edge weight
func (bsky *BSky) ProcessRelation(
	ctx context.Context,
	authorDID, authorHandle, parentPostURI string,
	workerID int,
) (string, string, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ProcessRelation")
	defer span.End()

	log := bsky.Workers[workerID].Logger

	slicedURI := strings.TrimPrefix(parentPostURI, "at://")
	parentAuthorDID := slicedURI[0:strings.Index(slicedURI, "/")]

	parentAuthorHandle, err := bsky.ResolveDID(ctx, parentAuthorDID)
	if err != nil {
		errmsg := fmt.Sprintf("error resolving replying-to post author (%s): %+v\n", parentPostURI, err)
		log.Errorf("%s\n", errmsg)
		return "", "", errors.Wrap(err, errmsg)
	}

	span.SetAttributes(attribute.String("parent.uri", parentPostURI))
	span.SetAttributes(attribute.String("parent.author_handle", parentAuthorHandle))
	span.SetAttributes(attribute.String("parent.author_did", parentAuthorDID))

	// Update the graph
	from := graph.Node{
		DID:    graph.NodeID(authorDID),
		Handle: authorHandle,
	}

	to := graph.Node{
		DID:    graph.NodeID(parentAuthorDID),
		Handle: parentAuthorHandle,
	}

	err = bsky.PersistedGraph.IncrementEdge(ctx, from, to, 1)
	if err != nil {
		span.SetAttributes(attribute.String("persisted_graph.error", err.Error()))
		log.Errorf("error incrementing edge in persisted graph: %+v\n", err)
	}

	return parentAuthorDID, parentAuthorHandle, nil
}
