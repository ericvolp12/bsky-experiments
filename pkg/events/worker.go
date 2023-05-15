package events

import (
	"context"
	"fmt"
	"log"
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
}

func (bsky *BSky) worker(workerID int) {
	ctx := context.Background()
	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v\n", err)
	}
	defer func() {
		err := rawlog.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar()

	log = log.With("worker_id", workerID)

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
		record := <-bsky.RepoRecordQueue
		err := bsky.ProcessRepoRecord(
			record.ctx,
			log,
			record.pst,
			record.opPath,
			record.repoName,
			record.eventTime,
			workerID,
		)
		if err != nil {
			log.Error("failed to process record: %v\n", err)
		}
	}
}

func (bsky *BSky) ProcessRepoRecord(
	ctx context.Context,
	log *zap.SugaredLogger,
	pst appbsky.FeedPost,
	opPath string,
	repoName string,
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

	span.SetAttributes(attribute.String("repo.name", repoName))
	span.SetAttributes(attribute.String("record.type.id", pst.LexiconTypeID))
	log = log.With(
		"repo_name", repoName,
		"record_type_id", pst.LexiconTypeID,
		"trace_id", span.SpanContext().TraceID().String(),
	)
	span.AddEvent("HandleRepoCommit:ResolveProfile")
	authorProfile, err := bsky.ResolveProfile(ctx, repoName, workerID)
	if err != nil {
		log.Errorf("error getting profile for %s: %+v\n", repoName, err)
		return nil
	}

	span.SetAttributes(attribute.String("author.did", authorProfile.Did))
	span.SetAttributes(attribute.String("author.handle", authorProfile.Handle))

	span.AddEvent("HandleRepoCommit:DecodeFacets")
	mentions, links, err := bsky.DecodeFacets(ctx, authorProfile.Did, authorProfile.Handle, pst.Facets, workerID)
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
		_, parentAuthorHandle, err := bsky.ProcessRelation(ctx, log, authorProfile, replyingToURI, workerID)
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
		_, parentAuthorHandle, err := bsky.ProcessRelation(ctx, log, authorProfile, quotingURI, workerID)
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

	postLink := fmt.Sprintf("https://staging.bsky.app/profile/%s/post/%s", authorProfile.Handle, postID)

	// Write the post to the Post Registry if enabled
	if bsky.PostRegistryEnabled {
		author := search.Author{
			DID:    authorProfile.Did,
			Handle: authorProfile.Handle,
		}

		post := search.Post{
			ID:        postID,
			Text:      pst.Text,
			AuthorDID: authorProfile.Did,
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

		// If sentiment is enabled, get the sentiment for the post
		if bsky.SentimentAnalysisEnabled {
			span.AddEvent("HandleRepoCommit:GetPostsSentiment")
			posts, err := bsky.SentimentAnalysis.GetPostsSentiment(ctx, []search.Post{post})
			if err != nil {
				span.SetAttributes(attribute.String("sentiment.error", err.Error()))
				log.Errorf("error getting sentiment for post %s: %+v\n", postID, err)
			} else if len(posts) > 0 {
				post.Sentiment = posts[0].Sentiment
				post.SentimentConfidence = posts[0].SentimentConfidence
			}
		}

		err = bsky.PostRegistry.AddAuthor(ctx, &author)
		if err != nil {
			log.Errorf("error writing author to registry: %+v\n", err)
		}

		err = bsky.PostRegistry.AddPost(ctx, &post)
		if err != nil {
			log.Errorf("error writing post to registry: %+v\n", err)
		}
	}

	log.Infow("post processed",
		"post_id", postID,
		"post_link", postLink,
		"post_body", postBody,
		"mentions", mentions,
		"links", links,
		"author_handle", authorProfile.Handle,
		"author_did", authorProfile.Did,
		"quoting_handle", quotingHandle,
		"replying_to_handle", replyingToHandle,
		"parent_id", parentID,
		"root_id", rootID,
		"parent_relationship", parentRelationsip,
		"created_at", t)

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
	log *zap.SugaredLogger,
	authorProfile *appbsky.ActorDefs_ProfileViewDetailed,
	parentPostURI string,
	workerID int,
) (string, string, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ProcessRelation")
	defer span.End()
	parentAuthorDID := ""
	parentAuthorHandle := ""

	post, err := bsky.ResolvePost(ctx, parentPostURI, workerID)
	if err != nil {
		errmsg := fmt.Sprintf("error resolving replying-to post (%s): %+v\n", parentPostURI, err)
		log.Errorf("%s\n", errmsg)
		return "", "", errors.Wrap(err, errmsg)
	} else if post == nil {
		errmsg := fmt.Sprintf("replying-to post (%s) not found", parentPostURI)
		log.Errorf("%s\n", errmsg)
		return "", "", fmt.Errorf(errmsg)
	}

	parentAuthorDID = post.Author.Did
	parentAuthorHandle = post.Author.Handle

	span.SetAttributes(attribute.String("parent.uri", parentPostURI))
	span.SetAttributes(attribute.String("parent.author_handle", parentAuthorHandle))
	span.SetAttributes(attribute.String("parent.author_did", parentAuthorDID))

	// Update the graph
	from := graph.Node{
		DID:    graph.NodeID(authorProfile.Did),
		Handle: authorProfile.Handle,
	}

	to := graph.Node{
		DID:    graph.NodeID(parentAuthorDID),
		Handle: parentAuthorHandle,
	}
	span.AddEvent("ProcessRelation:AcquireGraphLock")
	bsky.SocialGraphMux.Lock()
	span.AddEvent("ProcessRelation:GraphLockAcquired")
	bsky.SocialGraph.IncrementEdge(from, to, 1)
	span.AddEvent("ProcessRelation:ReleaseGraphLock")
	bsky.SocialGraphMux.Unlock()

	return parentAuthorDID, parentAuthorHandle, nil
}
