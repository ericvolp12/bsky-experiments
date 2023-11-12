package endpoints

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/layout"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
)

type ThreadViewCacheEntry struct {
	ThreadView []search.PostView
	Expiration time.Time
}

type LayoutCacheEntry struct {
	Layout     []layout.ThreadViewLayout
	Expiration time.Time
}

func (api *API) LayoutThread(ctx context.Context, rootPostID string, threadView []search.PostView) ([]layout.ThreadViewLayout, error) {
	ctx, span := tracer.Start(ctx, "LayoutThread")
	defer span.End()

	// Check for the layout in the ARC Cache
	entry, ok := api.LayoutCache.Get(rootPostID)
	if ok && entry.Expiration.After(time.Now()) {
		cacheHits.WithLabelValues("layout").Inc()
		span.SetAttributes(attribute.Bool("caches.layouts.hit", true))
		return entry.Layout, nil
	}

	cacheMisses.WithLabelValues("layout").Inc()

	threadViewLayout, err := layout.SendEdgeListRequestTS(ctx, api.LayoutServiceHost, threadView)
	if err != nil {
		return nil, fmt.Errorf("error sending edge list request: %w", err)
	}

	if threadViewLayout == nil {
		return nil, errors.New("layout service returned nil")
	}

	// Update the ARC Cache
	api.LayoutCache.Add(rootPostID, LayoutCacheEntry{
		Layout:     threadViewLayout,
		Expiration: time.Now().Add(api.LayoutCacheTTL),
	})

	return threadViewLayout, nil
}

func (api *API) ProcessThreadRequest(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "processThreadRequest")
	defer span.End()

	authorID := c.Query("authorID")
	authorHandle := c.Query("authorHandle")
	postID := c.Query("postID")

	span.SetAttributes(
		attribute.String("author.id", authorID),
		attribute.String("author.handle", authorHandle),
		attribute.String("post.id", postID),
	)

	if authorID == "" && authorHandle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorID or authorHandle must be provided"})
		return
	}

	if postID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "postID must be provided"})
		return
	}

	if authorID == "" {
		authors, err := api.PostRegistry.GetAuthorsByHandle(ctx, authorHandle)
		if err != nil {
			log.Printf("Error getting authors: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(authors) == 0 {
			log.Printf("Author with handle '%s' not found", authorHandle)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Author with handle '%s' not found", authorHandle)})
			return
		}
		authorID = authors[0].DID
		span.SetAttributes(attribute.String("author.resolved_id", authorID))
	}

	// Get highest level post in thread
	rootPost, err := api.getRootOrOldestParent(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Post with postID '%s' not found", postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Post with postID '%s' not found", postID)})
		} else {
			log.Printf("Error getting root post: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	if rootPost == nil {
		log.Printf("Post with postID '%s' not found", postID)
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Post with postID '%s' not found", postID)})
		return
	}

	// Set the rootPostID in the context for the RequestLogger middleware
	c.Set("rootPostID", rootPost.ID)
	c.Set("rootPostAuthorDID", rootPost.AuthorDID)

	// Get thread view
	threadView, err := api.GetThreadView(ctx, rootPost.ID, rootPost.AuthorDID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)})
		} else {
			log.Printf("Error getting thread view: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	if c.Query("layout") == "true" {
		threadViewLayout, err := api.LayoutThread(ctx, rootPost.ID, threadView)
		if err != nil {
			log.Printf("Error laying out thread: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, threadViewLayout)
		return
	}

	c.JSON(http.StatusOK, threadView)
}

func (api *API) GetThreadView(ctx context.Context, postID, authorID string) ([]search.PostView, error) {
	ctx, span := tracer.Start(ctx, "GetThreadView")
	defer span.End()

	// Check for the thread in the ARC Cache
	entry, ok := api.ThreadViewCache.Get(postID)
	if ok && entry.Expiration.After(time.Now()) {
		cacheHits.WithLabelValues("thread").Inc()
		span.SetAttributes(attribute.Bool("caches.threads.hit", true))
		return entry.ThreadView, nil
	}

	cacheMisses.WithLabelValues("thread").Inc()

	threadView, err := api.PostRegistry.GetThreadView(ctx, postID, authorID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, fmt.Errorf("thread with authorID '%s' and postID '%s' not found: %w", authorID, postID, err)
		}
		return nil, err
	}

	// Update the ARC Cache
	api.ThreadViewCache.Add(postID, ThreadViewCacheEntry{
		ThreadView: threadView,
		Expiration: time.Now().Add(api.ThreadViewCacheTTL),
	})

	return threadView, nil
}

func (api *API) getRootOrOldestParent(ctx context.Context, postID string) (*search.Post, error) {
	ctx, span := tracer.Start(ctx, "getRootOrOldestParent")
	defer span.End()
	// Get post from registry to look for root post
	span.AddEvent("getRootOrOldestParent:ResolvePrimaryPost")
	post, err := api.PostRegistry.GetPost(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			span.SetAttributes(attribute.Bool("post.primary.found", false))
			return nil, fmt.Errorf("post with postID '%s' not found: %w", postID, err)
		}
		return nil, err
	}

	span.SetAttributes(attribute.Bool("post.primary.found", true))

	var rootPost *search.Post

	// If post has a root post and we've stored it, grab it from the registry
	if post.RootPostID != nil {
		span.AddEvent("getRootOrOldestParent:ResolveRootPost")
		rootPost, err = api.PostRegistry.GetPost(ctx, *post.RootPostID)
		if err != nil {
			// If we don't have the root post, continue to just return the oldest parent
			if !errors.As(err, &search.NotFoundError{}) {
				return nil, err
			}
			span.SetAttributes(attribute.Bool("post.root.found", false))
		}

		if rootPost != nil {
			span.SetAttributes(attribute.Bool("post.root.found", true))
		}
	}

	var oldestParent *search.Post

	// Get the oldest parent from the registry
	span.AddEvent("getRootOrOldestParent:ResolveOldestParent")
	oldestParent, err = api.PostRegistry.GetOldestPresentParent(ctx, postID)
	if err != nil {
		if !errors.As(err, &search.NotFoundError{}) {
			return nil, err
		}
		span.SetAttributes(attribute.Bool("post.oldest_parent.found", false))
	}

	if oldestParent != nil {
		span.SetAttributes(attribute.Bool("post.oldest_parent.found", true))
	}

	// If we have both a root post and an oldest parent, check if they're the same
	if rootPost != nil && oldestParent != nil {
		log.Printf("Comparing root post '%s' and oldest parent '%s'", rootPost.ID, oldestParent.ID)

		if rootPost.ID == oldestParent.ID {
			// If they're the same, return the root post
			return rootPost, nil
		}

		// If they're different, return the oldest parent
		// This should make it possible to visualize the oldest parent in the thread view when
		// it detaches from the root post due to a missing link in the graph
		return oldestParent, nil
	}

	// If we only have a root post, return it
	if rootPost != nil {
		return rootPost, nil
	}

	// If we only have an oldest parent, return it
	if oldestParent != nil {
		return oldestParent, nil
	}

	// If we have neither, return the post
	return post, nil
}
