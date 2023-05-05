package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/zsais/go-gin-prometheus"
)

func main() {
	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING environment variable is required")
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to create PostRegistry: %v", err)
	}
	defer postRegistry.Close()

	router := gin.Default()

	// CORS middleware
	router.Use(cors.Default())

	// Prometheus middleware
	p := ginprometheus.NewPrometheus("gin")
	p.Use(router)

	router.GET("/thread", func(c *gin.Context) {
		authorID := c.Query("authorID")
		authorHandle := c.Query("authorHandle")
		postID := c.Query("postID")

		processThreadRequest(c, postRegistry, authorID, authorHandle, postID)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}

func processThreadRequest(c *gin.Context, postRegistry *search.PostRegistry, authorID, authorHandle, postID string) {
	ctx := c.Request.Context()
	if authorID == "" && authorHandle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorID or authorHandle must be provided"})
		return
	}

	if postID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "postID must be provided"})
		return
	}

	if authorID == "" {
		authors, err := postRegistry.GetAuthorsByHandle(context.Background(), authorHandle)
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
	}

	// Get highest level post in thread
	rootPost, err := getRootOrOldestParent(ctx, postRegistry, postID)

	threadView, err := postRegistry.GetThreadView(ctx, rootPost.ID, rootPost.AuthorDID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			log.Printf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)})
		} else {
			log.Printf("Error getting thread: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.JSON(http.StatusOK, threadView)
}

func getRootOrOldestParent(ctx context.Context, postRegistry *search.PostRegistry, postID string) (*search.Post, error) {
	// Get post from registry to look for root post
	post, err := postRegistry.GetPost(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return nil, fmt.Errorf("post with postID '%s' not found: %w", postID, err)
		}
		return nil, err
	}

	// If post has a root post and we've stored it, return it
	if post.RootPostID != nil {
		rootPost, err := postRegistry.GetPost(ctx, *post.RootPostID)
		if err != nil {
			// If we don't have the root post, continue to just return the oldest parent
			if !errors.As(err, &search.NotFoundError{}) {
				return nil, err
			}
		}

		if rootPost != nil {
			return rootPost, nil
		}
	}

	// Otherwise, get the oldest parent from the registry
	oldestParent, err := postRegistry.GetOldestPresentParent(ctx, postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			return post, nil
		}
		return nil, err
	}

	if oldestParent != nil {
		return oldestParent, nil
	}

	return post, nil
}
