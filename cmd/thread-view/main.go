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
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(authors) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Author with handle '%s' not found", authorHandle)})
			return
		}
		authorID = authors[0].DID
	}

	threadView, err := postRegistry.GetThreadView(ctx, postID, authorID)
	if err != nil {
		// Use errors.As to check for specific error types
		if errors.As(err, &search.NotFoundError{}) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Thread with authorID '%s' and postID '%s' not found", authorID, postID)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.JSON(http.StatusOK, threadView)
}
