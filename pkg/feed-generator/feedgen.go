package feedgenerator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/clusters"
	"github.com/gin-gonic/gin"
)

type FeedGenerator struct {
	PostRegistry    *search.PostRegistry
	Client          *xrpc.Client
	ClusterManager  *clusters.ClusterManager
	LegacyFeedNames map[string]string
	DefaultLookback int32
}

type FeedPostItem struct {
	Post string `json:"post"`
}

type FeedSkeleton struct {
	Feed   []FeedPostItem `json:"feed"`
	Cursor *string        `json:"cursor,omitempty"`
}

// NewFeedGenerator returns a new FeedGenerator
func NewFeedGenerator(
	ctx context.Context,
	postRegistry *search.PostRegistry,
	client *xrpc.Client,
	graphJSONUrl string,
) (*FeedGenerator, error) {

	clusterManager, err := clusters.NewClusterManager(graphJSONUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster manager: %w", err)
	}

	legacyFeedNames := map[string]string{
		"positivifeed": "sentiment:pos",
		"negativifeed": "sentiment:neg",
	}

	return &FeedGenerator{
		PostRegistry:    postRegistry,
		Client:          client,
		ClusterManager:  clusterManager,
		LegacyFeedNames: legacyFeedNames,
		DefaultLookback: 12, // hours
	}, nil
}

func (fg *FeedGenerator) GetWellKnownDID(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"@context": []string{"https://www.w3.org/ns/did/v1"},
		"id":       "did:web:feedsky.jazco.io",
		"service": []gin.H{
			{
				"id":              "#bsky_fg",
				"type":            "BskyFeedGenerator",
				"serviceEndpoint": "https://feedsky.jazco.io",
			},
		},
	})
}

func (fg *FeedGenerator) UpdateClusterAssignments(c *gin.Context) {
	log.Println("Updating cluster assignments...")
	// Iterate over all authors in the Manager and update them in the registry
	errs := make([]error, 0)

	count := 0
	for _, author := range fg.ClusterManager.DIDClusterMap {
		if count%1000 == 0 {
			log.Printf("Updated %d/%d authors", count, len(fg.ClusterManager.DIDClusterMap))
		}

		count++

		clusterID, err := strconv.ParseInt(author.ClusterID, 10, 64)
		if err != nil {
			newErr := fmt.Errorf("failed to parse cluster ID %s: %w", author.ClusterID, err)
			errs = append(errs, newErr)
			log.Println(newErr.Error())
			continue
		}
		err = fg.PostRegistry.AssignAuthorToCluster(c.Request.Context(), author.UserDID, int32(clusterID))
		if err != nil {
			newErr := fmt.Errorf("failed to assign author %s to cluster %d: %w", author.UserDID, clusterID, err)
			errs = append(errs, newErr)
			log.Println(newErr.Error())
			continue
		}
	}

	log.Println("Finished updating cluster assignments")

	c.JSON(http.StatusOK, gin.H{"message": "cluster assignments updated", "errors": errs})
}

func (fg *FeedGenerator) GetFeedSkeleton(c *gin.Context) {
	// Incoming requests should have a query parameter "feed" that looks like: at://did:web:feedsky.jazco.io/app.bsky.feed.generator/feed-name
	// Also a query parameter "limit" that looks like: 50
	// Also a query parameter "cursor" that contains the last post ID from the previous page of results
	feedQuery := c.Query("feed")
	if feedQuery == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed query parameter is required"})
		return
	}

	// Make sure the feed query is for our feed generator service
	if !strings.HasPrefix(feedQuery, "at://did:web:feedsky.jazco.io/app.bsky.feed.generator/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed requested is not provided by this generator"})
		return
	}

	// Get the feed name from the query
	feedName := strings.TrimPrefix(feedQuery, "at://did:web:feedsky.jazco.io/app.bsky.feed.generator/")
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed name is required"})
		return
	}

	// Check if this is a legacy feed name
	if fg.LegacyFeedNames[feedName] != "" {
		feedName = fg.LegacyFeedNames[feedName]
	}

	var cluster *string

	// Check if the feed is a "cluster-{alias}" feed
	if strings.HasPrefix(feedName, "cluster-") {
		clusterAlias := strings.TrimPrefix(feedName, "cluster-")
		cluster = &clusterAlias
	}

	// Get the limit from the query, default to 50, maximum of 250
	limit := int32(50)
	limitQuery := c.Query("limit")
	if limitQuery != "" {
		parsedLimit, err := strconv.ParseInt(limitQuery, 10, 32)
		if err != nil {
			limit = 50
		} else {
			limit = int32(parsedLimit)
			if limit > 250 {
				limit = 250
			}
		}
	}

	// Get the cursor from the query
	cursor := c.Query("cursor")

	var posts []*search.Post

	// Get cluster posts if a cluster is specified
	if cluster != nil {
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForCluster(c.Request.Context(), *cluster, fg.DefaultLookback, limit, cursor)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
		}

		posts = postsFromRegistry
	} else { // Otherwise lookup labels
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForLabel(c.Request.Context(), feedName, fg.DefaultLookback, limit, cursor)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}

			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		posts = postsFromRegistry
	}

	feedItems := make([]FeedPostItem, len(posts))
	for i, post := range posts {
		postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
		feedItems[i] = FeedPostItem{Post: postAtURL}
	}

	feedSkeleton := FeedSkeleton{
		Feed: feedItems,
	}

	if len(posts) > 0 {
		feedSkeleton.Cursor = &posts[len(posts)-1].ID
	}

	c.JSON(http.StatusOK, feedSkeleton)
}
