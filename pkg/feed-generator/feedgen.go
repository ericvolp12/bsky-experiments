package feedgenerator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/clusters"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var feedRequestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "feed_request_count",
	Help: "The total number of feed requests",
}, []string{"feed_name"})

var feedRequestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "feed_request_latency",
	Help:    "The latency of feed requests",
	Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
}, []string{"feed_name"})

type FeedGenerator struct {
	PostRegistry          *search.PostRegistry
	Client                *xrpc.Client
	ClusterManager        *clusters.ClusterManager
	LegacyFeedNames       map[string]string
	DefaultLookback       int32
	AcceptableURIPrefixes []string
}

type FeedPostItem struct {
	Post string `json:"post"`
}

type FeedSkeleton struct {
	Feed   []FeedPostItem `json:"feed"`
	Cursor *string        `json:"cursor,omitempty"`
}

type FeedDescription struct {
	URI string `json:"uri"`
}

type FeedGeneratorDescription struct {
	DID   string            `json:"did"`
	Feeds []FeedDescription `json:"feeds"`
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

	acceptableURIPrefixes := []string{
		"at://did:web:feedsky.jazco.io/app.bsky.feed.generator/",
		"at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.generator/",
	}

	return &FeedGenerator{
		PostRegistry:          postRegistry,
		Client:                client,
		ClusterManager:        clusterManager,
		LegacyFeedNames:       legacyFeedNames,
		DefaultLookback:       12, // hours
		AcceptableURIPrefixes: acceptableURIPrefixes,
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

func (fg *FeedGenerator) DescribeFeedGenerator(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:DescribeFeedGenerator")
	defer span.End()

	clusterAliases := []string{}
	clusters, err := fg.PostRegistry.GetClusters(ctx)
	if err != nil {
		span.SetAttributes(attribute.String("clusters.error", err.Error()))
		log.Printf("failed to get clusters: %s", err.Error())
	} else {
		for _, cluster := range clusters {
			clusterAliases = append(clusterAliases, cluster.LookupAlias)
		}
	}

	span.SetAttributes(attribute.Int("clusters.length", len(clusterAliases)))

	labels := []string{}
	uniqueLabels, err := fg.PostRegistry.GetUniqueLabels(ctx)
	if err != nil {
		span.SetAttributes(attribute.String("labels.error", err.Error()))
		log.Printf("failed to get unique labels: %s", err.Error())
	} else {
		labels = append(labels, uniqueLabels...)
	}

	span.SetAttributes(attribute.Int("labels.length", len(labels)))

	feedPrefix := "at://did:web:feedsky.jazco.io/app.bsky.feed.generator/"

	feedDescriptions := []FeedDescription{}
	for _, feedName := range fg.LegacyFeedNames {
		feedDescriptions = append(feedDescriptions, FeedDescription{URI: feedPrefix + feedName})
	}

	for _, clusterAlias := range clusterAliases {
		feedDescriptions = append(feedDescriptions, FeedDescription{URI: feedPrefix + "cluster-" + clusterAlias})
	}

	for _, label := range labels {
		feedDescriptions = append(feedDescriptions, FeedDescription{URI: feedPrefix + label})
	}

	feedGeneratorDescription := FeedGeneratorDescription{
		DID:   "did:web:feedsky.jazco.io",
		Feeds: feedDescriptions,
	}

	span.SetAttributes(attribute.Int("feeds.length", len(feedDescriptions)))

	c.JSON(http.StatusOK, feedGeneratorDescription)
}

func (fg *FeedGenerator) UpdateClusterAssignments(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:UpdateClusterAssignments")
	defer span.End()

	log.Println("Updating cluster assignments...")
	// Iterate over all authors in the Manager and update them in the registry
	errs := make([]error, 0)
	errStrings := make([]string, 0)

	span.SetAttributes(attribute.Int("authors.length", len(fg.ClusterManager.DIDClusterMap)))
	span.SetAttributes(attribute.Int("clusters.length", len(fg.ClusterManager.Clusters)))

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
		err = fg.PostRegistry.AssignAuthorToCluster(ctx, author.UserDID, int32(clusterID))
		if err != nil {
			newErr := fmt.Errorf("failed to assign author %s to cluster %d: %w", author.UserDID, clusterID, err)
			errs = append(errs, newErr)
			errStrings = append(errStrings, newErr.Error())
			log.Println(newErr.Error())
			continue
		}
	}

	log.Println("Finished updating cluster assignments")

	span.SetAttributes(attribute.Int("errors.length", len(errs)))
	span.SetAttributes(attribute.StringSlice("errors", errStrings))

	c.JSON(http.StatusOK, gin.H{"message": "cluster assignments updated", "errors": errs})
}

func (fg *FeedGenerator) GetFeedSkeleton(c *gin.Context) {
	// Incoming requests should have a query parameter "feed" that looks like: at://did:web:feedsky.jazco.io/app.bsky.feed.generator/feed-name
	// Also a query parameter "limit" that looks like: 50
	// Also a query parameter "cursor" that contains the last post ID from the previous page of results
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:GetFeedSkeleton")
	defer span.End()

	start := time.Now()
	feedQuery := c.Query("feed")
	if feedQuery == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed query parameter is required"})
		return
	}

	span.SetAttributes(attribute.String("feed.query", feedQuery))

	feedPrefix := ""
	for _, acceptablePrefix := range fg.AcceptableURIPrefixes {
		if strings.HasPrefix(feedQuery, acceptablePrefix) {
			feedPrefix = acceptablePrefix
			break
		}
	}

	if feedPrefix == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed query parameter is not a valid feed URI"})
		return
	}

	// Get the feed name from the query
	feedName := strings.TrimPrefix(feedQuery, feedPrefix)
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed name is required"})
		return
	}

	span.SetAttributes(attribute.String("feed.name.raw", feedName))

	// Check if this is a legacy feed name
	if fg.LegacyFeedNames[feedName] != "" {
		feedName = fg.LegacyFeedNames[feedName]
	}

	span.SetAttributes(attribute.String("feed.name.parsed", feedName))

	var cluster *string

	// Check if the feed is a "cluster-{alias}" feed
	if strings.HasPrefix(feedName, "cluster-") {
		clusterAlias := strings.TrimPrefix(feedName, "cluster-")
		cluster = &clusterAlias
		span.SetAttributes(attribute.String("feed.cluster", clusterAlias))
	}

	feedRequestCounter.WithLabelValues(feedName).Inc()

	// Get the limit from the query, default to 50, maximum of 250
	limit := int32(50)
	limitQuery := c.Query("limit")
	span.SetAttributes(attribute.String("feed.limit.raw", limitQuery))
	if limitQuery != "" {
		parsedLimit, err := strconv.ParseInt(limitQuery, 10, 32)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.limit.failed_to_parse", true))
			limit = 50
		} else {
			limit = int32(parsedLimit)
			if limit > 250 {
				span.SetAttributes(attribute.Bool("feed.limit.clamped", true))
				limit = 250
			}
		}
	}

	span.SetAttributes(attribute.Int64("feed.limit.parsed", int64(limit)))

	// Get the cursor from the query
	cursor := c.Query("cursor")

	span.SetAttributes(attribute.String("feed.cursor.raw", cursor))

	var posts []*search.Post

	// Get cluster posts if a cluster is specified
	if cluster != nil {
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForCluster(ctx, *cluster, fg.DefaultLookback, limit, cursor)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				span.SetAttributes(attribute.Bool("feed.cluster.not_found", true))
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			span.SetAttributes(attribute.Bool("feed.cluster.error", true))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		posts = postsFromRegistry
	} else { // Otherwise lookup labels
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForLabel(ctx, feedName, fg.DefaultLookback, limit, cursor)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				span.SetAttributes(attribute.Bool("feed.label.not_found", true))
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			span.SetAttributes(attribute.Bool("feed.label.error", true))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		posts = postsFromRegistry
	}

	span.SetAttributes(attribute.Int("feed.posts_returned", len(posts)))

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

	feedRequestLatency.WithLabelValues(feedName).Observe(time.Since(start).Seconds())

	c.JSON(http.StatusOK, feedSkeleton)
}
