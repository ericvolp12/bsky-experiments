package feedgenerator

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
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

var animalLabels = []string{
	"cv:bird",
	"cv:cat",
	"cv:dog",
	"cv:horse",
	"cv:sheep",
	"cv:cow",
	"cv:elephant",
	"cv:bear",
	"cv:zebra",
	"cv:giraffe",
}

var foodLabels = []string{
	"cv:banana",
	"cv:apple",
	"cv:sandwich",
	"cv:orange",
	"cv:broccoli",
	"cv:carrot",
	"cv:hot dog",
	"cv:pizza",
	"cv:donut",
	"cv:cake",
}

type FeedGenerator struct {
	PostRegistry          *search.PostRegistry
	Client                *xrpc.Client
	ClusterManager        *clusters.ClusterManager
	LegacyFeedNames       map[string]string
	DefaultLookback       int32
	AcceptableURIPrefixes []string
	FeedGeneratorCache    FeedGeneratorDescriptionCacheItem
	FeedGeneratorCacheTTL time.Duration
}

type FeedPostItem struct {
	Post string `json:"post"`
}

type FeedSkeleton struct {
	Feed   []FeedPostItem `json:"feed"`
	Cursor *string        `json:"cursor,omitempty"`
}

type FeedGeneratorDescriptionCacheItem struct {
	FeedGeneratorDescription FeedGeneratorDescription
	ExpiresAt                time.Time
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
		"cl-web3":      "cluster-web3",
		"cl-tqsp":      "cluster-tq-shitposters",
		"cl-eng":       "cluster-eng",
		"cl-wrestling": "cluster-wrestling",
		"cl-turkish":   "cluster-turkish",
		"cl-japanese":  "cluster-japanese",
		"cl-brasil":    "cluster-brasil",
		"cl-korean":    "cluster-korean",
		"cl-tpot":      "cluster-tpot",
		"cl-persian":   "cluster-persian",
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
		FeedGeneratorCache:    FeedGeneratorDescriptionCacheItem{},
		FeedGeneratorCacheTTL: 5 * time.Minute,
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

	if fg.FeedGeneratorCache.ExpiresAt.After(time.Now()) {
		span.SetAttributes(attribute.String("cache", "hit"))
		c.JSON(http.StatusOK, fg.FeedGeneratorCache.FeedGeneratorDescription)
		return
	}

	span.SetAttributes(attribute.String("cache", "miss"))

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

	for legacyFeed, _ := range fg.LegacyFeedNames {
		feedDescriptions = append(feedDescriptions, FeedDescription{URI: feedPrefix + legacyFeed})
	}

	for _, label := range labels {
		feedDescriptions = append(feedDescriptions, FeedDescription{URI: feedPrefix + label})
	}

	feedGeneratorDescription := FeedGeneratorDescription{
		DID:   "did:web:feedsky.jazco.io",
		Feeds: feedDescriptions,
	}

	span.SetAttributes(attribute.Int("feeds.length", len(feedDescriptions)))

	fg.FeedGeneratorCache = FeedGeneratorDescriptionCacheItem{
		ExpiresAt:                time.Now().Add(fg.FeedGeneratorCacheTTL),
		FeedGeneratorDescription: feedGeneratorDescription,
	}

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

	bloomFilterMaxSize := uint(1000)     // expected number of items in the bloom filter
	bloomFilterFalsePositiveRate := 0.01 // false positive rate of the bloom filter

	start := time.Now()
	feedQuery := c.Query("feed")
	if feedQuery == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed query parameter is required"})
		return
	}

	c.Set("feedQuery", feedQuery)
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
	c.Set("feedName", feedName)

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

	c.Set("limit", int64(limit))

	span.SetAttributes(attribute.Int64("feed.limit.parsed", int64(limit)))

	// Get the cursor from the query (post_id:hotness)
	cursor := c.Query("cursor")
	c.Set("cursor", cursor)
	cursorPostID := ""
	cursorHotness := float64(-1)
	var bloomFilter *bloom.BloomFilter

	span.SetAttributes(attribute.String("feed.cursor.raw", cursor))
	if cursor != "" {
		cursorParts := strings.Split(cursor, ":")
		if len(cursorParts) != 3 {
			span.SetAttributes(attribute.Bool("feed.cursor.invalid", true))
			c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid"})
			return
		}
		cursorPostID = cursorParts[0]
		parsedHotness, err := strconv.ParseFloat(cursorParts[1], 64)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.cursor.failed_to_parse", true))
			c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid (failed to parse hotness)"})
			return
		}
		cursorHotness = parsedHotness

		// grab the bloom filter from the cursor
		filterString := cursorParts[2]
		// convert the string back to a byte slice
		filterBytes, err := base64.URLEncoding.DecodeString(filterString)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.cursor.failed_to_decode_filter", true))
			c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid (failed to decode filter)"})
			return
		}
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
		err = bloomFilter.UnmarshalBinary(filterBytes)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.cursor.failed_to_unmarshal_filter", true))
			c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid (failed to unmarshal filter)"})
			return
		}

		span.SetAttributes(attribute.Float64("feed.cursor.hotness", cursorHotness))
	}

	if bloomFilter == nil {
		// create a new bloom filter
		bloomFilter = bloom.NewWithEstimates(bloomFilterMaxSize, bloomFilterFalsePositiveRate)
	}

	var posts []*search.Post

	// Get cluster posts if a cluster is specified
	if cluster != nil {
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForCluster(ctx, *cluster, fg.DefaultLookback, limit, cursorPostID)
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
	} else if feedName == "animals" {
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForLabelsByHotness(ctx, animalLabels, limit, cursorHotness)
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
	} else if feedName == "food" {
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForLabelsByHotness(ctx, foodLabels, limit, cursorHotness)
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
	} else { // Otherwise lookup labels
		postsFromRegistry, err := fg.PostRegistry.GetPostsPageForLabelByHotness(ctx, feedName, limit, cursorHotness)
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
	feedItems := []FeedPostItem{}
	var newCursor string

	if len(posts) > 0 {
		for _, post := range posts {
			alreadySeen := bloomFilter.Test([]byte(post.ID))
			if !alreadySeen {
				postAtURL := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.AuthorDID, post.ID)
				feedItems = append(feedItems, FeedPostItem{Post: postAtURL})
				bloomFilter.AddString(post.ID)
			}
		}

		// serialize the filter to a byte slice
		filterBytes, err := bloomFilter.MarshalBinary()
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.filter.failed_to_marshal", true))
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to marshal filter: %s", err.Error())})
			return
		}

		// convert the byte slice to a URL-safe string
		filterString := base64.URLEncoding.EncodeToString(filterBytes)

		lastPostHotness := 0.0
		if posts[len(posts)-1].Hotness != nil {
			// get the hotness of the last post
			lastPostHotness = *posts[len(posts)-1].Hotness
		}

		// construct the cursor with the last post ID, hotness, and serialized filter
		newCursor = fmt.Sprintf("%s:%f:%s", posts[len(posts)-1].ID, lastPostHotness, filterString)
	}

	feedSkeleton := FeedSkeleton{
		Feed: feedItems,
	}

	feedSkeleton.Cursor = &newCursor

	feedRequestLatency.WithLabelValues(feedName).Observe(time.Since(start).Seconds())

	c.JSON(http.StatusOK, feedSkeleton)
}
