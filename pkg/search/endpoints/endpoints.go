package endpoints

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/ericvolp12/bsky-experiments/pkg/layout"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/usercount"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	"go.opentelemetry.io/otel"
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

type StatsCacheEntry struct {
	Stats      AuthorStatsResponse
	Expiration time.Time
}

type AuthorStatsResponse struct {
	TotalUsers    int                               `json:"total_users"`
	TotalAuthors  int64                             `json:"total_authors"`
	MeanPostCount float64                           `json:"mean_post_count"`
	Percentiles   []search.Percentile               `json:"percentiles"`
	Brackets      []search.Bracket                  `json:"brackets"`
	UpdatedAt     time.Time                         `json:"updated_at"`
	TopPosters    []search_queries.GetTopPostersRow `json:"top_posters"`
}

type API struct {
	PostRegistry search.PostRegistry
	UserCount    *usercount.UserCount

	SocialGraph *graph.Graph

	LayoutServiceHost string

	ThreadViewCacheTTL time.Duration
	ThreadViewCache    *lru.ARCCache
	LayoutCacheTTL     time.Duration
	LayoutCache        *lru.ARCCache
	StatsCacheTTL      time.Duration
	StatsCache         *StatsCacheEntry
	StatsCacheRWMux    *sync.RWMutex
}

func NewAPI(
	postRegistry search.PostRegistry,
	userCount *usercount.UserCount,
	socialGraphPath string,
	layoutServiceHost string,
	threadViewCacheTTL time.Duration,
	layoutCacheTTL time.Duration,
	statsCacheTTL time.Duration,
) (*API, error) {

	// Read the graph from the Binary file
	readerWriter := graph.BinaryGraphReaderWriter{}

	g1, err := readerWriter.ReadGraph(socialGraphPath)
	if err != nil {
		return nil, fmt.Errorf("error reading graph: %w", err)
	}

	// Hellthread is around 300KB right now so 1000 worst-case threads should be around 300MB
	threadViewCache, err := lru.NewARC(1000)
	if err != nil {
		return nil, fmt.Errorf("error initializing thread view cache: %w", err)
	}

	layoutCache, err := lru.NewARC(500)
	if err != nil {
		return nil, fmt.Errorf("error initializing layout cache: %w", err)
	}

	return &API{
		PostRegistry:       postRegistry,
		UserCount:          userCount,
		SocialGraph:        &g1,
		LayoutServiceHost:  layoutServiceHost,
		ThreadViewCacheTTL: threadViewCacheTTL,
		ThreadViewCache:    threadViewCache,
		LayoutCacheTTL:     layoutCacheTTL,
		LayoutCache:        layoutCache,
		StatsCacheTTL:      statsCacheTTL,
		StatsCacheRWMux:    &sync.RWMutex{},
	}, nil
}

func (api *API) GetSocialDistance(c *gin.Context) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GetSocialDistance")
	defer span.End()

	src := c.Query("src")
	dest := c.Query("dest")

	if src == "" || dest == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "src and dest must be provided"})
		return
	}

	// Make sure src and dst DIDs are in the graph
	if _, ok := api.SocialGraph.Nodes[graph.NodeID(src)]; !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("src with DID '%s' not found", src)})
		return
	}
	if _, ok := api.SocialGraph.Nodes[graph.NodeID(dest)]; !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("dest with DID '%s' not found", dest)})
		return
	}

	distance, path, weights := api.SocialGraph.FindSocialDistance(graph.NodeID(src), graph.NodeID(dest))

	// Return the distance, path, and weights with Handles and DIDs

	// Get the handles for the nodes in the path
	handles := make([]string, len(path))
	for i, nodeID := range path {
		handles[i] = api.SocialGraph.Nodes[nodeID].Handle
	}

	// Make sure weights and distnaces aren't infinite before returning them
	for i, weight := range weights {
		if math.IsInf(weight, 0) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("infinite weight for edge %s -> %s", path[i-1], path[i])})
			return
		}
	}

	if math.IsInf(distance, 0) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("infinite distance between %s and %s", src, dest)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"distance": distance, "did_path": path, "handle_path": handles, "weights": weights})
}

func (api *API) GetAuthorStats(c *gin.Context) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GetAuthorStats")
	defer span.End()

	timeout := 30 * time.Second
	timeWaited := 0 * time.Second
	sleepTime := 100 * time.Millisecond

	// Wait for the stats cache to be populated
	if api.StatsCache == nil {
		span.AddEvent("GetAuthorStats:WaitForStatsCache")
		for api.StatsCache == nil {
			if timeWaited > timeout {
				c.JSON(http.StatusRequestTimeout, gin.H{"error": "timed out waiting for stats cache to populate"})
				return
			}

			time.Sleep(sleepTime)
			timeWaited += sleepTime
		}
	}

	// Lock the stats mux for reading
	span.AddEvent("GetAuthorStats:AcquireStatsCacheRLock")
	api.StatsCacheRWMux.RLock()
	span.AddEvent("GetAuthorStats:StatsCacheRLockAcquired")

	statsFromCache := api.StatsCache.Stats

	// Unlock the stats mux for reading
	span.AddEvent("GetAuthorStats:ReleaseStatsCacheRLock")
	api.StatsCacheRWMux.RUnlock()

	c.JSON(http.StatusOK, statsFromCache)
	return
}

func (api *API) RefreshSiteStats(ctx context.Context) error {
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "RefreshSiteStats")
	defer span.End()

	authorStats, err := api.PostRegistry.GetAuthorStats(ctx)
	if err != nil {
		log.Printf("Error getting author stats: %v", err)
		return fmt.Errorf("error getting author stats: %w", err)
	}

	if authorStats == nil {
		log.Printf("Author stats returned nil")
		return errors.New("author stats returned nil")
	}

	// Get the top 25 posters
	topPosters, err := api.PostRegistry.GetTopPosters(ctx, 25)
	if err != nil {
		log.Printf("Error getting top posters: %v", err)
		return fmt.Errorf("error getting top posters: %w", err)
	}

	// Get usercount from UserCount service
	userCount, err := api.UserCount.GetUserCount(ctx)
	if err != nil {
		log.Printf("Error getting user count: %v", err)
		return fmt.Errorf("error getting user count: %w", err)
	}

	// Update the metrics
	totalUsers.Set(float64(userCount))
	totalAuthors.Set(float64(authorStats.TotalAuthors))
	meanPostCount.Set(authorStats.MeanPostCount)

	// Lock the stats mux for writing
	span.AddEvent("RefreshSiteStats:AcquireStatsCacheWLock")
	api.StatsCacheRWMux.Lock()
	span.AddEvent("RefreshSiteStats:StatsCacheWLockAcquired")
	// Update the plain old struct cache
	api.StatsCache = &StatsCacheEntry{
		Stats: AuthorStatsResponse{
			TotalUsers:    userCount,
			TotalAuthors:  authorStats.TotalAuthors,
			MeanPostCount: authorStats.MeanPostCount,
			Percentiles:   authorStats.Percentiles,
			Brackets:      authorStats.Brackets,
			UpdatedAt:     authorStats.UpdatedAt,
			TopPosters:    topPosters,
		},
		Expiration: time.Now().Add(api.StatsCacheTTL),
	}

	// Unlock the stats mux for writing
	span.AddEvent("RefreshSiteStats:ReleaseStatsCacheWLock")
	api.StatsCacheRWMux.Unlock()

	return nil
}

func (api *API) LayoutThread(ctx context.Context, rootPostID string, threadView []search.PostView) ([]layout.ThreadViewLayout, error) {
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "LayoutThread")
	defer span.End()

	// Check for the layout in the ARC Cache
	entry, ok := api.LayoutCache.Get(rootPostID)
	if ok {
		cacheEntry := entry.(LayoutCacheEntry)
		if cacheEntry.Expiration.After(time.Now()) {
			cacheHits.WithLabelValues("layout").Inc()
			span.SetAttributes(attribute.Bool("caches.layouts.hit", true))
			return cacheEntry.Layout, nil
		}
		// If the layout is expired, remove it from the cache
		api.LayoutCache.Remove(rootPostID)
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
	tracer := otel.Tracer("search-api")
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
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GetThreadView")
	defer span.End()

	// Check for the thread in the ARC Cache
	entry, ok := api.ThreadViewCache.Get(postID)
	if ok {
		cacheEntry := entry.(ThreadViewCacheEntry)
		if cacheEntry.Expiration.After(time.Now()) {
			cacheHits.WithLabelValues("thread").Inc()
			span.SetAttributes(attribute.Bool("caches.threads.hit", true))
			return cacheEntry.ThreadView, nil
		}
		// If the thread is expired, remove it from the cache
		api.ThreadViewCache.Remove(postID)
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
	tracer := otel.Tracer("search-api")
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
