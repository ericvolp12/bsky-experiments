package endpoints

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/layout"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/ericvolp12/bsky-experiments/pkg/search/clusters"

	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/usercount"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	TotalUsers      int                               `json:"total_users"`
	TotalAuthors    int64                             `json:"total_authors"`
	TotalPosts      int64                             `json:"total_posts"`
	HellthreadPosts int64                             `json:"hellthread_posts"`
	MeanPostCount   float64                           `json:"mean_post_count"`
	Percentiles     []search.Percentile               `json:"percentiles"`
	Brackets        []search.Bracket                  `json:"brackets"`
	UpdatedAt       time.Time                         `json:"updated_at"`
	TopPosters      []search_queries.GetTopPostersRow `json:"top_posters"`
}

type API struct {
	PostRegistry *search.PostRegistry
	UserCount    *usercount.UserCount

	ClusterManager *clusters.ClusterManager

	LayoutServiceHost string

	ThreadViewCacheTTL time.Duration
	ThreadViewCache    *lru.ARCCache[string, ThreadViewCacheEntry]
	LayoutCacheTTL     time.Duration
	LayoutCache        *lru.ARCCache[string, LayoutCacheEntry]
	StatsCacheTTL      time.Duration
	StatsCache         *StatsCacheEntry
	StatsCacheRWMux    *sync.RWMutex
}

func NewAPI(
	postRegistry *search.PostRegistry,
	userCount *usercount.UserCount,
	graphJSONUrl string,
	layoutServiceHost string,
	threadViewCacheTTL time.Duration,
	layoutCacheTTL time.Duration,
	statsCacheTTL time.Duration,
) (*API, error) {
	// Hellthread is around 300KB right now so 1000 worst-case threads should be around 300MB
	threadViewCache, err := lru.NewARC[string, ThreadViewCacheEntry](1000)
	if err != nil {
		return nil, fmt.Errorf("error initializing thread view cache: %w", err)
	}

	layoutCache, err := lru.NewARC[string, LayoutCacheEntry](500)
	if err != nil {
		return nil, fmt.Errorf("error initializing layout cache: %w", err)
	}

	clusterManager, err := clusters.NewClusterManager(graphJSONUrl)
	if err != nil {
		return nil, fmt.Errorf("error initializing cluster manager: %w", err)
	}

	return &API{
		PostRegistry:       postRegistry,
		UserCount:          userCount,
		ClusterManager:     clusterManager,
		LayoutServiceHost:  layoutServiceHost,
		ThreadViewCacheTTL: threadViewCacheTTL,
		ThreadViewCache:    threadViewCache,
		LayoutCacheTTL:     layoutCacheTTL,
		LayoutCache:        layoutCache,
		StatsCacheTTL:      statsCacheTTL,
		StatsCacheRWMux:    &sync.RWMutex{},
	}, nil
}

func (api *API) GetPost(c *gin.Context) {
	postID := c.Param("id")
	post, err := api.PostRegistry.GetPost(c.Request.Context(), postID)
	if err != nil {
		if errors.As(err, &search.NotFoundError{}) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("post with ID '%s' not found", postID)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.JSON(http.StatusOK, post)
}

func (api *API) GetClusterForHandle(c *gin.Context) {
	handle := c.Param("handle")
	cluster, err := api.ClusterManager.GetClusterForHandle(c.Request.Context(), handle)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if cluster == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("handle '%s' not found or not assigned to a labeled cluster", handle)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"cluster_id": cluster.ID, "cluster_name": cluster.Name})
}

func (api *API) GetClusterForDID(c *gin.Context) {
	did := c.Param("did")
	cluster, err := api.ClusterManager.GetClusterForDID(c.Request.Context(), did)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if cluster == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("did '%s' not found or not assigned to a labeled cluster", did)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"cluster_id": cluster.ID, "cluster_name": cluster.Name})
}

func (api *API) GetClusterList(c *gin.Context) {
	c.JSON(http.StatusOK, api.ClusterManager.Clusters)
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
	totalPostCount.Set(float64(authorStats.TotalPosts))
	hellthreadPostCount.Set(float64(authorStats.HellthreadPosts))

	// Lock the stats mux for writing
	span.AddEvent("RefreshSiteStats:AcquireStatsCacheWLock")
	api.StatsCacheRWMux.Lock()
	span.AddEvent("RefreshSiteStats:StatsCacheWLockAcquired")
	// Update the plain old struct cache
	api.StatsCache = &StatsCacheEntry{
		Stats: AuthorStatsResponse{
			TotalUsers:      userCount,
			TotalAuthors:    authorStats.TotalAuthors,
			TotalPosts:      authorStats.TotalPosts,
			HellthreadPosts: authorStats.HellthreadPosts,
			MeanPostCount:   authorStats.MeanPostCount,
			Percentiles:     authorStats.Percentiles,
			Brackets:        authorStats.Brackets,
			UpdatedAt:       authorStats.UpdatedAt,
			TopPosters:      topPosters,
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

type GraphOptRequest struct {
	Username    string `json:"username"`
	AppPassword string `json:"appPassword"`
}

func (api *API) GraphOptOut(c *gin.Context) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GraphOptOut")
	defer span.End()

	// get Username and appPassword from Post Body
	var optOutRequest GraphOptRequest
	if err := c.ShouldBindJSON(&optOutRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create an instrumented transport for OTEL Tracing of HTTP Requests
	instrumentedTransport := otelhttp.NewTransport(&http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// Create the XRPC Client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: instrumentedTransport,
		},
		Host: "https://bsky.social",
	}

	// Create a new XRPC Client authenticated as the user
	ses, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: optOutRequest.Username,
		Password:   optOutRequest.AppPassword,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error creating authenticated ATProto session: %w\nYour username and/or AppPassword may be incorrect", err).Error()})
		return
	}

	client.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	// Try to Get the user's profile to confirm that the user is authenticated
	profile, err := bsky.ActorGetProfile(ctx, &client, ses.Handle)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting user profile while confirming identity: %w\n"+
			"There may have been a problem communicating with the BSky API, "+
			"as we can't confirm your identity without that info, we are unable to process your opt-out right now.", err).Error()})
		return
	}

	if profile == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to get your profile from the BSky API when confirming your identity, we can't process your opt-out right now."})
		return
	}

	// Create an OptOut record for the user
	err = api.PostRegistry.UpdateAuthorOptOut(ctx, ses.Did, true)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error while updating author opt out record in the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "You have successfully opted out of the Atlas"})
}

func (api *API) GraphOptIn(c *gin.Context) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GraphOptIn")
	defer span.End()

	// get Username and appPassword from Post Body
	var optInRequest GraphOptRequest
	if err := c.ShouldBindJSON(&optInRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create an instrumented transport for OTEL Tracing of HTTP Requests
	instrumentedTransport := otelhttp.NewTransport(&http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	// Create the XRPC Client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: instrumentedTransport,
		},
		Host: "https://bsky.social",
	}

	// Create a new XRPC Client authenticated as the user
	ses, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: optInRequest.Username,
		Password:   optInRequest.AppPassword,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error creating authenticated ATProto session: %w\nYour username and/or AppPassword may be incorrect", err).Error()})
		return
	}

	client.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	// Try to Get the user's profile to confirm that the user is authenticated
	profile, err := bsky.ActorGetProfile(ctx, &client, ses.Handle)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting user profile while confirming identity: %w\n"+
			"There may have been a problem communicating with the BSky API, "+
			"as we can't confirm your identity without that info, we are unable to process your opt-in right now.", err).Error()})
		return
	}

	if profile == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to get your profile from the BSky API when confirming your identity, we can't process your opt-in right now."})
		return
	}

	// Set the user's opt-out record to false
	err = api.PostRegistry.UpdateAuthorOptOut(ctx, ses.Did, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error while updating author opt out record in the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "You have successfully opted back into the Atlas"})
}

func (api *API) GetOptedOutAuthors(c *gin.Context) {
	ctx := c.Request.Context()
	tracer := otel.Tracer("search-api")
	ctx, span := tracer.Start(ctx, "GetOptedOutAuthors")
	defer span.End()

	authors, err := api.PostRegistry.GetOptedOutAuthors(ctx)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error getting opted out authors from the Atlas Database: %w\n"+
			"Please feel free to @mention jaz.bsky.social on the Skyline for support for this error, since it's likely an issue with something Jaz can fix.", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"authors": authors})
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
