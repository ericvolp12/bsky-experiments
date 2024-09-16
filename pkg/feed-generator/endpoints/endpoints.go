package endpoints

import (
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/bsky-experiments/pkg/auth"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	feedgenerator "github.com/ericvolp12/bsky-experiments/pkg/feed-generator"
	"golang.org/x/time/rate"

	"github.com/gin-gonic/gin"
	"github.com/whyrusleeping/go-did"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type DescriptionCacheItem struct {
	Description appbsky.FeedDescribeFeedGenerator_Output
	ExpiresAt   time.Time
}

type Endpoints struct {
	FeedGenerator   *feedgenerator.FeedGenerator
	GraphJSONUrl    string
	FeedUsers       map[string][]string
	usersLk         sync.RWMutex
	UniqueSeenUsers *bloom.BloomFilter

	dir *identity.CacheDirectory

	Store *store.Store

	DescriptionCache    *DescriptionCacheItem
	DescriptionCacheTTL time.Duration
}

type DidResponse struct {
	Context []string      `json:"@context"`
	ID      string        `json:"id"`
	Service []did.Service `json:"service"`
}

func NewEndpoints(feedGenerator *feedgenerator.FeedGenerator, graphJSONUrl string, store *store.Store) (*Endpoints, error) {
	uniqueSeenUsers := bloom.NewWithEstimates(1000000, 0.01)

	base := identity.BaseDirectory{
		PLCURL: "https://plc.directory",
		HTTPClient: http.Client{
			Timeout: time.Second * 15,
		},
		PLCLimiter:            rate.NewLimiter(rate.Limit(10), 1),
		TryAuthoritativeDNS:   true,
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}
	dir := identity.NewCacheDirectory(&base, 250_000, time.Hour*24, time.Minute*2, time.Minute*5)

	return &Endpoints{
		FeedGenerator:       feedGenerator,
		GraphJSONUrl:        graphJSONUrl,
		UniqueSeenUsers:     uniqueSeenUsers,
		FeedUsers:           map[string][]string{},
		dir:                 &dir,
		Store:               store,
		DescriptionCacheTTL: 30 * time.Minute,
	}, nil
}

func (ep *Endpoints) GetWellKnownDID(c *gin.Context) {
	tracer := otel.Tracer("feedgenerator")
	_, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:GetWellKnownDID")
	defer span.End()

	// Use a custom struct to fix missing omitempty on did.Document
	didResponse := DidResponse{
		Context: ep.FeedGenerator.DIDDocument.Context,
		ID:      ep.FeedGenerator.DIDDocument.ID.String(),
		Service: ep.FeedGenerator.DIDDocument.Service,
	}

	c.JSON(http.StatusOK, didResponse)
}

func (ep *Endpoints) DescribeFeedGenerator(c *gin.Context) {
	tracer := otel.Tracer("feedgenerator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:DescribeFeedGenerator")
	defer span.End()

	if ep.DescriptionCache != nil && ep.DescriptionCache.ExpiresAt.After(time.Now()) {
		span.SetAttributes(attribute.String("cache.hit", "true"))
		c.JSON(http.StatusOK, ep.DescriptionCache.Description)
		return
	}

	span.SetAttributes(attribute.String("cache.hit", "false"))

	feedDescriptions := []*appbsky.FeedDescribeFeedGenerator_Feed{}

	for _, feed := range ep.FeedGenerator.Feeds {
		newDescriptions, err := feed.Describe(ctx)
		if err != nil {
			span.RecordError(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		for _, newDescription := range newDescriptions {
			description := newDescription
			feedDescriptions = append(feedDescriptions, &description)
		}
	}

	span.SetAttributes(attribute.Int("feeds.length", len(feedDescriptions)))

	feedGeneratorDescription := appbsky.FeedDescribeFeedGenerator_Output{
		Did:   ep.FeedGenerator.FeedActorDID.String(),
		Feeds: feedDescriptions,
	}

	ep.DescriptionCache = &DescriptionCacheItem{
		Description: feedGeneratorDescription,
		ExpiresAt:   time.Now().Add(ep.DescriptionCacheTTL),
	}

	c.JSON(http.StatusOK, feedGeneratorDescription)
}

func (ep *Endpoints) GetFeedSkeleton(c *gin.Context) {
	// Incoming requests should have a query parameter "feed" that looks like:
	// 		at://did:web:feedsky.jazco.io/app.bsky.feed.generator/feed-name
	// Also a query parameter "limit" that looks like: 50
	// Also a query parameter "cursor" that is either the empty string
	// or the cursor returned from a previous request
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:GetFeedSkeleton")
	defer span.End()

	// Get userDID from the request context, which is set by the auth middleware
	userDID := c.GetString("user_did")

	start := time.Now()

	feedQuery := c.Query("feed")
	if feedQuery == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed query parameter is required"})
		return
	}

	c.Set("feedQuery", feedQuery)
	span.SetAttributes(attribute.String("feed.query", feedQuery))

	feedPrefix := ""
	for _, acceptablePrefix := range ep.FeedGenerator.AcceptableURIPrefixes {
		if strings.HasPrefix(feedQuery, acceptablePrefix) {
			feedPrefix = acceptablePrefix
			break
		}
	}

	if feedPrefix == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "this feed generator does not serve feeds for the given DID"})
		return
	}

	// Get the feed name from the query
	feedName := strings.TrimPrefix(feedQuery, feedPrefix)
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feed name is required"})
		return
	}

	// Count the user
	ep.ProcessUser(feedName, userDID)

	span.SetAttributes(attribute.String("feed.name", feedName))
	c.Set("feedName", feedName)
	feedRequestCounter.WithLabelValues(feedName).Inc()

	// Get the limit from the query, default to 50, maximum of 250
	limit := int64(50)
	limitQuery := c.Query("limit")
	span.SetAttributes(attribute.String("feed.limit.raw", limitQuery))
	if limitQuery != "" {
		parsedLimit, err := strconv.ParseInt(limitQuery, 10, 64)
		if err != nil {
			span.SetAttributes(attribute.Bool("feed.limit.failed_to_parse", true))
			limit = 50
		} else {
			limit = parsedLimit
			if limit > 250 {
				span.SetAttributes(attribute.Bool("feed.limit.clamped", true))
				limit = 250
			}
		}
	}

	span.SetAttributes(attribute.Int64("feed.limit.parsed", limit))

	// Get the cursor from the query
	cursor := c.Query("cursor")
	c.Set("cursor", cursor)

	if ep.FeedGenerator.FeedMap == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "feed generator has no feeds configured"})
		return
	}

	feed, ok := ep.FeedGenerator.FeedMap[feedName]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "feed not found"})
		return
	}

	// Get the feed items
	feedItems, newCursor, err := feed.GetPage(ctx, feedName, userDID, limit, cursor)
	if err != nil {
		span.RecordError(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to get feed items: %s", err.Error())})
		return
	}

	span.SetAttributes(attribute.Int("feed.items.length", len(feedItems)))

	feedRequestLatency.WithLabelValues(feedName).Observe(time.Since(start).Seconds())

	c.JSON(http.StatusOK, appbsky.FeedGetFeedSkeleton_Output{
		Feed:   feedItems,
		Cursor: newCursor,
	})
}

func (ep *Endpoints) ProcessUser(feedName string, userDID string) {
	// Check if the user has ever been seen before
	if !ep.UniqueSeenUsers.TestString(userDID) {
		ep.UniqueSeenUsers.AddString(userDID)
		uniqueFeedUserCounter.Inc()
	}

	ep.usersLk.Lock()
	defer ep.usersLk.Unlock()
	// Check if the feed user list exists
	if ep.FeedUsers[feedName] == nil {
		// If not, create the feed user list
		ep.FeedUsers[feedName] = []string{
			userDID,
		}
	} else {
		// Check if the user is already in the list
		for _, existingUserDID := range ep.FeedUsers[feedName] {
			if existingUserDID == userDID {
				return
			}
		}

		ep.FeedUsers[feedName] = append(ep.FeedUsers[feedName], userDID)
	}

	feedUserCounter.WithLabelValues(feedName).Inc()
}

//
//
// Additional endpoints below are for private feeds only, not part of a standard Feed Generator implementation
//
//

func (ep *Endpoints) AssignUserToFeed(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:AssignUserToFeed")
	defer span.End()

	rawAuthEntity, exists := c.Get("feed.auth.entity")
	if !exists {
		span.SetAttributes(attribute.Bool("feed.assign_label.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: no user DID in context"})
		return
	}

	// Cast the rawAuthEntity to a FeedAuthEntity
	authEntity, ok := rawAuthEntity.(*auth.FeedAuthEntity)
	if !ok || authEntity == nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: could not cast auth entity"})
		return
	}

	feedName := c.Query("feedName")
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feedName is required"})
		return
	}

	if authEntity.FeedAlias != feedName {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: you are not authorized to assign users to this feed"})
		return
	}

	targetHandle := c.Query("handle")
	if targetHandle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "handle of user to add to feed is required"})
		return
	}

	handle, err := syntax.ParseHandle(targetHandle)
	if err != nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.invalid_handle", true))
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid handle: %s", err.Error())})
		return
	}

	id, err := ep.dir.LookupHandle(ctx, handle)
	if err != nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.handle_not_found", true))
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("failed to resolve handle to did: %s", err.Error())})
		return
	}

	err = ep.Store.Queries.CreateActorLabel(ctx, store_queries.CreateActorLabelParams{
		ActorDid: id.DID.String(),
		Label:    feedName,
	})
	if err != nil {
		slog.Error("failed to assign label to user", "error", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "success"})
}

func (ep *Endpoints) UnassignUserFromFeed(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:UnassignUserFromFeed")
	defer span.End()

	rawAuthEntity, exists := c.Get("feed.auth.entity")
	if !exists {
		span.SetAttributes(attribute.Bool("feed.assign_label.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: no user DID in context"})
		return
	}

	// Cast the rawAuthEntity to a FeedAuthEntity
	authEntity, ok := rawAuthEntity.(*auth.FeedAuthEntity)
	if !ok || authEntity == nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: could not cast auth entity"})
		return
	}

	feedName := c.Query("feedName")
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feedName is required"})
		return
	}

	if authEntity.FeedAlias != feedName {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: you are not authorized to assign users to this feed"})
		return
	}

	targetHandle := c.Query("handle")
	if targetHandle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "handle of user to add to feed is required"})
		return
	}

	handle, err := syntax.ParseHandle(targetHandle)
	if err != nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.invalid_handle", true))
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid handle: %s", err.Error())})
		return
	}

	id, err := ep.dir.LookupHandle(ctx, handle)
	if err != nil {
		span.SetAttributes(attribute.Bool("feed.assign_label.handle_not_found", true))
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("failed to resolve handle to did: %s", err.Error())})
		return
	}

	err = ep.Store.Queries.DeleteActorLabel(ctx, store_queries.DeleteActorLabelParams{
		ActorDid: id.DID.String(),
		Label:    feedName,
	})
	if err != nil {
		slog.Error("failed to unassign label from user", "error", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "success"})
}

func (ep *Endpoints) GetFeedMembers(c *gin.Context) {
	tracer := otel.Tracer("feed-generator")
	ctx, span := tracer.Start(c.Request.Context(), "FeedGenerator:Endpoints:GetFeedMembers")
	defer span.End()

	rawAuthEntity, exists := c.Get("feed.auth.entity")
	if !exists {
		span.SetAttributes(attribute.Bool("feed.get_members.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: no user DID in context"})
		return
	}

	// Cast the rawAuthEntity to a FeedAuthEntity
	authEntity, ok := rawAuthEntity.(*auth.FeedAuthEntity)
	if !ok || authEntity == nil {
		span.SetAttributes(attribute.Bool("feed.get_members.not_authorized", true))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: could not cast auth entity"})
		return
	}

	feedName := c.Query("feedName")
	if feedName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "feedName is required"})
		return
	}

	if authEntity.FeedAlias != feedName {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authorized: you are not authorized to list the users assigned to this feed"})
		return
	}

	authors, err := ep.Store.Queries.ListActorsByLabel(ctx, feedName)
	if err != nil {
		span.SetAttributes(attribute.Bool("feed.get_members.error", true))
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("error getting authors: %s", err.Error())})
		return
	}

	c.JSON(http.StatusOK, gin.H{"authors": authors})
}

type FeedMeta struct {
	FeedType  string `json:"feed_type"`
	UserCount int    `json:"user_count"`
}

// Debug endpoints for the Feed Generator Admin Dashboard
type GetFeedsResponse struct {
	Feeds map[string]FeedMeta `json:"feeds"`
}

func (ep *Endpoints) GetFeeds(c *gin.Context) {
	_, span := otel.Tracer("feed-generator").Start(c.Request.Context(), "GetFeeds")
	defer span.End()

	feeds := make(map[string]FeedMeta)

	ep.usersLk.RLock()
	for alias, feed := range ep.FeedGenerator.FeedMap {
		feedType := reflect.TypeOf(feed).String()
		feedType = strings.TrimPrefix(feedType, "*")

		feeds[alias] = FeedMeta{
			FeedType:  feedType,
			UserCount: len(ep.FeedUsers[alias]),
		}
	}
	ep.usersLk.RUnlock()

	resp := GetFeedsResponse{
		Feeds: feeds,
	}

	c.JSON(http.StatusOK, resp)
}
