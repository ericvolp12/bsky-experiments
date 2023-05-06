package events

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/bluesky-social/indigo/api/bsky"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// ProfileCacheEntry is a struct that holds a profile and an expiration time
type ProfileCacheEntry struct {
	Profile *bsky.ActorDefs_ProfileViewDetailed
	Expire  time.Time
}

// PostCacheEntry is a struct that holds a PostView and an expiration time
type PostCacheEntry struct {
	Post         *bsky.FeedDefs_PostView
	TimeoutCount int
	Expire       time.Time
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context, workerID int) error {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "RefreshAuthToken")
	defer span.End()
	span.AddEvent("RefreshAuthToken:AcquireClientLock")
	worker.ClientMux.Lock()
	span.AddEvent("RefreshAuthToken:ClientLockAcquired")
	err := intXRPC.RefreshAuth(ctx, worker.Client)
	span.AddEvent("RefreshAuthToken:ReleaseClientLock")
	worker.ClientMux.Unlock()
	return err
}

// ResolveProfile resolves a profile from a DID using the cache or the API
func (bsky *BSky) ResolveProfile(ctx context.Context, did string, workerID int) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveProfile")
	defer span.End()
	// Check the cache first
	entry, ok := bsky.profileCache.Get(did)
	if ok {
		cacheEntry := entry.(ProfileCacheEntry)
		if cacheEntry.Expire.After(time.Now()) {
			if cacheEntry.Expire.After(time.Now()) {
				cacheHits.WithLabelValues("profile").Inc()
				span.SetAttributes(attribute.Bool("profile_cache_hit", true))
				return cacheEntry.Profile, nil
			}
		}
	}

	span.SetAttributes(attribute.Bool("profile_cache_hit", false))
	cacheMisses.WithLabelValues("profile").Inc()

	//Lock the client
	span.AddEvent("ResolveProfile:AcquireClientRLock")
	worker.ClientMux.RLock()
	span.AddEvent("ResolveProfile:ClientRLockAcquired")
	// Get the profile from the API
	start := time.Now()
	profile, err := appbsky.ActorGetProfile(ctx, worker.Client, did)
	elapsed := time.Since(start)
	apiCallDurationHistogram.WithLabelValues("ActorGetProfile").Observe(elapsed.Seconds())
	// Unlock the client
	span.AddEvent("ResolveProfile:ReleaseClientRLock")
	worker.ClientMux.RUnlock()
	if err != nil {
		return nil, err
	}

	if profile == nil {
		span.SetAttributes(attribute.Bool("profile_found", false))
		return nil, fmt.Errorf("profile not found for: %s", did)
	}

	span.SetAttributes(attribute.Bool("profile_found", true))

	newEntry := ProfileCacheEntry{
		Profile: profile,
		Expire:  time.Now().Add(bsky.profileCacheTTL),
	}

	// Cache the profile
	bsky.profileCache.Add(did, newEntry)

	// Update the cache size metric
	cacheSize.WithLabelValues("profile").Add(float64(unsafe.Sizeof(newEntry)))

	return profile, nil
}

// ResolvePost resolves a post from a URI using the cache or the API
func (bsky *BSky) ResolvePost(ctx context.Context, uri string, workerID int) (*bsky.FeedDefs_PostView, error) {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolvePost")
	defer span.End()
	// Check the cache first
	entry, ok := bsky.postCache.Get(uri)
	if ok {
		cacheEntry := entry.(PostCacheEntry)
		if cacheEntry.Expire.After(time.Now()) {
			// If we've timed out 5 times in a row trying to get this post, it's probably a hellthread
			// Return the cached post and don't try to get it again
			if cacheEntry.TimeoutCount > 5 {
				span.SetAttributes(attribute.Bool("post_timeout_cached", true))
				return nil, fmt.Errorf("hellthread detected - returning cached post timeout for: %s", uri)
			} else if cacheEntry.Post != nil {
				cacheHits.WithLabelValues("post").Inc()
				span.SetAttributes(attribute.Bool("post_cache_hit", true))
				return cacheEntry.Post, nil
			}
		}
	}

	span.SetAttributes(attribute.Bool("post_cache_hit", false))
	cacheMisses.WithLabelValues("post").Inc()

	//Lock the client
	span.AddEvent("ResolvePost:AcquireClientRLock")
	worker.ClientMux.RLock()
	span.AddEvent("ResolvePost:ClientRLockAcquired")
	// Get the post from the API
	posts, err := FeedGetPostsWithTimeout(ctx, worker.Client, []string{uri}, time.Second*2)
	// Unlock the client
	span.AddEvent("ResolvePost:ReleaseClientRLock")
	worker.ClientMux.RUnlock()
	if err != nil {
		// Check if the error is a timeout
		var timeoutErr *TimeoutError
		if errors.As(err, &timeoutErr) {
			span.SetAttributes(attribute.Bool("post_timeout", true))
			entry, ok := bsky.postCache.Get(uri)
			if ok {
				cacheEntry := entry.(PostCacheEntry)
				// If the post is cached, increment the timeout count
				cacheEntry.TimeoutCount++
				cacheEntry.Expire = time.Now().Add(bsky.postCacheTTL)
				bsky.postCache.Add(uri, cacheEntry)
			} else {
				// If the post isn't cached, cache it with a timeout count of 1
				bsky.postCache.Add(uri, PostCacheEntry{
					Post:         nil,
					TimeoutCount: 1,
					Expire:       time.Now().Add(bsky.postCacheTTL),
				})
			}
		}
		return nil, err
	}

	if posts != nil &&
		posts.Posts != nil &&
		len(posts.Posts) > 0 &&
		posts.Posts[0].Author != nil {

		span.SetAttributes(attribute.Bool("profile_found", true))

		newEntry := PostCacheEntry{
			Post:   posts.Posts[0],
			Expire: time.Now().Add(bsky.postCacheTTL),
		}

		// Cache the profile
		bsky.postCache.Add(uri, newEntry)

		// Update the cache size metric
		cacheSize.WithLabelValues("post").Add(float64(unsafe.Sizeof(newEntry)))

		return posts.Posts[0], nil
	}

	span.SetAttributes(attribute.Bool("post_found", false))
	return nil, fmt.Errorf("post not found for: %s", uri)
}
