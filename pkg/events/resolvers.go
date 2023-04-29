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

// ThreadCacheEntry is a struct that holds a FeedPostThread and an expiration time
type ThreadCacheEntry struct {
	Thread       *bsky.FeedDefs_ThreadViewPost
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
	// Get the profile from the API
	profile, err := appbsky.ActorGetProfile(ctx, worker.Client, did)
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

// ResolveThread resolves a thread from a URI using the cache or the API
func (bsky *BSky) ResolveThread(ctx context.Context, uri string, workerID int) (*bsky.FeedDefs_ThreadViewPost, error) {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveThread")
	defer span.End()
	// Check the cache first
	entry, ok := bsky.threadCache.Get(uri)
	if ok {
		cacheEntry := entry.(ThreadCacheEntry)
		if cacheEntry.Expire.After(time.Now()) {
			// If we've timed out 5 times in a row trying to get this thread, it's probably a hellthread
			// Return the cached thread and don't try to get it again
			if cacheEntry.TimeoutCount > 5 {
				span.SetAttributes(attribute.Bool("thread_timeout_cached", true))
				return nil, fmt.Errorf("hellthread detected - returning cached thread timeout for: %s", uri)
			} else if cacheEntry.Thread != nil {
				cacheHits.WithLabelValues("thread").Inc()
				span.SetAttributes(attribute.Bool("thread_cache_hit", true))
				return cacheEntry.Thread, nil
			}
		}
	}

	span.SetAttributes(attribute.Bool("thread_cache_hit", false))
	cacheMisses.WithLabelValues("thread").Inc()

	//Lock the client
	span.AddEvent("ResolveThread:AcquireClientRLock")
	worker.ClientMux.RLock()
	// Get the profile from the API
	thread, err := FeedGetPostThreadWithTimeout(ctx, worker.Client, 1, uri, time.Second*2)
	// Unlock the client
	span.AddEvent("ResolveThread:ReleaseClientRLock")
	worker.ClientMux.RUnlock()
	if err != nil {
		// Check if the error is a timeout
		var timeoutErr *TimeoutError
		if errors.As(err, &timeoutErr) {
			span.SetAttributes(attribute.Bool("thread_timeout", true))
			entry, ok := bsky.threadCache.Get(uri)
			if ok {
				cacheEntry := entry.(ThreadCacheEntry)
				// If the thread is cached, increment the timeout count
				cacheEntry.TimeoutCount++
				cacheEntry.Expire = time.Now().Add(bsky.threadCacheTTL)
				bsky.threadCache.Add(uri, cacheEntry)
			} else {
				// If the thread isn't cached, cache it with a timeout count of 1
				bsky.threadCache.Add(uri, ThreadCacheEntry{
					Thread:       nil,
					TimeoutCount: 1,
					Expire:       time.Now().Add(bsky.threadCacheTTL),
				})
			}
		}
		return nil, err
	}

	if thread != nil &&
		thread.Thread != nil &&
		thread.Thread.FeedDefs_ThreadViewPost != nil &&
		thread.Thread.FeedDefs_ThreadViewPost.Post != nil &&
		thread.Thread.FeedDefs_ThreadViewPost.Post.Author != nil {

		span.SetAttributes(attribute.Bool("profile_found", true))

		newEntry := ThreadCacheEntry{
			Thread: thread.Thread.FeedDefs_ThreadViewPost,
			Expire: time.Now().Add(bsky.threadCacheTTL),
		}

		// Cache the profile
		bsky.threadCache.Add(uri, newEntry)

		// Update the cache size metric
		cacheSize.WithLabelValues("thread").Add(float64(unsafe.Sizeof(newEntry)))

		return thread.Thread.FeedDefs_ThreadViewPost, nil
	}

	span.SetAttributes(attribute.Bool("thread_found", false))
	return nil, fmt.Errorf("thread not found for: %s", uri)
}
