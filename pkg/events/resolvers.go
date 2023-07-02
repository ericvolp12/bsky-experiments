package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// PostCacheEntry is a struct that holds a PostView and an expiration time
type PostCacheEntry struct {
	Post         *bsky.FeedDefs_PostView
	TimeoutCount int
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context, workerID int) error {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "RefreshAuthToken")
	defer span.End()
	err := intXRPC.RefreshAuth(ctx, worker.Client, &worker.ClientMux)
	return err
}

// ResolveProfile resolves a profile from a DID using the cache or the API
func (bsky *BSky) ResolveProfile(ctx context.Context, did string, workerID int) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveProfile")
	defer span.End()

	profileCacheKey := bsky.cachesPrefix + ":profile:" + did
	didCacheKey := bsky.cachesPrefix + ":did:" + did

	// Check the cache first
	profString, err := bsky.redisClient.Get(ctx, profileCacheKey).Result()
	if err == nil {
		// If the profile is in the cache, return it
		profile := &appbsky.ActorDefs_ProfileViewDetailed{}
		err := json.Unmarshal([]byte(profString), profile)
		if err == nil {
			span.SetAttributes(attribute.Bool("caches.profiles.hit", true))
			cacheHits.WithLabelValues("profile").Inc()
			return profile, nil
		}

		// If there was an error scanning the profile, add attributes to the span
		span.SetAttributes(attribute.String("caches.profiles.scan.error", err.Error()))
	} else if err != redis.Nil {
		span.SetAttributes(attribute.String("caches.profiles.get.error", err.Error()))
	}

	span.SetAttributes(attribute.Bool("caches.profiles.hit", false))
	cacheMisses.WithLabelValues("profile").Inc()

	// Wait on the rate limiter
	span.AddEvent("WaitOnRateLimiter")
	err = bsky.bskyLimiter.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	//Lock the client
	span.AddEvent("AcquireClientRLock")
	worker.ClientMux.RLock()
	span.AddEvent("ClientRLockAcquired")
	// Get the profile from the API
	start := time.Now()
	profile, err := appbsky.ActorGetProfile(ctx, worker.Client, did)
	elapsed := time.Since(start)
	apiCallDurationHistogram.WithLabelValues("ActorGetProfile").Observe(elapsed.Seconds())
	// Unlock the client
	span.AddEvent("ReleaseClientRLock")
	worker.ClientMux.RUnlock()
	if err != nil {
		return nil, err
	}

	if profile == nil {
		span.SetAttributes(attribute.Bool("profile.found", false))
		return nil, fmt.Errorf("profile not found for: %s", did)
	}

	span.SetAttributes(attribute.Bool("profile.found", true))

	// Marshal the profile as JSON
	profAsJSON, err := json.Marshal(profile)
	if err != nil {
		span.SetAttributes(attribute.String("profile.marshal.error", err.Error()))
		return profile, nil
	}

	// Cache the profile
	setCmd := bsky.redisClient.Set(ctx, profileCacheKey, profAsJSON, bsky.profileCacheTTL)
	if setCmd.Err() != nil {
		span.SetAttributes(attribute.String("caches.profiles.set.error", setCmd.Err().Error()))
	}

	// Any time we resolve a profile, we also resolve the handle for free, so we might as well cache it
	setCmd = bsky.redisClient.Set(ctx, didCacheKey, profile.Handle, bsky.profileCacheTTL)
	if setCmd.Err() != nil {
		span.SetAttributes(attribute.String("caches.handles.set.error", setCmd.Err().Error()))
	}

	return profile, nil
}

type didLookup struct {
	Did                 string `json:"did"`
	VerificationMethods struct {
		Atproto string `json:"atproto"`
	} `json:"verificationMethods"`
	RotationKeys []string `json:"rotationKeys"`
	AlsoKnownAs  []string `json:"alsoKnownAs"`
	Services     struct {
		AtprotoPds struct {
			Type     string `json:"type"`
			Endpoint string `json:"endpoint"`
		} `json:"atproto_pds"`
	} `json:"services"`
}

func (bsky *BSky) getHandleFromDirectory(ctx context.Context, did string) (handle string, err error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "getHandleFromDirectory")
	defer span.End()

	start := time.Now()

	// Use rate limiter before each request
	err = bsky.directoryLimiter.Wait(ctx)
	if err != nil {
		span.SetAttributes(attribute.String("rate.limiter.error", err.Error()))
		return handle, fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	// HTTP GET to https://plc.directory/{did}/data
	req, err := http.NewRequest("GET", fmt.Sprintf("https://plc.directory/%s/data", did), nil)
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return handle, fmt.Errorf("error creating request for %s: %w", did, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return handle, fmt.Errorf("error getting handle for %s: %w", did, err)
	}
	defer resp.Body.Close()

	// Read the response body into a didLookup
	didLookup := didLookup{}
	err = json.NewDecoder(resp.Body).Decode(&didLookup)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return handle, fmt.Errorf("error decoding response body for %s: %w", did, err)
	}

	// Record the duration of the request
	apiCallDurationHistogram.WithLabelValues("LookupDID").Observe(time.Since(start).Seconds())

	// If the didLookup has a handle, return it
	if len(didLookup.AlsoKnownAs) > 0 {
		// Handles from the DID service look like: "at://jaz.bsky.social", remove the "at://" prefix
		handle = strings.TrimPrefix(didLookup.AlsoKnownAs[0], "at://")
		span.SetAttributes(attribute.String("handle", handle))
	}

	return handle, nil
}

func (bsky *BSky) ResolveDID(ctx context.Context, did string) (handle string, err error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveDID")
	defer span.End()

	cacheKey := bsky.cachesPrefix + ":did:" + did

	// Check the cache first
	handleFromCache, err := bsky.redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		span.SetAttributes(attribute.Bool("caches.did.hit", true))
		cacheHits.WithLabelValues("did").Inc()
		return handleFromCache, nil
	} else if err != redis.Nil {
		span.SetAttributes(attribute.String("caches.did.get.error", err.Error()))
	}

	span.SetAttributes(attribute.Bool("caches.did.hit", false))
	cacheMisses.WithLabelValues("did").Inc()

	// Get the handle from the DID service
	handle, err = bsky.getHandleFromDirectory(ctx, did)
	if err != nil {
		span.SetAttributes(attribute.String("did.get.error", err.Error()))
		return handle, fmt.Errorf("error getting handle for %s: %w", did, err)
	}

	setCmd := bsky.redisClient.Set(ctx, cacheKey, handle, bsky.profileCacheTTL)
	if setCmd.Err() != nil {
		span.SetAttributes(attribute.String("caches.did.set.error", setCmd.Err().Error()))
	}

	return handle, nil
}

// ResolvePost resolves a post from a URI using the cache or the API
func (bsky *BSky) ResolvePost(ctx context.Context, uri string, workerID int) (*bsky.FeedDefs_PostView, error) {
	worker := bsky.Workers[workerID]
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolvePost")
	defer span.End()
	// Check the cache first
	var cacheEntry *PostCacheEntry
	postString, err := bsky.redisClient.Get(ctx, bsky.cachesPrefix+":post:"+uri).Result()
	if err == nil {
		// If the post is in the cache, return it
		cacheEntry = &PostCacheEntry{}
		err := json.Unmarshal([]byte(postString), cacheEntry)
		if err == nil {
			if cacheEntry.TimeoutCount > 5 {
				span.SetAttributes(attribute.Bool("caches.posts.timeout_present", true))
				return nil, fmt.Errorf("hellthread detected - returning cached post timeout for: %s", uri)
			} else if cacheEntry.Post != nil {
				span.SetAttributes(attribute.Bool("caches.post.hit", true))
				cacheHits.WithLabelValues("post").Inc()
				return cacheEntry.Post, nil
			}
		} else {
			// If there was an error scanning the post, add attributes to the span
			span.SetAttributes(attribute.String("caches.post.scan.error", err.Error()))
		}
	} else if err != redis.Nil {
		span.SetAttributes(attribute.String("caches.post.get.error", err.Error()))
	}

	span.SetAttributes(attribute.Bool("caches.post.hit", false))
	cacheMisses.WithLabelValues("post").Inc()

	span.AddEvent("WaitOnRateLimiter")
	// Wait on the rate limiter
	err = bsky.bskyLimiter.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	//Lock the client
	span.AddEvent("AcquireClientRLock")
	worker.ClientMux.RLock()
	span.AddEvent("ClientRLockAcquired")
	// Get the post from the API
	posts, err := FeedGetPostsWithTimeout(ctx, worker.Client, []string{uri}, time.Second*5)
	// Unlock the client
	span.AddEvent("ReleaseClientRLock")
	worker.ClientMux.RUnlock()
	if err != nil {
		// Check if the error is a timeout
		if errors.Is(err, TimeoutError) {
			span.SetAttributes(attribute.Bool("post.resolve.timeout", true))
			if cacheEntry != nil {
				// If the post is cached, increment the timeout count
				cacheEntry.TimeoutCount++
			} else {
				cacheEntry = &PostCacheEntry{
					Post:         nil,
					TimeoutCount: 1,
				}
			}
			// Marshal the cacheEntry as JSON
			entryJSON, err := json.Marshal(cacheEntry)
			if err != nil {
				span.SetAttributes(attribute.String("post.marshal.error", err.Error()))
				return nil, err
			}

			// Cache the profile
			bsky.redisClient.Set(ctx, bsky.cachesPrefix+":post:"+uri, entryJSON, bsky.postCacheTTL)
		}
		return nil, err
	}

	if posts != nil &&
		posts.Posts != nil &&
		len(posts.Posts) > 0 &&
		posts.Posts[0].Author != nil {

		span.SetAttributes(attribute.Bool("post.found", true))

		cacheEntry = &PostCacheEntry{
			Post: posts.Posts[0],
		}

		// Marshal the cacheEntry as JSON
		entryJSON, err := json.Marshal(cacheEntry)
		if err != nil {
			span.SetAttributes(attribute.String("post.marshal.error", err.Error()))
			return posts.Posts[0], nil
		}

		// Cache the profile
		bsky.redisClient.Set(ctx, bsky.cachesPrefix+":post:"+uri, entryJSON, bsky.postCacheTTL)

		return posts.Posts[0], nil
	}

	span.SetAttributes(attribute.Bool("post.found", false))
	return nil, fmt.Errorf("post not found for: %s", uri)
}
