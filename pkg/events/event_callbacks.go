package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"unsafe"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_hits_total",
	Help: "The total number of cache hits",
}, []string{"cache_type"})

var cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_misses_total",
	Help: "The total number of cache misses",
}, []string{"cache_type"})

var cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsky_cache_size_bytes",
	Help: "The size of the cache in bytes",
}, []string{"cache_type"})

// Initialize Prometheus Metrics for mentions and replies
var mentionCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_mentions_total",
	Help: "The total number of mentions",
})

var replyCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_replies_total",
	Help: "The total number of replies",
})

// Initialize Prometheus Metrics for total number of posts processed
var postsProcessedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_posts_processed_total",
	Help: "The total number of posts processed",
})

// Initialize Prometheus metrics for duration of processing posts
var postProcessingDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsky_post_processing_duration_seconds",
	Help:    "The duration of processing posts",
	Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
})

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

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	Client    *xrpc.Client
	ClientMux sync.Mutex

	IncludeLinks bool

	SocialGraph    graph.Graph
	SocialGraphMux sync.Mutex

	// Generate a Profile Cache with a TTL
	profileCache    map[string]ProfileCacheEntry
	profileCacheTTL time.Duration

	// Generate a Thread Cache with a TTL
	threadCache    map[string]ThreadCacheEntry
	threadCacheTTL time.Duration
}

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(ctx context.Context, includeLinks bool) (*BSky, error) {
	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		return nil, err
	}

	return &BSky{
		Client:    client,
		ClientMux: sync.Mutex{},

		IncludeLinks: includeLinks,

		SocialGraph:    graph.NewGraph(),
		SocialGraphMux: sync.Mutex{},

		profileCache: make(map[string]ProfileCacheEntry),
		threadCache:  make(map[string]ThreadCacheEntry),
		// 60 minute TTL
		profileCacheTTL: time.Minute * 60,
		// 60 minute TTL
		threadCacheTTL: time.Minute * 60,
	}, nil
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context) error {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "RefreshAuthToken")
	defer span.End()
	span.AddEvent("RefreshAuthToken:AcquireClientLock")
	bsky.ClientMux.Lock()
	err := intXRPC.RefreshAuth(ctx, bsky.Client)
	span.AddEvent("RefreshAuthToken:ReleaseClientLock")
	bsky.ClientMux.Unlock()
	return err
}

// ResolveProfile resolves a profile from a DID using the cache or the API
func (bsky *BSky) ResolveProfile(ctx context.Context, did string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveProfile")
	defer span.End()
	// Check the cache first
	if entry, ok := bsky.profileCache[did]; ok {
		if entry.Expire.After(time.Now()) {
			cacheHits.WithLabelValues("profile").Inc()
			span.SetAttributes(attribute.Bool("profile_cache_hit", true))
			return entry.Profile, nil
		}
	}

	span.SetAttributes(attribute.Bool("profile_cache_hit", false))
	cacheMisses.WithLabelValues("profile").Inc()

	//Lock the client
	span.AddEvent("ResolveProfile:AcquireClientLock")
	bsky.ClientMux.Lock()
	// Get the profile from the API
	profile, err := appbsky.ActorGetProfile(ctx, bsky.Client, did)
	// Unlock the client
	span.AddEvent("ResolveProfile:ReleaseClientLock")
	bsky.ClientMux.Unlock()
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
	bsky.profileCache[did] = newEntry

	// Update the cache size metric
	cacheSize.WithLabelValues("profile").Add(float64(unsafe.Sizeof(newEntry)))

	return profile, nil
}

// ResolveThread resolves a thread from a URI using the cache or the API
func (bsky *BSky) ResolveThread(ctx context.Context, uri string) (*bsky.FeedDefs_ThreadViewPost, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "ResolveThread")
	defer span.End()
	// Check the cache first
	if entry, ok := bsky.threadCache[uri]; ok {
		if entry.Expire.After(time.Now()) {
			// If we've timed out 5 times in a row trying to get this thread, it's probably a hellthread
			// Return the cached thread and don't try to get it again
			if entry.TimeoutCount > 5 {
				span.SetAttributes(attribute.Bool("thread_timeout_cached", true))
				return nil, fmt.Errorf("returning cached thread timeout for: %s", uri)
			} else if entry.Thread != nil {
				cacheHits.WithLabelValues("thread").Inc()
				span.SetAttributes(attribute.Bool("thread_cache_hit", true))
				return entry.Thread, nil
			}
		}
	}

	span.SetAttributes(attribute.Bool("thread_cache_hit", false))
	cacheMisses.WithLabelValues("thread").Inc()

	//Lock the client
	span.AddEvent("ResolveThread:AcquireClientLock")
	bsky.ClientMux.Lock()
	// Get the profile from the API
	thread, err := FeedGetPostThreadWithTimeout(ctx, bsky.Client, 1, uri, time.Second*2)
	// Unlock the client
	span.AddEvent("ResolveThread:ReleaseClientLock")
	bsky.ClientMux.Unlock()
	if err != nil {
		// Check if the error is a timeout
		var timeoutErr *TimeoutError
		if errors.As(err, &timeoutErr) {
			span.SetAttributes(attribute.Bool("thread_timeout", true))
			if entry, ok := bsky.threadCache[uri]; ok {
				// If the thread is cached, increment the timeout count
				entry.TimeoutCount++
				entry.Expire = time.Now().Add(bsky.threadCacheTTL)
				bsky.threadCache[uri] = entry
			} else {
				// If the thread isn't cached, cache it with a timeout count of 1
				bsky.threadCache[uri] = ThreadCacheEntry{
					Thread:       nil,
					TimeoutCount: 1,
					Expire:       time.Now().Add(bsky.threadCacheTTL),
				}
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
		bsky.threadCache[uri] = newEntry

		// Update the cache size metric
		cacheSize.WithLabelValues("thread").Add(float64(unsafe.Sizeof(newEntry)))

		return thread.Thread.FeedDefs_ThreadViewPost, nil
	}

	span.SetAttributes(attribute.Bool("thread_found", false))
	return nil, fmt.Errorf("thread not found for: %s", uri)
}

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(ctx context.Context, authorDID string, authorHandle string, facets []*appbsky.RichtextFacet) ([]string, []string, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "DecodeFacets")
	defer span.End()
	span.SetAttributes(attribute.Int("num_facets", len(facets)))

	mentions := []string{}
	links := []string{}
	// Lock the graph
	span.AddEvent("DecodeFacets:AcquireGraphLock")
	bsky.SocialGraphMux.Lock()

	failedLookups := 0

	for _, facet := range facets {
		if facet.Features != nil {
			for _, feature := range facet.Features {
				if feature != nil {
					if feature.RichtextFacet_Link != nil {
						links = append(links, feature.RichtextFacet_Link.Uri)
					} else if feature.RichtextFacet_Mention != nil {
						mentionedUser, err := bsky.ResolveProfile(ctx, feature.RichtextFacet_Mention.Did)
						if err != nil {
							log.Printf("error getting profile for %s: %s", feature.RichtextFacet_Mention.Did, err)
							mentions = append(mentions, fmt.Sprintf("[failed-lookup]@%s", feature.RichtextFacet_Mention.Did))
							failedLookups++
							continue
						}
						mentions = append(mentions, fmt.Sprintf("@%s", mentionedUser.Handle))

						// Track mentions in the social graph
						from := graph.Node{
							DID:    graph.NodeID(authorDID),
							Handle: authorHandle,
						}

						to := graph.Node{
							DID:    graph.NodeID(mentionedUser.Did),
							Handle: mentionedUser.Handle,
						}

						// Increment the edge in the graph
						bsky.SocialGraph.IncrementEdge(from, to, 1)
					}
				}
			}
		}
	}

	span.AddEvent("DecodeFacets:ReleaseGraphLock")
	bsky.SocialGraphMux.Unlock()

	if failedLookups > 0 {
		span.SetAttributes(attribute.Int("failed_lookups", failedLookups))
	}
	span.SetAttributes(attribute.Int("num_mentions", len(mentions)))
	span.SetAttributes(attribute.Int("num_links", len(links)))
	mentionCounter.Add(float64(len(mentions)))

	return mentions, links, nil
}

// HandleRepoCommit is called when a repo commit is received and prints its contents
func (bsky *BSky) HandleRepoCommit(evt *comatproto.SyncSubscribeRepos_Commit) error {
	ctx := context.Background()
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		fmt.Println(err)
	} else {
		for _, op := range evt.Ops {
			ek := repomgr.EventKind(op.Action)
			switch ek {
			case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
				start := time.Now()
				rc, rec, err := rr.GetRecord(ctx, op.Path)
				if err != nil {
					e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
					log.Printf("failed to get a record from the event: %+v\n", e)
					return nil
				}

				if lexutil.LexLink(rc) != *op.Cid {
					e := fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
					log.Printf("failed to LexLink the record in the event: %+v\n", e)
					return nil
				}

				recordAsCAR := lexutil.LexiconTypeDecoder{
					Val: rec,
				}

				var pst = appbsky.FeedPost{}
				b, err := recordAsCAR.MarshalJSON()
				if err != nil {
					log.Printf("failed to marshal record as CAR: %+v\n", err)
					return nil
				}

				err = json.Unmarshal(b, &pst)
				if err != nil {
					log.Printf("failed to unmarshal post into a FeedPost: %+v\n", err)
					return nil
				}

				if pst.LexiconTypeID != "app.bsky.feed.post" {
					return nil
				}

				tracer := otel.Tracer("graph-builder")
				ctx, span := tracer.Start(ctx, "HandleRepoCommit")
				defer span.End()

				span.AddEvent("HandleRepoCommit:ResolveProfile")
				authorProfile, err := bsky.ResolveProfile(ctx, evt.Repo)
				if err != nil {
					log.Printf("error getting profile for %s: %+v\n", evt.Repo, err)
					return nil
				}

				span.SetAttributes(attribute.String("author_did", authorProfile.Did))
				span.SetAttributes(attribute.String("author_handle", authorProfile.Handle))

				span.AddEvent("HandleRepoCommit:DecodeFacets")
				mentions, links, err := bsky.DecodeFacets(ctx, authorProfile.Did, authorProfile.Handle, pst.Facets)
				if err != nil {
					log.Printf("error decoding post facets: %+v\n", err)
				}

				// Parse time from the event time string
				t, err := time.Parse(time.RFC3339, evt.Time)
				if err != nil {
					log.Printf("error parsing time: %+v\n", err)
					return nil
				}

				postBody := strings.ReplaceAll(pst.Text, "\n", "\n\t")

				replyingTo := ""
				replyingToDID := ""
				if pst.Reply != nil && pst.Reply.Parent != nil {
					thread, err := bsky.ResolveThread(ctx, pst.Reply.Parent.Uri)
					if err != nil {
						log.Printf("error resolving thread (%s): %+v\n", pst.Reply.Parent.Uri, err)
						return nil
					} else if thread == nil {
						log.Printf("thread (%s) not found\n", pst.Reply.Parent.Uri)
						return nil
					}

					replyingTo = thread.Post.Author.Handle
					replyingToDID = thread.Post.Author.Did
					span.SetAttributes(attribute.String("replying_to", replyingTo))
					span.SetAttributes(attribute.String("replying_to_did", replyingToDID))
				}

				// Track replies in the social graph
				if replyingTo != "" && replyingToDID != "" {
					from := graph.Node{
						DID:    graph.NodeID(authorProfile.Did),
						Handle: authorProfile.Handle,
					}

					to := graph.Node{
						DID:    graph.NodeID(replyingToDID),
						Handle: replyingTo,
					}
					span.AddEvent("HandleRepoCommit:AcquireGraphLock")
					bsky.SocialGraphMux.Lock()
					bsky.SocialGraph.IncrementEdge(from, to, 1)
					span.AddEvent("HandleRepoCommit:ReleaseGraphLock")
					bsky.SocialGraphMux.Unlock()

					// Increment the reply count metric
					replyCounter.Inc()
				}

				// Grab Post ID from the Path
				pathParts := strings.Split(op.Path, "/")
				postID := pathParts[len(pathParts)-1]

				postLink := fmt.Sprintf("https://staging.bsky.app/profile/%s/post/%s", authorProfile.Handle, postID)

				logMsg := ""

				// Print the content of the post and any mentions or links

				// Add a Timestamp with a post link in it if we want one
				if bsky.IncludeLinks {
					logMsg += fmt.Sprintf("\u001b[90m[\x1b]8;;%s\x07%s\x1b]8;;\x07]\u001b[0m", postLink, t.Local().Format("02.01.06 15:04:05"))
				} else {
					logMsg += fmt.Sprintf("\u001b[90m%s\u001b[0m", t.Local().Format("02.01.06 15:04:05"))
				}

				// Add the user and who they are replying to if they are
				logMsg += fmt.Sprintf(" %s", authorProfile.Handle)
				if replyingTo != "" {
					logMsg += fmt.Sprintf(" \u001b[90m->\u001b[0m %s", replyingTo)
				}

				// Add the Post Body
				logMsg += fmt.Sprintf(": \n\t%s\n", postBody)

				// Add any Mentions or Links
				if len(mentions) > 0 {
					logMsg += fmt.Sprintf("\tMentions: %s\n", mentions)
				}
				if len(links) > 0 {
					logMsg += fmt.Sprintf("\tLinks: %s\n", links)
				}

				// Print the log message
				fmt.Printf("%s", logMsg)

				// Record the time to process and the count
				postsProcessedCounter.Inc()
				postProcessingDurationHistogram.Observe(time.Since(start).Seconds())
			}
		}
	}
	return nil
}

func HandleRepoInfo(info *comatproto.SyncSubscribeRepos_Info) error {
	return nil
}

func HandleError(errf *events.ErrorFrame) error {
	return nil
}
