package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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
)

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_profile_cache_hits_total",
	Help: "The total number of profile cache hits",
})

var cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_profile_cache_misses_total",
	Help: "The total number of profile cache misses",
})

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
	Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
})

// ProfileCacheEntry is a struct that holds a profile and an expiration time
type ProfileCacheEntry struct {
	Profile *bsky.ActorDefs_ProfileViewDetailed
	Expire  time.Time
}

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	Client    *xrpc.Client
	ClientMux sync.Mutex

	IncludeLinks bool

	MentionCounterMap    map[string]int
	MentionCounterMapMux sync.Mutex

	ReplyCounterMap    map[string]int
	ReplyCounterMapMux sync.Mutex

	SocialGraph    graph.Graph
	SocialGraphMux sync.Mutex

	// Generate a Profile Cache with a TTL
	profileCache    map[string]ProfileCacheEntry
	profileCacheTTL time.Duration
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

		MentionCounterMap:    make(map[string]int),
		MentionCounterMapMux: sync.Mutex{},

		ReplyCounterMap:    make(map[string]int),
		ReplyCounterMapMux: sync.Mutex{},

		SocialGraph:    graph.NewGraph(),
		SocialGraphMux: sync.Mutex{},

		profileCache: make(map[string]ProfileCacheEntry),
		// 60 minute TTL
		profileCacheTTL: time.Minute * 60,
	}, nil
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context) error {
	bsky.ClientMux.Lock()
	defer bsky.ClientMux.Unlock()
	return intXRPC.RefreshAuth(ctx, bsky.Client)
}

// ResolveProfile resolves a profile from a DID using the cache or the API
func (bsky *BSky) ResolveProfile(ctx context.Context, did string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	// Check the cache first
	if entry, ok := bsky.profileCache[did]; ok {
		if entry.Expire.After(time.Now()) {
			cacheHits.Inc()
			return entry.Profile, nil
		}
	}

	cacheMisses.Inc()

	//Lock the client
	bsky.ClientMux.Lock()
	// Get the profile from the API
	profile, err := appbsky.ActorGetProfile(ctx, bsky.Client, did)
	// Unlock the client
	bsky.ClientMux.Unlock()
	if err != nil {

		return nil, err
	}
	// Unlock the client

	if profile == nil {
		return nil, fmt.Errorf("profile not found for: %s", did)
	}

	// Cache the profile
	bsky.profileCache[did] = ProfileCacheEntry{
		Profile: profile,
		Expire:  time.Now().Add(bsky.profileCacheTTL),
	}

	return profile, nil
}

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(ctx context.Context, authorDID string, authorHandle string, facets []*appbsky.RichtextFacet) ([]string, []string, error) {
	mentions := []string{}
	links := []string{}
	// Lock the mentions counter
	bsky.MentionCounterMapMux.Lock()
	defer bsky.MentionCounterMapMux.Unlock()
	bsky.SocialGraphMux.Lock()
	defer bsky.SocialGraphMux.Unlock()

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

						bsky.SocialGraph.IncrementEdge(from, to, 1)

						// Track mention counts
						bsky.MentionCounterMap[mentionedUser.Handle]++
					}
				}
			}
		}
	}

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

				authorProfile, err := bsky.ResolveProfile(ctx, evt.Repo)
				if err != nil {
					log.Printf("error getting profile for %s: %+v\n", evt.Repo, err)
					return nil
				}

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
					// Lock the client
					bsky.ClientMux.Lock()
					thread, err := appbsky.FeedGetPostThread(ctx, bsky.Client, 2, pst.Reply.Parent.Uri)
					bsky.ClientMux.Unlock()
					if err != nil {
						log.Printf("error getting thread for %s: %s\n", pst.Reply.Parent.Cid, err)
					} else {
						if thread != nil &&
							thread.Thread != nil &&
							thread.Thread.FeedDefs_ThreadViewPost != nil &&
							thread.Thread.FeedDefs_ThreadViewPost.Post != nil &&
							thread.Thread.FeedDefs_ThreadViewPost.Post.Author != nil {
							replyingTo = thread.Thread.FeedDefs_ThreadViewPost.Post.Author.Handle
							replyingToDID = thread.Thread.FeedDefs_ThreadViewPost.Post.Author.Did
						}
					}
				}

				// Track reply counts
				if replyingTo != "" && replyingToDID != "" {
					// Add to the social graph
					// Lock the Social Graph and Reply Counters
					bsky.ReplyCounterMapMux.Lock()
					bsky.SocialGraphMux.Lock()
					from := graph.Node{
						DID:    graph.NodeID(authorProfile.Did),
						Handle: authorProfile.Handle,
					}

					to := graph.Node{
						DID:    graph.NodeID(replyingToDID),
						Handle: replyingTo,
					}

					bsky.SocialGraph.IncrementEdge(from, to, 1)
					bsky.ReplyCounterMap[replyingTo]++

					bsky.ReplyCounterMapMux.Unlock()
					bsky.SocialGraphMux.Unlock()

					// Increment the reply count
					replyCounter.Inc()
				}

				// Grab Post ID from the Path
				pathParts := strings.Split(op.Path, "/")
				postID := pathParts[len(pathParts)-1]

				postLink := fmt.Sprintf("https://staging.bsky.app/profile/%s/post/%s", authorProfile.Handle, postID)

				logMsg := ""

				// Print the content of the post and any mentions or links
				if pst.LexiconTypeID == "app.bsky.feed.post" {
					// Print a Timestamp
					if bsky.IncludeLinks {
						logMsg += fmt.Sprintf("\u001b[90m[\x1b]8;;%s\x07%s\x1b]8;;\x07]\u001b[0m", postLink, t.Local().Format("02.01.06 15:04:05"))
					} else {
						logMsg += fmt.Sprintf("\u001b[90m%s\u001b[0m", t.Local().Format("02.01.06 15:04:05"))
					}

					// Print the user and who they are replying to if they are
					logMsg += fmt.Sprintf(" %s", authorProfile.Handle)
					if replyingTo != "" {
						logMsg += fmt.Sprintf(" \u001b[90m->\u001b[0m %s", replyingTo)
					}

					// Print the Post Body
					logMsg += fmt.Sprintf(": \n\t%s\n", postBody)

					// Print any Mentions or Links
					if len(mentions) > 0 {
						logMsg += fmt.Sprintf("\tMentions: %s\n", mentions)
					}
					if len(links) > 0 {
						logMsg += fmt.Sprintf("\tLinks: %s\n", links)
					}

					fmt.Printf("%s", logMsg)

					// Record the time to process and the count
					postsProcessedCounter.Inc()
					postProcessingDurationHistogram.Observe(time.Since(start).Seconds())
				}
			case repomgr.EvtKindDeleteRecord:
				// if err := cb(ek, evt.Seq, op.Path, evt.Repo, nil, nil); err != nil {
				// 	return err
				// }
			}
		}

	}

	return nil
}

func HandleRepoInfo(info *comatproto.SyncSubscribeRepos_Info) error {

	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	fmt.Println(string(b))

	return nil
}

func HandleError(errf *events.ErrorFrame) error {
	return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
}
