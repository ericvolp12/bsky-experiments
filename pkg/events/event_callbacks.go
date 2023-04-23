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
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
)

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	Client    *xrpc.Client
	ClientMux sync.Mutex

	MentionCounters    map[string]int
	MentionCountersMux sync.Mutex

	ReplyCounters    map[string]int
	ReplyCountersMux sync.Mutex

	SocialGraph    graph.Graph
	SocialGraphMux sync.Mutex
}

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(ctx context.Context) (*BSky, error) {
	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		return nil, err
	}

	return &BSky{
		Client:    client,
		ClientMux: sync.Mutex{},

		MentionCounters:    make(map[string]int),
		MentionCountersMux: sync.Mutex{},

		ReplyCounters:    make(map[string]int),
		ReplyCountersMux: sync.Mutex{},

		SocialGraph:    graph.NewGraph(),
		SocialGraphMux: sync.Mutex{},
	}, nil
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context) error {
	bsky.ClientMux.Lock()
	defer bsky.ClientMux.Unlock()
	return intXRPC.RefreshAuth(ctx, bsky.Client)
}

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(ctx context.Context, facets []*appbsky.RichtextFacet) ([]string, []string, error) {
	mentions := []string{}
	links := []string{}
	// Lock the mentions counter
	bsky.MentionCountersMux.Lock()
	defer bsky.MentionCountersMux.Unlock()

	for _, facet := range facets {
		if facet.Features != nil {
			for _, feature := range facet.Features {
				if feature != nil {
					if feature.RichtextFacet_Link != nil {
						links = append(links, feature.RichtextFacet_Link.Uri)
					} else if feature.RichtextFacet_Mention != nil {
						mentionedUser, err := appbsky.ActorGetProfile(ctx, bsky.Client, feature.RichtextFacet_Mention.Did)
						if err != nil {
							log.Printf("error getting profile for %s: %s", feature.RichtextFacet_Mention.Did, err)
							mentions = append(mentions, fmt.Sprintf("[failed-lookup]@%s", feature.RichtextFacet_Mention.Did))
							continue
						}
						mentions = append(mentions, fmt.Sprintf("@%s", mentionedUser.Handle))

						// Track mention counts
						bsky.MentionCounters[mentionedUser.Handle]++
					}
				}
			}
		}
	}
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

				postAsCAR := lexutil.LexiconTypeDecoder{
					Val: rec,
				}

				var pst = appbsky.FeedPost{}
				b, err := postAsCAR.MarshalJSON()
				if err != nil {
					log.Printf("failed to marshal post as CAR: %+v\n", err)
					return nil
				}

				err = json.Unmarshal(b, &pst)
				if err != nil {
					log.Printf("failed to unmarshal post into a FeedPost: %+v\n", err)
					return nil
				}

				// Lock the client
				bsky.ClientMux.Lock()

				authorProfile, err := appbsky.ActorGetProfile(ctx, bsky.Client, evt.Repo)
				if err != nil {
					log.Printf("error getting profile for %s: %+v\n", evt.Repo, err)
					bsky.ClientMux.Unlock()
					return nil
				}

				mentions, links, err := bsky.DecodeFacets(ctx, pst.Facets)
				if err != nil {
					log.Printf("error decoding post facets: %+v\n", err)
				}

				// Parse time from the event time string
				t, err := time.Parse(time.RFC3339, evt.Time)
				if err != nil {
					log.Printf("error parsing time: %+v\n", err)
					bsky.ClientMux.Unlock()
					return nil
				}

				postBody := strings.ReplaceAll(pst.Text, "\n", "\n\t")

				replyingTo := ""
				if pst.Reply != nil && pst.Reply.Parent != nil {
					thread, err := appbsky.FeedGetPostThread(ctx, bsky.Client, 2, pst.Reply.Parent.Uri)
					if err != nil {
						log.Printf("error getting thread for %s: %s\n", pst.Reply.Parent.Cid, err)
					} else {
						if thread != nil &&
							thread.Thread != nil &&
							thread.Thread.FeedDefs_ThreadViewPost != nil &&
							thread.Thread.FeedDefs_ThreadViewPost.Post != nil &&
							thread.Thread.FeedDefs_ThreadViewPost.Post.Author != nil {
							replyingTo = thread.Thread.FeedDefs_ThreadViewPost.Post.Author.Handle
						}
					}
				}

				bsky.ClientMux.Unlock()

				// Track reply counts
				if replyingTo != "" {
					// Add to the social graph
					// Lock the Social Graph and Reply Counters
					bsky.ReplyCountersMux.Lock()
					bsky.SocialGraphMux.Lock()

					bsky.SocialGraph.IncrementEdge(graph.NodeID(authorProfile.Handle), graph.NodeID(replyingTo), 1)
					bsky.ReplyCounters[replyingTo]++

					bsky.ReplyCountersMux.Unlock()
					bsky.SocialGraphMux.Unlock()
				}

				// Grab Post ID from the Path
				pathParts := strings.Split(op.Path, "/")
				postID := pathParts[len(pathParts)-1]

				postLink := fmt.Sprintf("https://staging.bsky.app/profile/%s/post/%s", authorProfile.Handle, postID)

				// Print the content of the post and any mentions or links
				if pst.LexiconTypeID == "app.bsky.feed.post" {
					// Print a Timestamp
					fmt.Printf("\u001b[90m[\x1b]8;;%s\x07%s\x1b]8;;\x07]\u001b[0m", postLink, t.Local().Format("02.01.06 15:04:05"))

					// Print the user and who they are replying to if they are
					fmt.Printf(" %s", authorProfile.Handle)
					if replyingTo != "" {
						fmt.Printf(" \u001b[90m->\u001b[0m %s", replyingTo)
					}

					// Print the Post Body
					fmt.Printf(": \n\t%s\n", postBody)

					// Print any Mentions or Links
					if len(mentions) > 0 {
						fmt.Printf("\tMentions: %s\n", mentions)
					}
					if len(links) > 0 {
						fmt.Printf("\tLinks: %s\n", links)
					}
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
