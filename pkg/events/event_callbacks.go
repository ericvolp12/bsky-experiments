package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
)

type BSky struct {
	Client          *xrpc.Client
	MentionCounters map[string]int
}

func NewBSky() (*BSky, error) {
	client, err := intXRPC.GetXRPCClient()
	if err != nil {
		return nil, err
	}

	return &BSky{
		Client:          client,
		MentionCounters: make(map[string]int),
	}, nil
}

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(ctx context.Context, facets []*appbsky.RichtextFacet) ([]string, []string, error) {
	mentions := []string{}
	links := []string{}
	for _, facet := range facets {
		if facet.Features != nil {
			for _, feature := range facet.Features {
				if feature != nil {
					if feature.RichtextFacet_Link != nil {
						links = append(links, feature.RichtextFacet_Link.Uri)
					} else if feature.RichtextFacet_Mention != nil {
						mentionedUser, err := appbsky.ActorGetProfile(ctx, bsky.Client, feature.RichtextFacet_Mention.Did)
						if err != nil {
							fmt.Printf("error getting profile for %s: %s", feature.RichtextFacet_Mention.Did, err)
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
					fmt.Printf("%e", e)
					return nil
				}

				if lexutil.LexLink(rc) != *op.Cid {
					return fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
				}

				postAsCAR := lexutil.LexiconTypeDecoder{
					Val: rec,
				}

				var pst = appbsky.FeedPost{}
				b, err := postAsCAR.MarshalJSON()
				if err != nil {
					fmt.Println(err)
				}

				err = json.Unmarshal(b, &pst)
				if err != nil {
					fmt.Println(err)
				}

				authorProfile, err := appbsky.ActorGetProfile(ctx, bsky.Client, evt.Repo)
				if err != nil {
					fmt.Printf("error getting profile for %s: %s", evt.Repo, err)
					return err
				}

				mentions, links, err := bsky.DecodeFacets(ctx, pst.Facets)
				if err != nil {
					fmt.Printf("error decoding post facets: %+e", err)
				}

				// Parse time from the event time string
				t, err := time.Parse(time.RFC3339, evt.Time)
				if err != nil {
					fmt.Printf("error parsing time: %s", err)
				}

				postBody := strings.ReplaceAll(pst.Text, "\n", "\n\t")

				// Print the content of the post and any mentions or links
				if pst.LexiconTypeID == "app.bsky.feed.post" {
					fmt.Printf("\u001b[90m[%s]\u001b[0m %s: \n\t%s\n", t.Local().Format("02.01.06 15:04:05"), authorProfile.Handle, postBody)
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
