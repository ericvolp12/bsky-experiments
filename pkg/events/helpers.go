package events

import (
	"context"
	"fmt"
	"log"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type TimeoutError struct {
	error
}

func FeedGetPostsWithTimeout(
	ctx context.Context,
	client *xrpc.Client,
	uris []string,
	timeout time.Duration,
) (*appbsky.FeedGetPosts_Output, error) {
	type result struct {
		posts *appbsky.FeedGetPosts_Output
		err   error
	}

	resultChan := make(chan result, 1)

	go func() {
		start := time.Now()
		posts, err := appbsky.FeedGetPosts(ctx, client, uris)
		elapsed := time.Since(start)
		apiCallDurationHistogram.WithLabelValues("FeedGetPosts").Observe(elapsed.Seconds())
		resultChan <- result{posts: posts, err: err}
	}()

	select {
	case res := <-resultChan:
		return res.posts, res.err
	case <-time.After(timeout):
		return nil, &TimeoutError{fmt.Errorf("FeedGetPosts timed out after %v", timeout)}
	}
}

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(
	ctx context.Context,
	authorDID string,
	authorHandle string,
	facets []*appbsky.RichtextFacet,
	workerID int,
) ([]string, []string, error) {
	tracer := otel.Tracer("graph-builder")
	ctx, span := tracer.Start(ctx, "DecodeFacets")
	defer span.End()
	span.SetAttributes(attribute.Int("num_facets", len(facets)))

	mentions := []string{}
	links := []string{}
	// Lock the graph
	span.AddEvent("DecodeFacets:AcquireGraphLock")
	bsky.SocialGraphMux.Lock()
	span.AddEvent("DecodeFacets:GraphLockAcquired")

	failedLookups := 0

	for _, facet := range facets {
		if facet.Features != nil {
			for _, feature := range facet.Features {
				if feature != nil {
					if feature.RichtextFacet_Link != nil {
						links = append(links, feature.RichtextFacet_Link.Uri)
					} else if feature.RichtextFacet_Mention != nil {
						mentionedUser, err := bsky.ResolveProfile(ctx, feature.RichtextFacet_Mention.Did, workerID)
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
