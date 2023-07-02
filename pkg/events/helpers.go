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

var TimeoutError = fmt.Errorf("timeout")

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

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		start := time.Now()
		posts, err := appbsky.FeedGetPosts(ctx, client, uris)
		elapsed := time.Since(start)
		apiCallDurationHistogram.WithLabelValues("FeedGetPosts").Observe(elapsed.Seconds())
		resultChan <- result{posts: posts, err: err}
	}()

	select {
	case res := <-resultChan:
		cancel()
		return res.posts, res.err
	case <-time.After(timeout):
		cancel()
		return nil, TimeoutError
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
	span.SetAttributes(attribute.Int("facets.count", len(facets)))

	mentions := []string{}
	links := []string{}

	failedLookups := 0

	for _, facet := range facets {
		if facet.Features != nil {
			for _, feature := range facet.Features {
				if feature != nil {
					if feature.RichtextFacet_Link != nil {
						links = append(links, feature.RichtextFacet_Link.Uri)
					} else if feature.RichtextFacet_Mention != nil {
						mentionedHandle, err := bsky.ResolveDID(ctx, feature.RichtextFacet_Mention.Did)
						if err != nil {
							log.Printf("error getting handle for %s: %s", feature.RichtextFacet_Mention.Did, err)
							mentions = append(mentions, fmt.Sprintf("[failed-lookup]@%s", feature.RichtextFacet_Mention.Did))
							failedLookups++
							continue
						}
						mentions = append(mentions, fmt.Sprintf("@%s", mentionedHandle))

						// Track mentions in the social graph
						from := graph.Node{
							DID:    graph.NodeID(authorDID),
							Handle: authorHandle,
						}

						to := graph.Node{
							DID:    graph.NodeID(feature.RichtextFacet_Mention.Did),
							Handle: mentionedHandle,
						}

						// Increment the edge in the graph
						bsky.PersistedGraph.IncrementEdge(ctx, from, to, 1)
					}
				}
			}
		}
	}

	if failedLookups > 0 {
		span.SetAttributes(attribute.Int("lookups.failed", failedLookups))
	}
	span.SetAttributes(attribute.Int("mentions.count", len(mentions)))
	span.SetAttributes(attribute.Int("links.count", len(links)))
	mentionCounter.Add(float64(len(mentions)))

	return mentions, links, nil
}
