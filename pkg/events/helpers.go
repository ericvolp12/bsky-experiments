package events

import (
	"context"
	"fmt"
	"log"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// DecodeFacets decodes the facets of a richtext record into mentions and links
func (bsky *BSky) DecodeFacets(
	ctx context.Context,
	authorDID string,
	authorHandle string,
	facets []*appbsky.RichtextFacet,
	workerID int,
) ([]string, []string, error) {
	ctx, span := otel.Tracer("graph-builder").Start(ctx, "DecodeFacets")
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
