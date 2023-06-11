// Package feedgenerator describes the FeedGenerator type, which is responsible for generating feeds for a given DID.
// It also describes the Feed interface, which is implemented by the various feed types.
package feedgenerator

import (
	"context"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	did "github.com/whyrusleeping/go-did"
)

type Feed interface {
	GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) (feedPosts []*appbsky.FeedDefs_SkeletonFeedPost, newCursor *string, err error)
	Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error)
}

type FeedGenerator struct {
	FeedActorDID          did.DID         // DID of the Repo the Feed is published under
	ServiceEndpoint       string          // URL of the FeedGenerator service
	ServiceDID            did.DID         // DID of the FeedGenerator service
	DIDDocument           did.Document    // DID Document of the FeedGenerator service
	AcceptableURIPrefixes []string        // URIs that the FeedGenerator is allowed to generate feeds for
	FeedMap               map[string]Feed // map of FeedName to Feed
	Feeds                 []Feed
}

type NotFoundError struct {
	error
}

// NewFeedGenerator returns a new FeedGenerator
func NewFeedGenerator(
	ctx context.Context,
	feedActorDIDString string,
	serviceDIDString string,
	acceptableDIDs []string,
	serviceEndpoint string,
) (*FeedGenerator, error) {
	acceptableURIPrefixes := []string{}
	for _, did := range acceptableDIDs {
		acceptableURIPrefixes = append(acceptableURIPrefixes, "at://"+did+"/app.bsky.feed.generator/")
	}

	serviceDID, err := did.ParseDID(serviceDIDString)
	if err != nil {
		return nil, fmt.Errorf("error parsing serviceDID: %w", err)
	}

	feedActorDID, err := did.ParseDID(feedActorDIDString)
	if err != nil {
		return nil, fmt.Errorf("error parsing feedActorDID: %w", err)
	}

	serviceID, err := did.ParseDID("#bsky_fg")
	if err != nil {
		panic(err)
	}

	doc := did.Document{
		Context: []string{did.CtxDIDv1},
		ID:      serviceDID,
		Service: []did.Service{
			{
				ID:              serviceID,
				Type:            "BskyFeedGenerator",
				ServiceEndpoint: serviceEndpoint,
			},
		},
	}

	return &FeedGenerator{
		FeedMap:               map[string]Feed{},
		FeedActorDID:          feedActorDID,
		ServiceDID:            serviceDID,
		DIDDocument:           doc,
		AcceptableURIPrefixes: acceptableURIPrefixes,
		ServiceEndpoint:       serviceEndpoint,
	}, nil
}

// AddFeed adds a feed to the FeedGenerator
// Feed precedence for overlapping aliases is determined by the order in which
// they are added (first added is highest precedence)
func (fg *FeedGenerator) AddFeed(feedAliases []string, feed Feed) {
	if fg.FeedMap == nil {
		fg.FeedMap = map[string]Feed{}
	}

	for _, feedAlias := range feedAliases {
		// Skip the feed if we already have the alias registered so we don't add it twice
		// Feed precedence is determined by the order in which they are added
		if _, ok := fg.FeedMap[feedAlias]; ok {
			continue
		}

		fg.FeedMap[feedAlias] = feed
	}

	fg.Feeds = append(fg.Feeds, feed)
}
