package jazbot

import (
	"context"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
)

func (j *Jazbot) SearchMyPosts(ctx context.Context, actorDid string, searchText string) (string, []*appbsky.RichtextFacet, error) {
	ctx, span := tracer.Start(ctx, "SearchMyPosts")
	defer span.End()

	resp := ""
	posts, err := j.Store.Queries.GetMyPostsByFuzzyContent(ctx, store_queries.GetMyPostsByFuzzyContentParams{
		ActorDid: actorDid,
		Query:    searchText,
		Limit:    5,
	})
	if err != nil {
		resp = fmt.Sprintf("I had trouble finding your posts 😢\nPlease try again later!")
		return resp, nil, fmt.Errorf("failed to search posts for user (%s): %+v", actorDid, err)
	}

	if len(posts) == 0 {
		resp = fmt.Sprint("I couldn't find any posts matching your query, I'm sorry 😢")
		return resp, nil, nil
	}

	resp = fmt.Sprintf("Here are the posts I found matching your query:\n\n")

	facets := []*appbsky.RichtextFacet{}
	links := []string{}
	texts := []string{}
	for i, post := range posts {
		resp += fmt.Sprintf("📝 {link:%d}\n", i)
		links = append(links, fmt.Sprintf("https://bsky.app/profile/%s/post/%s", actorDid, post.Rkey))
		texts = append(texts, post.Content.String)
	}

	resp, facets, err = insertLinks(resp, links, texts, facets)
	if err != nil {
		resp = fmt.Sprintf("I had trouble finding your posts 😢\nPlease try again later!")
		return resp, nil, fmt.Errorf("failed to insert links: %+v", err)
	}

	return resp, facets, nil
}

func (j *Jazbot) FindFriends(ctx context.Context, actorDid string) (string, []*appbsky.RichtextFacet, error) {
	ctx, span := tracer.Start(ctx, "FindFriends")
	defer span.End()

	resp := ""
	candidateList, err := j.Store.Queries.FindPotentialFriends(ctx, store_queries.FindPotentialFriendsParams{
		ActorDid: actorDid,
		Limit:    5,
	})
	if err != nil {
		resp = fmt.Sprintf("I had trouble finding a friend for you 😢\nPlease try again later!")
		return resp, nil, fmt.Errorf("failed to get potential friends for user (%s): %+v", actorDid, err)
	}
	// Lookup the handles for the candidates
	candidates := []friendCandidate{}
	for _, candidate := range candidateList {
		handle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, candidate.ActorDid)
		if err != nil {
			j.Logger.Errorf("failed to get handle for user (%s): %+v", candidate, err)
			continue
		}
		candidates = append(candidates, friendCandidate{
			Did:       candidate.ActorDid,
			Handle:    handle,
			LikeCount: candidate.OverlapCount,
		})
	}

	if len(candidates) == 0 {
		resp = "I'm sorry but I couldn't find any potential friends for you 😢\nPlease try again later!"
		return resp, nil, fmt.Errorf("no potential friends found for user (%s)", actorDid)
	}

	resp = "The following users have recent likes in common with you:"

	facets := []*appbsky.RichtextFacet{}

	for _, candidate := range candidates {
		resp += "\n"
		truncatedHandle := candidate.Handle
		if len(truncatedHandle) > 40 {
			truncatedHandle = truncatedHandle[:37] + "..."
		}
		facets = append(facets, &appbsky.RichtextFacet{
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Link: &appbsky.RichtextFacet_Link{
					Uri: fmt.Sprintf("https://bsky.app/profile/%s", candidate.Did),
				},
			}},
			Index: &appbsky.RichtextFacet_ByteSlice{
				ByteStart: int64(len(resp) - 1),
				ByteEnd:   int64(len(resp) + len(truncatedHandle) + 1),
			},
		})
		resp += fmt.Sprintf("@%s (%d)", truncatedHandle, candidate.LikeCount)
	}

	return resp, facets, nil
}
