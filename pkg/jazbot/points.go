package jazbot

import (
	"context"
	"fmt"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

func (j *Jazbot) GetLeaderboard(ctx context.Context, actorDid string) (string, []*appbsky.RichtextFacet, error) {
	ctx, span := tracer.Start(ctx, "GetLeaderboard")
	defer span.End()

	resp := ""
	topScorers, err := j.Store.Queries.GetTopUsersByPoints(ctx, 5)
	if err != nil {
		resp = fmt.Sprintf("I had trouble getting the leaderboard 😢\nPlease try again later!")
		return resp, nil, fmt.Errorf("failed to get leaderboard: %+v", err)
	}

	if len(topScorers) == 0 {
		resp = "No one has any points yet 😢\nParticipate in a challenge to earn some!"
		return resp, nil, nil
	}

	resp = "Here are the top scorers:\n"
	handles := []string{}
	uris := []string{}
	for i, scorer := range topScorers {
		resp += fmt.Sprintf("%d: {link:%d} (%d points)\n", i+1, i, scorer.TotalPoints)
		uris = append(uris, fmt.Sprintf("https://bsky.app/profile/%s", scorer.ActorDid))

		// Lookup the handle
		handle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, scorer.ActorDid)
		if err != nil {
			resp = fmt.Sprintf("I had trouble getting the leaderboard 😢\nPlease try again later!")
			return resp, nil, fmt.Errorf("failed to get handle for user (%s): %+v", scorer.ActorDid, err)
		}
		handles = append(handles, "@"+handle)
	}

	facets := []*appbsky.RichtextFacet{}
	resp, facets, err = insertLinks(resp, uris, handles, facets)
	if err != nil {
		resp = fmt.Sprintf("I had trouble getting the leaderboard 😢\nPlease try again later!")
		return resp, nil, fmt.Errorf("failed to insert links: %+v", err)
	}

	return resp, facets, nil
}

func (j *Jazbot) GetPoints(ctx context.Context, actorDid string) (string, error) {
	ctx, span := tracer.Start(ctx, "GetPoints")
	defer span.End()

	resp := ""
	points, err := j.Store.Queries.GetTotalPointsForActor(ctx, actorDid)
	if err != nil {
		resp = fmt.Sprintf("I had trouble getting your points 😢\nPlease try again later!")
		return resp, fmt.Errorf("failed to get points for user (%s): %+v", actorDid, err)
	}

	if points == 0 {
		resp = "You don't have any points yet 😢\nParticipate in a challenge to earn some!"
		return resp, nil
	}

	if points > 100 {
		resp = fmt.Sprintf("You have %d points! You're a star 🌟!", points)
		return resp, nil
	}

	resp = fmt.Sprintf("You have %d points!", points)

	return resp, nil
}
