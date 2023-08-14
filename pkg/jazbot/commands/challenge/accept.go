package challenge

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/util"
)

type AcceptCommand struct {
	name string
	desc string
	args []string

	Ctx *util.CommandCtx

	Store     *store.Store
	PLCMirror string
}

func NewAcceptCommand(
	ctx *util.CommandCtx,
	store *store.Store,
	plcMirror string,
) *ChallengeCommand {
	return &ChallengeCommand{
		name: "accept",
		desc: "Accept a challenge that has been issued to you.",
		Ctx:  ctx,

		Store:     store,
		PLCMirror: plcMirror,

		shutdown: make(chan chan struct{}),
	}
}

func (c *AcceptCommand) Name() string {
	return c.name
}

func (c *AcceptCommand) Help() *jazbot.Help {
	return &jazbot.Help{
		Name: c.name,
		Desc: c.desc,
		Args: c.args,
	}
}

func (c *AcceptCommand) Startup() error {
	return nil
}

func (c *AcceptCommand) Shutdown() error {
	return nil
}

func (c *AcceptCommand) Execute(
	ctx context.Context,
	actorDid string,
	args []string,
	parentURI string,
) (
	resp string,
	facets []*appbsky.RichtextFacet,
	cb *func(*comatproto.RepoCreateRecord_Output, error) error,
	err error,
) {
	ctx, span := tracer.Start(ctx, "AcceptChallenge")
	defer span.End()

	facets = []*appbsky.RichtextFacet{}

	// Check if the parent post is a challenge and the user is the target and it hasn't started yet
	event, err := c.Store.Queries.GetUnconfirmedEvent(ctx, store_queries.GetUnconfirmedEventParams{
		PostUri:   sql.NullString{Valid: true, String: parentURI},
		TargetDid: actorDid,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			c.Ctx.Logger.Infow("Attempted to accept non-existent challenge", "parent_uri", parentURI, "target_did", actorDid)
			resp = fmt.Sprintf("I couldn't find a pending challenge to accept, you need to be challenged by someone first!")
			return resp, facets, nil, nil
		}
		resp = fmt.Sprintf("I couldn't accept the challenge, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to get unconfirmed challenge for user (%s): %+v", actorDid, err)
	}

	// Check if the challenge has expired
	if event.ExpiredAt.Valid && event.ExpiredAt.Time.Before(time.Now()) {
		c.Ctx.Logger.Infow("Attempted to accept expired challenge", "event_id", event.ID, "expired_at", event.ExpiredAt.Time)
		resp = fmt.Sprintf("This challenge has expired, please create a new one if you'd like to battle!")
		return resp, facets, nil, nil
	}

	widowEnd := time.Now().Add(eventDuration)

	// Update the challenge to be confirmed
	err = c.Store.Queries.ConfirmEvent(ctx, store_queries.ConfirmEventParams{
		ID:          event.ID,
		WindowStart: sql.NullTime{Valid: true, Time: time.Now()},
		WindowEnd:   sql.NullTime{Valid: true, Time: widowEnd},
	})
	if err != nil {
		resp = fmt.Sprintf("I couldn't accept the challenge, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to confirm challenge for user (%s): %+v", actorDid, err)
	}

	// Get the initiator's handle
	initiatorHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, event.InitiatorDid)
	if err != nil {
		resp = fmt.Sprintf("I had trouble accepting your battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to get handle for initiator (%s): %+v", event.InitiatorDid, err)
	}

	// Get the target's handle
	targetHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, event.TargetDid)
	if err != nil {
		resp = fmt.Sprintf("I had trouble accepting your battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to get handle for target (%s): %+v", event.TargetDid, err)
	}

	resp = fmt.Sprintf("{handle:0} has accepted {handle:1}'s challenge!\n")
	resp += fmt.Sprint("The user that gives out the most likes in the next 48 hours will be the winner!")
	resp += fmt.Sprintf("\n\nThe battle will conclude at %s\nGood Luck!", widowEnd.Format(timeFormat))

	resp, facets, err = util.InsertMentions(resp, []string{event.TargetDid, event.InitiatorDid}, []string{targetHandle, initiatorHandle}, facets)
	if err != nil {
		resp = fmt.Sprintf("I had trouble accepting your battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to insert mention: %+v", err)
	}

	c.Ctx.Logger.Infow("Like Battle accepted", "initiator", event.InitiatorDid, "target", event.TargetDid, "challenge_ends_at", widowEnd)

	return resp, facets, nil, nil
}
