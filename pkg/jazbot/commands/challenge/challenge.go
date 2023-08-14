package challenge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/metrics"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/util"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("challenge")

var timeFormat = "Jan 02, 2006 at 15:04 MST"
var challengeAcceptWindow = time.Hour * 12
var eventDuration = time.Hour * 48

type ChallengeCommand struct {
	name string
	desc string
	args []string

	Ctx *util.CommandCtx

	Store     *store.Store
	PLCMirror string

	shutdown chan chan struct{}
}

func NewChallengeCommand(
	ctx *util.CommandCtx,
	store *store.Store,
	plcMirror string,
) *ChallengeCommand {
	return &ChallengeCommand{
		name: "challenge",
		desc: "Challenge a user to a like battle!" +
			"\nA like battle is a contest to see who can give the most likes to the other person's posts in a 48 hour period." +
			"\nYou can only have one active challenge at a time.",
		args: []string{"@{handle}"},
		Ctx:  ctx,

		Store:     store,
		PLCMirror: plcMirror,

		shutdown: make(chan chan struct{}),
	}
}

func (c *ChallengeCommand) Name() string {
	return c.name
}

func (c *ChallengeCommand) Help() *jazbot.Help {
	return &jazbot.Help{
		Name: c.name,
		Desc: c.desc,
		Args: c.args,
	}
}

func (c *ChallengeCommand) Startup() error {
	// Start a goroutine to conclude challenges once every minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for {
			ctx := context.Background()
			select {
			case done := <-c.shutdown:
				c.Ctx.Logger.Info("shutting down challenge ticker")
				ticker.Stop()
				close(done)
				return
			case <-ticker.C:
				errs := c.ConcludeChallenges(ctx)
				if len(errs) > 0 {
					c.Ctx.Logger.Errorf("failed to conclude challenges: %+v", errs)
				}
			}
		}
	}()
	return nil
}

func (c *ChallengeCommand) Shutdown() error {
	c.Ctx.Logger.Info("shutting down challenge command")
	done := make(chan struct{})
	c.shutdown <- done
	<-done
	c.Ctx.Logger.Info("challenge command shutdown complete")
	return nil
}

func (c *ChallengeCommand) Execute(
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
	ctx, span := tracer.Start(ctx, "Execute")
	defer span.End()

	if len(args) == 0 {
		resp = fmt.Sprintf("You need to specify a user to challenge")
		metrics.FailedCommandsReceivedCounter.WithLabelValues("no_args").Inc()
		return resp, nil, nil, fmt.Errorf("no args")
	}

	arg := args[0]

	// Check if arg is in the format of @{handle} and that the handle is valid
	if !strings.HasPrefix(arg, "@") {
		resp = fmt.Sprintf("I couldn't find the handle in your message")
		metrics.FailedCommandsReceivedCounter.WithLabelValues("parse_failed").Inc()
		return resp, nil, nil, fmt.Errorf("failed to parse handle from arg (%s)", arg)
	}

	targetHandle := strings.TrimPrefix(arg, "@")

	targetDid, err := util.GetDIDFromPLCMirror(ctx, c.PLCMirror, targetHandle)
	if err != nil {
		resp = fmt.Sprintf("I couldn't find a user with the handle: %s", targetHandle)
		metrics.FailedCommandsReceivedCounter.WithLabelValues("handle_not_found").Inc()
		return resp, nil, nil, fmt.Errorf("failed to get did for handle (%s): %+v", targetHandle, err)
	}

	if targetDid == actorDid {
		resp = fmt.Sprintf("You can't challenge yourself, silly!")
		metrics.FailedCommandsReceivedCounter.WithLabelValues("self_challenge").Inc()
		return resp, nil, nil, fmt.Errorf("user (%s) tried to challenge themself", actorDid)
	}

	if targetDid == c.Ctx.BotDid {
		resp = fmt.Sprintf("Sorry but I can't accept your challenge, I'm just a helpful bot!")
		metrics.FailedCommandsReceivedCounter.WithLabelValues("bot_challenge").Inc()
		return resp, nil, nil, fmt.Errorf("user (%s) tried to challenge the bot", actorDid)
	}

	// Check if there's already a challenge in progress
	events, err := c.Store.Queries.GetActiveEventsForInitiator(ctx, store_queries.GetActiveEventsForInitiatorParams{
		InitiatorDid: actorDid,
		EventType:    "challenge",
		Limit:        1,
	})
	if err != nil {
		resp = fmt.Sprintf("I couldn't check if you already have an active battle, please try again later!")
		return resp, nil, nil, fmt.Errorf("failed to get active challenge for user (%s): %+v", actorDid, err)
	}

	facets = []*appbsky.RichtextFacet{}

	// If there's an existing challenge, return the status
	if len(events) > 0 {
		existingEvent := events[0]

		// Lookup the handle for the target
		targetHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, existingEvent.TargetDid)
		if err != nil {
			resp = fmt.Sprintf("I had trouble loading the status of your existing battle, please try again later!")
			return resp, facets, nil, fmt.Errorf("failed to get handle for participant (%s): %+v", existingEvent.TargetDid, err)
		}

		resp = fmt.Sprintf(
			"You already have an active battle with {handle:0} \n",
		)

		if existingEvent.WindowEnd.Valid {
			resp += fmt.Sprintf("It ends at %s so please wait until it is concluded before starting a new one!", existingEvent.WindowEnd.Time.Format(timeFormat))
		} else {
			resp += fmt.Sprintf(
				"Your challenged opponent has not yet accepted your challenge, they have until %s to accept it!",
				existingEvent.ExpiredAt.Time.Format(timeFormat),
			)
		}

		resp, facets, err = util.InsertMentions(resp, []string{existingEvent.TargetDid}, []string{targetHandle}, facets)
		if err != nil {
			resp = fmt.Sprintf("I had trouble loading the status of your existing battle, please try again later!")
			return resp, facets, nil, fmt.Errorf("failed to insert mention: %+v", err)
		}

		return resp, facets, nil, fmt.Errorf("user (%s) already has an active battle", actorDid)
	}

	initiatorHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, actorDid)
	if err != nil {
		resp = fmt.Sprintf("I had trouble creating your battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to get handle for initiator (%s): %+v", actorDid, err)
	}

	// Create a new challenge
	eventID, err := c.Store.Queries.CreateEvent(ctx, store_queries.CreateEventParams{
		InitiatorDid: actorDid,
		TargetDid:    targetDid,
		EventType:    "challenge",
		ExpiredAt:    sql.NullTime{Valid: true, Time: time.Now().Add(challengeAcceptWindow)},
	})
	if err != nil {
		resp = fmt.Sprintf("I couldn't create a new battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to create challenge for user (%s): %+v", actorDid, err)
	}

	resp = fmt.Sprintf("{handle:0} has challenged {handle:1} to a Like Battle!\n")
	resp += fmt.Sprint("The user that gives out the most likes 48 hours from the start will be the winner!")
	resp += fmt.Sprint("\n\nTo start the battle, the challenged must reply to this post in the next 12 hours with: !jazbot accept")

	resp, facets, err = util.InsertMentions(resp, []string{actorDid, targetDid}, []string{initiatorHandle, targetHandle}, facets)
	if err != nil {
		resp = fmt.Sprintf("I had trouble creating your battle, please try again later!")
		return resp, facets, nil, fmt.Errorf("failed to insert mention: %+v", err)
	}

	c.Ctx.Logger.Infow("Like Battle created", "initiator", actorDid, "target", targetDid, "challenge_expires_at", challengeAcceptWindow)

	addEventPost := func(out *comatproto.RepoCreateRecord_Output, createPostErr error) error {
		if createPostErr != nil && out != nil {
			err := c.Store.Queries.AddEventPost(ctx, store_queries.AddEventPostParams{
				ID:      eventID,
				PostUri: sql.NullString{String: out.Uri, Valid: true},
			})
			if err != nil {
				c.Ctx.Logger.Errorf("failed to add post URI to event: %+v", err)
			}
			return err
		}
		return nil
	}

	return resp, facets, &addEventPost, nil
}
