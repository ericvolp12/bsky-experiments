package challenge

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/metrics"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/util"
	"github.com/goccy/go-json"
	"github.com/sqlc-dev/pqtype"
	"go.uber.org/zap"
)

type ChallengeResults struct {
	InitiatorLikes int64  `json:"initiator_likes"`
	TargetLikes    int64  `json:"target_likes"`
	Tie            bool   `json:"tie"`
	WinnerDid      string `json:"winner_did"`
}

func (c *ChallengeCommand) ConcludeChallenges(ctx context.Context) []error {
	ctx, span := tracer.Start(ctx, "ConcludeChallenges")
	defer span.End()

	// Get challenges that have ended
	events, err := c.Store.Queries.GetEventsToConclude(ctx, store_queries.GetEventsToConcludeParams{
		EventType: "challenge",
		Limit:     100,
	})
	if err != nil {
		return []error{fmt.Errorf("failed to get events to conclude: %+v", err)}
	}

	errs := []error{}

	for _, event := range events {
		err = c.ConcludeChallenge(ctx, &event)
		if err != nil {
			c.Ctx.Logger.Errorw("failed to conclude challenge", zap.Error(err))
			errs = append(errs, err)
		}
	}

	return errs
}

func (c *ChallengeCommand) ConcludeChallenge(ctx context.Context, event *store_queries.Event) error {
	ctx, span := tracer.Start(ctx, "ConcludeChallenge")
	defer span.End()

	c.Ctx.Logger.Infow("concluding challenge",
		"initiator_did", event.InitiatorDid,
		"target_did", event.TargetDid,
		"event_id", event.ID,
		"event_type", event.EventType,
		"event_created_at", event.CreatedAt,
		"event_window_start", event.WindowStart.Time,
		"event_window_end", event.WindowEnd.Time,
	)

	// Get the number of likes for each user
	initiatorLikes, err := c.Store.Queries.GetLikesGivenByActorFromTo(ctx, store_queries.GetLikesGivenByActorFromToParams{
		ActorDid: event.InitiatorDid,
		From:     sql.NullTime{Time: event.WindowStart.Time, Valid: true},
		To:       sql.NullTime{Time: event.WindowEnd.Time, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to get likes given by initator (%s): %+v", event.InitiatorDid, err)
	}

	targetLikes, err := c.Store.Queries.GetLikesGivenByActorFromTo(ctx, store_queries.GetLikesGivenByActorFromToParams{
		ActorDid: event.TargetDid,
		From:     sql.NullTime{Time: event.WindowStart.Time, Valid: true},
		To:       sql.NullTime{Time: event.WindowEnd.Time, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to get likes given by target (%s): %+v", event.TargetDid, err)
	}

	// Get the handles for each user
	initiatorHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, event.InitiatorDid)
	if err != nil {
		return fmt.Errorf("failed to get handle for initiator (%s): %+v", event.InitiatorDid, err)
	}

	targetHandle, err := util.GetHandleFromPLCMirror(ctx, c.PLCMirror, event.TargetDid)
	if err != nil {
		return fmt.Errorf("failed to get handle for target (%s): %+v", event.TargetDid, err)
	}

	resp := ""
	facets := []*appbsky.RichtextFacet{}
	tie := initiatorLikes == targetLikes
	winnerDid := ""

	if initiatorLikes <= 10 || targetLikes <= 10 {
		resp = fmt.Sprintf("The battle between {handle:0} (%d) and {handle:1} (%d) has ended!\n", initiatorLikes, targetLikes)
		resp += fmt.Sprint("Unfortunately, one or both participants failed to give out > 10 likes, so there is no winner!")

		resp, facets, err = util.InsertMentions(resp, []string{event.InitiatorDid, event.TargetDid}, []string{initiatorHandle, targetHandle}, facets)
		if err != nil {
			return fmt.Errorf("failed to insert mention: %+v", err)
		}
	} else {
		switch {
		case initiatorLikes > targetLikes:
			resp = fmt.Sprint("{handle:0} has won the Like Battle against {handle:1}!\n")
			resp += fmt.Sprintf("{handle:0} gave out %d likes to {handle:1}'s %d likes!", initiatorLikes, targetLikes)
			resp += fmt.Sprint("\n\n{handle:0} has earned 3 points for winning the battle!")
			resp, facets, err = util.InsertMentions(resp, []string{event.InitiatorDid, event.TargetDid}, []string{initiatorHandle, targetHandle}, facets)
			if err != nil {
				return fmt.Errorf("failed to insert mention: %+v", err)
			}
			winnerDid = event.InitiatorDid
		case initiatorLikes < targetLikes:
			resp = fmt.Sprint("{handle:0} has won the Like Battle against {handle:1}!\n")
			resp += fmt.Sprintf("{handle:0} gave out %d likes to {handle:1}'s %d likes!", targetLikes, initiatorLikes)
			resp += fmt.Sprint("\n\n{handle:0} has earned 3 points for winning the battle!")
			resp, facets, err = util.InsertMentions(resp, []string{event.TargetDid, event.InitiatorDid}, []string{targetHandle, initiatorHandle}, facets)
			if err != nil {
				return fmt.Errorf("failed to insert mention: %+v", err)
			}
			winnerDid = event.TargetDid
		case initiatorLikes == targetLikes:
			resp = fmt.Sprint("{handle:0} and {handle:1} have tied in the Like Battle!\n")
			resp += fmt.Sprintf("Both contestants gave out %d likes!", initiatorLikes)
			if initiatorLikes >= 10 {
				resp += fmt.Sprint("\n\nBoth contestants have earned 1 point for tying the battle!")
				// Add a point to each user
				err = c.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
					EventID:  event.ID,
					ActorDid: event.InitiatorDid,
					Points:   1,
				})
				if err != nil {
					return fmt.Errorf("failed to create point assignment: %+v", err)
				}

				err = c.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
					EventID:  event.ID,
					ActorDid: event.TargetDid,
					Points:   1,
				})
				if err != nil {
					return fmt.Errorf("failed to create point assignment: %+v", err)
				}
			}
			resp, facets, err = util.InsertMentions(resp, []string{event.InitiatorDid, event.TargetDid}, []string{initiatorHandle, targetHandle}, facets)
			if err != nil {
				return fmt.Errorf("failed to insert mention: %+v", err)
			}
		}

		if !tie {
			winnerDid := event.InitiatorDid
			if initiatorLikes < targetLikes {
				winnerDid = event.TargetDid
			}
			// Add a point to the winner
			err = c.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
				EventID:  event.ID,
				ActorDid: winnerDid,
				Points:   3,
			})
			if err != nil {
				return fmt.Errorf("failed to create point assignment: %+v", err)
			}
		}
	}

	results := ChallengeResults{
		Tie:            tie,
		WinnerDid:      winnerDid,
		InitiatorLikes: initiatorLikes,
		TargetLikes:    targetLikes,
	}

	resultsBytes, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to marshal results: %+v", err)
	}

	err = c.Store.Queries.ConcludeEvent(ctx, store_queries.ConcludeEventParams{
		ID:          event.ID,
		Results:     pqtype.NullRawMessage{RawMessage: resultsBytes, Valid: true},
		ConcludedAt: sql.NullTime{Time: time.Now(), Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to conclude event: %+v", err)
	}

	c.Ctx.Logger.Infow("Like Battle concluded",
		"event_id", event.ID,
		"initiator_did", event.InitiatorDid,
		"target_did", event.TargetDid,
		"initiator_likes", initiatorLikes,
		"target_likes", targetLikes,
		"tie", tie,
		"winner_did", winnerDid,
	)

	post := appbsky.FeedPost{
		Text:      resp,
		CreatedAt: time.Now().Format(time.RFC3339),
		Facets:    facets,
	}

	c.Ctx.Limiter.Wait(ctx)

	c.Ctx.ClientMux.RLock()
	out, err := comatproto.RepoCreateRecord(ctx, c.Ctx.Client, &comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.feed.post",
		Repo:       c.Ctx.BotDid,
		Record:     &lexutil.LexiconTypeDecoder{Val: &post},
	})
	c.Ctx.ClientMux.RUnlock()

	if err != nil {
		metrics.PostsFailedCounter.WithLabelValues("create_failed").Inc()
		return fmt.Errorf("failed to create record: %+v", err)
	}

	uri, err := consumer.GetURI(out.Uri)
	if err != nil {
		metrics.PostsFailedCounter.WithLabelValues("get_uri_failed").Inc()
		return fmt.Errorf("failed to get uri from post: %+v", err)
	}

	metrics.PostsSentCounter.WithLabelValues().Inc()

	c.Ctx.Logger.Infow("post published",
		"at_uri", out.Uri,
		"link", fmt.Sprintf("https://bsky.app/profile/%s/post/%s", uri.Did, uri.RKey),
	)

	return nil
}
