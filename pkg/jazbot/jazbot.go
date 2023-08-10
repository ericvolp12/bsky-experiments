package jazbot

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	intxrpc "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"github.com/goccy/go-json"
	"github.com/sqlc-dev/pqtype"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/time/rate"
)

type Jazbot struct {
	Store     *store.Store
	Client    *xrpc.Client
	Logger    *zap.SugaredLogger
	clientMux *sync.RWMutex
	BotDid    string
	limiter   *rate.Limiter
	PLCMirror string
}

var SupportedCommands = map[string]string{
	"getlikecount":        "Get the number of likes you have received",
	"getlikesgiven":       "Get the number of likes you have given",
	"findmeafriend":       "Find users with shared interests",
	"challenge @{handle}": "Challenge a user to a like battle!",
	"points":              "Get your current points",
}

func NewJazbot(ctx context.Context, store *store.Store, botDid, plcMirror string) (*Jazbot, error) {
	client, err := intxrpc.GetXRPCClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get xrpc client: %+v", err)
	}

	rawLogger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %+v", err)
	}

	logger := rawLogger.With(zap.String("source", "jazbot")).Sugar()

	j := Jazbot{
		Store:     store,
		Client:    client,
		Logger:    logger,
		clientMux: &sync.RWMutex{},
		BotDid:    botDid,
		limiter:   rate.NewLimiter(rate.Limit(0.5), 1), // One request every 2 seconds
		PLCMirror: plcMirror,
	}

	// Start a goroutine to refresh the xrpc client
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := intxrpc.RefreshAuth(ctx, j.Client, j.clientMux)
				if err != nil {
					fmt.Printf("failed to refresh auth: %+v\n", err)
				}
			}
		}
	}()

	// Start a goroutine to conclude challenges once every minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				errs := j.ConcludeChallenges(ctx)
				if len(errs) > 0 {
					logger.Errorf("failed to conclude challenges: %+v", errs)
				}
			}
		}
	}()

	return &j, nil
}

type friendCandidate struct {
	Did       string
	Handle    string
	LikeCount int64
}

func (j *Jazbot) HandleRequest(
	ctx context.Context,
	actorDid string,
	rkey string,
	text string,
	postCid *lexutil.LexLink,
) error {
	// Requests are of the form:
	// !jazbot <command>
	ctx, span := tracer.Start(ctx, "HandleRequest")
	defer span.End()

	commandsReceivedCounter.WithLabelValues().Inc()

	p := message.NewPrinter(language.English)

	resp := ""
	facets := []*appbsky.RichtextFacet{}
	for {
		// Get the command
		parts := strings.FieldsFunc(text, func(r rune) bool {
			return r == ' ' || r == '\n' || r == '\t'
		})
		if len(parts) < 2 {
			resp = fmt.Sprintf("I couldn't parse a command from your message")
			failedCommandsReceivedCounter.WithLabelValues("parse_failed").Inc()
			break
		}

		command := strings.ToLower(parts[1])

		// Handle the command
		switch command {
		case "help":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			resp = "I currently support the following commands:\n"
			// sort the commands
			keys := []string{}
			for k := range SupportedCommands {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			// print the commands
			for _, k := range keys {
				resp += fmt.Sprintf("%s: %s\n", k, SupportedCommands[k])
			}
		case "getlikecount":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			likeCount, err := j.Store.Queries.GetTotalLikesReceivedByActor(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get like count for user (%s): %+v", actorDid, err)
				resp = fmt.Sprintf("I had trouble getting your received like count 😢\nPlease try again later!")
				break
			}

			resp = p.Sprintf("You have received a total of %d likes", likeCount)
		case "getlikesgiven":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			likeCount, err := j.Store.Queries.GetTotalLikesGivenByActor(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get like count for user (%s): %+v", actorDid, err)
				resp = fmt.Sprintf("I had trouble getting your given like count 😢\nPlease try again later!")
				break
			}

			resp = p.Sprintf("You have given a total of %d likes", likeCount)
		case "findmeafriend":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			var err error
			resp, facets, err = j.FindFriends(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to find friends for user (%s): %+v", actorDid, err)
			}
		case "challenge":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			if len(parts) < 3 {
				resp = "Please provide the handle of the user you would like to challenge"
				break
			}

			arg := parts[2]

			var err error
			resp, facets, err = j.Challenge(ctx, actorDid, arg)
			if err != nil {
				j.Logger.Errorf("failed to challenge user (%s): %+v", actorDid, err)
			}
		case "points":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			var err error
			resp, err = j.GetPoints(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get points for user (%s): %+v", actorDid, err)
			}
		default:
			failedCommandsReceivedCounter.WithLabelValues("invalid_command").Inc()
			resp = fmt.Sprintf("I'm not familiar with the command: %s", command)
		}
		break
	}

	if resp != "" {
		parent := comatproto.RepoStrongRef{
			LexiconTypeID: "app.bsky.feed.post",
			Uri:           fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actorDid, rkey),
			Cid:           postCid.String(),
		}
		post := appbsky.FeedPost{
			Text:      resp,
			CreatedAt: time.Now().Format(time.RFC3339),
			Reply: &appbsky.FeedPost_ReplyRef{
				Parent: &parent,
				Root:   &parent,
			},
			Facets: facets,
		}

		j.limiter.Wait(ctx)

		j.clientMux.RLock()
		out, err := comatproto.RepoCreateRecord(ctx, j.Client, &comatproto.RepoCreateRecord_Input{
			Collection: "app.bsky.feed.post",
			Repo:       j.BotDid,
			Record:     &lexutil.LexiconTypeDecoder{Val: &post},
		})
		j.clientMux.RUnlock()

		if err != nil {
			postsFailedCounter.WithLabelValues("create_failed").Inc()
			return fmt.Errorf("failed to create record: %+v", err)
		}

		uri, err := consumer.GetURI(out.Uri)
		if err != nil {
			postsFailedCounter.WithLabelValues("get_uri_failed").Inc()
			return fmt.Errorf("failed to get uri from post: %+v", err)
		}

		postsSentCounter.WithLabelValues().Inc()

		j.Logger.Infow("post published",
			"at_uri", out.Uri,
			"link", fmt.Sprintf("https://bsky.app/profile/%s/post/%s", uri.Did, uri.RKey),
		)
	}

	return nil
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

var timeFormat = "Jan 02, 2006 at 15:04 MST"

func (j *Jazbot) Challenge(ctx context.Context, actorDid string, arg string) (string, []*appbsky.RichtextFacet, error) {
	ctx, span := tracer.Start(ctx, "Challenge")
	defer span.End()

	eventDuration := time.Hour * 48

	resp := ""
	// Check if arg is in the format of @{handle} and that the handle is valid
	if !strings.HasPrefix(arg, "@") {
		resp = fmt.Sprintf("I couldn't find the handle in your message")
		failedCommandsReceivedCounter.WithLabelValues("parse_failed").Inc()
		return resp, nil, fmt.Errorf("failed to parse handle from arg (%s)", arg)
	}

	targetHandle := strings.TrimPrefix(arg, "@")

	targetDid, err := GetDIDFromPLCMirror(ctx, j.PLCMirror, targetHandle)
	if err != nil {
		resp = fmt.Sprintf("I couldn't find a user with the handle: %s", targetHandle)
		failedCommandsReceivedCounter.WithLabelValues("handle_not_found").Inc()
		return resp, nil, fmt.Errorf("failed to get did for handle (%s): %+v", targetHandle, err)
	}

	// Check if there's already a challenge in progress
	events, err := j.Store.Queries.GetActiveEventsForInitiator(ctx, store_queries.GetActiveEventsForInitiatorParams{
		InitiatorDid: actorDid,
		EventType:    "challenge",
		Limit:        1,
	})
	if err != nil {
		resp = fmt.Sprintf("I couldn't check if you already have an active battle, please try again later!")
		return resp, nil, fmt.Errorf("failed to get active challenge for user (%s): %+v", actorDid, err)
	}

	facets := []*appbsky.RichtextFacet{}

	// If there's an existing challenge, return the status
	if len(events) > 0 {
		existingEvent := events[0]

		// Lookup the handle for the target
		targetHandle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, existingEvent.TargetDid)
		if err != nil {
			resp = fmt.Sprintf("I had trouble loading the status of your existing battle, please try again later!")
			return resp, facets, fmt.Errorf("failed to get handle for participant (%s): %+v", existingEvent.TargetDid, err)
		}

		resp = fmt.Sprintf(
			"You already have an active battle with {handle:0} \nIt ends at %s so please wait until it is concluded before starting a new one!",
			existingEvent.CompletedAt.Local().Format(timeFormat),
		)

		resp, facets, err = insertMentions(resp, []string{existingEvent.TargetDid}, []string{targetHandle}, facets)
		if err != nil {
			resp = fmt.Sprintf("I had trouble loading the status of your existing battle, please try again later!")
			return resp, facets, fmt.Errorf("failed to insert mention: %+v", err)
		}

		return resp, facets, fmt.Errorf("user (%s) already has an active battle", actorDid)
	}

	initiatorHandle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, actorDid)
	if err != nil {
		resp = fmt.Sprintf("I had trouble creating your battle, please try again later!")
		return resp, facets, fmt.Errorf("failed to get handle for initiator (%s): %+v", actorDid, err)
	}

	challengeEnd := time.Now().Add(eventDuration)

	// Create a new challenge
	err = j.Store.Queries.CreateEvent(ctx, store_queries.CreateEventParams{
		InitiatorDid: actorDid,
		TargetDid:    targetDid,
		EventType:    "challenge",
		CompletedAt:  challengeEnd,
	})
	if err != nil {
		resp = fmt.Sprintf("I couldn't create a new battle, please try again later!")
		return resp, facets, fmt.Errorf("failed to create challenge for user (%s): %+v", actorDid, err)
	}

	resp = fmt.Sprintf("{handle:0} has challenged {handle:1} to a Like Battle!\n")
	resp += fmt.Sprint("The user that gives out the most likes in the next 48 hours will be the winner!")
	resp += fmt.Sprintf("\n\nYour battle ends at %s\nGood luck!", challengeEnd.Local().Format(timeFormat))

	resp, facets, err = insertMentions(resp, []string{actorDid, targetDid}, []string{initiatorHandle, targetHandle}, facets)
	if err != nil {
		resp = fmt.Sprintf("I had trouble creating your battle, please try again later!")
		return resp, facets, fmt.Errorf("failed to insert mention: %+v", err)
	}

	j.Logger.Infow("Like Battle created", "initiator", actorDid, "target", targetDid, "challenge_end", challengeEnd)

	return resp, facets, nil
}

func (j *Jazbot) ConcludeChallenges(ctx context.Context) []error {
	ctx, span := tracer.Start(ctx, "ConcludeChallenges")
	defer span.End()

	// Get challenges that have ended
	events, err := j.Store.Queries.GetEventsToConclude(ctx, store_queries.GetEventsToConcludeParams{
		EventType: "challenge",
		Limit:     100,
	})
	if err != nil {
		return []error{fmt.Errorf("failed to get events to conclude: %+v", err)}
	}

	errs := []error{}

	for _, event := range events {
		err = j.ConcludeChallenge(ctx, &event)
		if err != nil {
			j.Logger.Errorw("failed to conclude challenge", zap.Error(err))
			errs = append(errs, err)
		}
	}

	return errs
}

type ChallengeResults struct {
	InitiatorLikes int64  `json:"initiator_likes"`
	TargetLikes    int64  `json:"target_likes"`
	Tie            bool   `json:"tie"`
	WinnerDid      string `json:"winner_did"`
}

func (j *Jazbot) ConcludeChallenge(ctx context.Context, event *store_queries.Event) error {
	ctx, span := tracer.Start(ctx, "ConcludeChallenge")
	defer span.End()

	j.Logger.Infow("concluding challenge",
		"initiator_did", event.InitiatorDid,
		"target_did", event.TargetDid,
		"event_id", event.ID,
		"event_type", event.EventType,
		"event_created_at", event.CreatedAt,
		"event_completed_at", event.CompletedAt,
	)

	// Get the number of likes for each user
	initiatorLikes, err := j.Store.Queries.GetLikesGivenByActorFromTo(ctx, store_queries.GetLikesGivenByActorFromToParams{
		ActorDid: event.InitiatorDid,
		From:     sql.NullTime{Time: event.CreatedAt, Valid: true},
		To:       sql.NullTime{Time: event.CompletedAt, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to get likes given by initator (%s): %+v", event.InitiatorDid, err)
	}

	targetLikes, err := j.Store.Queries.GetLikesGivenByActorFromTo(ctx, store_queries.GetLikesGivenByActorFromToParams{
		ActorDid: event.TargetDid,
		From:     sql.NullTime{Time: event.CreatedAt, Valid: true},
		To:       sql.NullTime{Time: event.CompletedAt, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to get likes given by target (%s): %+v", event.TargetDid, err)
	}

	// Get the handles for each user
	initiatorHandle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, event.InitiatorDid)
	if err != nil {
		return fmt.Errorf("failed to get handle for initiator (%s): %+v", event.InitiatorDid, err)
	}

	targetHandle, err := GetHandleFromPLCMirror(ctx, j.PLCMirror, event.TargetDid)
	if err != nil {
		return fmt.Errorf("failed to get handle for target (%s): %+v", event.TargetDid, err)
	}

	resp := ""
	facets := []*appbsky.RichtextFacet{}

	tie := initiatorLikes == targetLikes
	winnerDid := ""

	switch {
	case initiatorLikes > targetLikes:
		resp = fmt.Sprint("{handle:0} has won the Like Battle against {handle:1}!\n")
		resp += fmt.Sprintf("{handle:0} gave out %d likes to {handle:1}'s %d likes!", initiatorLikes, targetLikes)
		resp += fmt.Sprint("\n\n{handle:0} has earned 3 points for winning the battle!")
		resp, facets, err = insertMentions(resp, []string{event.InitiatorDid, event.TargetDid}, []string{initiatorHandle, targetHandle}, facets)
		if err != nil {
			return fmt.Errorf("failed to insert mention: %+v", err)
		}
		winnerDid = event.InitiatorDid
	case initiatorLikes < targetLikes:
		resp = fmt.Sprint("{handle:0} has won the Like Battle against {handle:1}!\n")
		resp += fmt.Sprintf("{handle:0} gave out %d likes to {handle:1}'s %d likes!", targetLikes, initiatorLikes)
		resp += fmt.Sprint("\n\n{handle:0} has earned 3 points for winning the battle!")
		resp, facets, err = insertMentions(resp, []string{event.TargetDid, event.InitiatorDid}, []string{targetHandle, initiatorHandle}, facets)
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
			err = j.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
				EventID:  event.ID,
				ActorDid: event.InitiatorDid,
				Points:   1,
			})
			if err != nil {
				return fmt.Errorf("failed to create point assignment: %+v", err)
			}

			err = j.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
				EventID:  event.ID,
				ActorDid: event.TargetDid,
				Points:   1,
			})
			if err != nil {
				return fmt.Errorf("failed to create point assignment: %+v", err)
			}
		} else {
			resp += fmt.Sprint("\n\nSince neither contestant issued more than 10 likes, no one earns a point for tying the battle!")
		}
		resp, facets, err = insertMentions(resp, []string{event.InitiatorDid, event.TargetDid}, []string{initiatorHandle, targetHandle}, facets)
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
		err = j.Store.Queries.CreatePointAssignment(ctx, store_queries.CreatePointAssignmentParams{
			EventID:  event.ID,
			ActorDid: winnerDid,
			Points:   3,
		})
		if err != nil {
			return fmt.Errorf("failed to create point assignment: %+v", err)
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

	err = j.Store.Queries.ConcludeEvent(ctx, store_queries.ConcludeEventParams{
		ID:          event.ID,
		Results:     pqtype.NullRawMessage{RawMessage: resultsBytes, Valid: true},
		ConcludedAt: sql.NullTime{Time: time.Now(), Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to conclude event: %+v", err)
	}

	j.Logger.Infow("Like Battle concluded",
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

	j.limiter.Wait(ctx)

	j.clientMux.RLock()
	out, err := comatproto.RepoCreateRecord(ctx, j.Client, &comatproto.RepoCreateRecord_Input{
		Collection: "app.bsky.feed.post",
		Repo:       j.BotDid,
		Record:     &lexutil.LexiconTypeDecoder{Val: &post},
	})
	j.clientMux.RUnlock()

	if err != nil {
		postsFailedCounter.WithLabelValues("create_failed").Inc()
		return fmt.Errorf("failed to create record: %+v", err)
	}

	uri, err := consumer.GetURI(out.Uri)
	if err != nil {
		postsFailedCounter.WithLabelValues("get_uri_failed").Inc()
		return fmt.Errorf("failed to get uri from post: %+v", err)
	}

	postsSentCounter.WithLabelValues().Inc()

	j.Logger.Infow("post published",
		"at_uri", out.Uri,
		"link", fmt.Sprintf("https://bsky.app/profile/%s/post/%s", uri.Did, uri.RKey),
	)

	return nil
}

// insertMentions replaces all {handle:n} mentions with the appropriate handle
func insertMentions(text string, dids []string, handles []string, facets []*appbsky.RichtextFacet) (string, []*appbsky.RichtextFacet, error) {
	if len(dids) != len(handles) {
		return "", nil, fmt.Errorf("length of DIDs and handles should be the same")
	}

	placeholderPattern := regexp.MustCompile(`\{handle:(\d+)\}`)
	matches := placeholderPattern.FindAllStringSubmatch(text, -1)

	for _, match := range matches {
		if len(match) != 2 {
			continue
		}

		index, err := strconv.Atoi(match[1])
		if err != nil || index >= len(handles) {
			continue
		}

		// Truncate if necessary
		truncatedHandle := handles[index]
		if len(truncatedHandle) > 40 {
			truncatedHandle = truncatedHandle[:37] + "..."
		}

		// Create the facet
		startIdx := int64(strings.Index(text, match[0]))
		endIdx := startIdx + int64(len(truncatedHandle)) + 1

		facet := &appbsky.RichtextFacet{
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Mention: &appbsky.RichtextFacet_Mention{
					Did: dids[index],
				},
			}},
			Index: &appbsky.RichtextFacet_ByteSlice{
				ByteStart: startIdx,
				ByteEnd:   endIdx,
			},
		}

		facets = append(facets, facet)

		// Replace the placeholder with the handle in the text
		text = strings.Replace(text, match[0], "@"+truncatedHandle, 1)
	}

	return text, facets, nil
}
