package jazbot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	intxrpc "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
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
	Directory identity.Directory
}

type Command struct {
	Name string
	Desc string
	Args []string
}

var SupportedCommands = map[string]Command{
	"getlikecount": {
		Name: "getlikecount",
		Desc: "Get the # of likes you have received",
	},
	"getlikesgiven": {
		Name: "getlikesgiven",
		Desc: "Get the # of likes you have given",
	},
	"findmeafriend": {
		Name: "findmeafriend",
		Desc: "Find users that have shared interests.\nCandidates are determined by looking at users with similar recent likes who you do not already follow.",
	},
	"challenge": {
		Name: "challenge",
		Desc: "Challenge a user to a like battle!" +
			"\nA like battle is a contest to see who can give the most likes to the other person's posts in a 48 hour period." +
			"\nYou can only have one active challenge at a time.",
		Args: []string{"@{handle}"},
	},
	"points": {
		Name: "points",
		Desc: "Get your current points.\nPoints can be acquired by participating in challenges.",
	},
	"search": {
		Name: "search",
		Desc: "Search your own posts.\nThe query can be a single word or a phrase that exists somewhere in any of your previous posts.\nTypos aren't supported yet, so make sure you spell everything correctly!",
		Args: []string{"{query}"},
	},
	"leaderboard": {
		Name: "leaderboard",
		Desc: "Get the top 5 coolest users ever (users with the most points)",
	},
	"getstoveylikes": {
		Name: "getstoveylikes",
		Desc: "Get the # of likes Stovey has given you",
	},
	"whereami": {
		Name: "whereami",
		Desc: "Get the name of your PDS",
	},
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

	dir := identity.DefaultDirectory()

	j := Jazbot{
		Store:     store,
		Client:    client,
		Logger:    logger,
		clientMux: &sync.RWMutex{},
		BotDid:    botDid,
		limiter:   rate.NewLimiter(rate.Limit(0.5), 1), // One request every 2 seconds
		PLCMirror: plcMirror,
		Directory: dir,
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
	parentURI *string,
	rootURI *string,
	rootCid *string,
) error {
	// Requests are of the form:
	// !jazbot <command>
	ctx, span := tracer.Start(ctx, "HandleRequest")
	defer span.End()

	commandsReceivedCounter.WithLabelValues().Inc()

	p := message.NewPrinter(language.English)

	resp := ""
	facets := []*appbsky.RichtextFacet{}
	var eventID *int64
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
			arg := ""
			if len(parts) > 2 {
				arg = parts[2]
			}
			switch {
			case arg == "":
				resp = "I currently support the following commands:\n"
				// sort the commands
				keys := []string{}
				for k := range SupportedCommands {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				// print the commands
				for i, k := range keys {
					if i > 0 {
						resp += "\n"
					}
					resp += fmt.Sprintf("%s", k)
				}
				resp += "\n\nYou can get more information about a command by typing:\n!jazbot help {command}"
			case arg != "":
				command, ok := SupportedCommands[arg]
				if !ok {
					resp = fmt.Sprintf("I'm not familiar with the command: %s", arg)
					break
				}
				resp = fmt.Sprintf("%s\n\nUsage: !jazbot %s", command.Desc, command.Name)
				if len(command.Args) > 0 {
					for _, arg := range command.Args {
						resp += fmt.Sprintf(" %s", arg)
					}
				}
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
		case "getstoveylikes":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			stoveyDID := "did:plc:3tm2l7kcljcgacctmmqru3hj"
			likeCount, err := j.Store.Queries.GetLikesReceivedByActorFromActor(ctx, store_queries.GetLikesReceivedByActorFromActorParams{
				From: stoveyDID,
				To:   actorDid,
			})
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					resp = fmt.Sprintf("Stovey hasn't given you any likes yet 😢")
					break
				}
				j.Logger.Errorf("failed to get like count for user (%s): %+v", actorDid, err)
				resp = fmt.Sprintf("I had trouble getting your received like count 😢\nPlease try again later!")
				break
			}

			if likeCount <= 0 {
				resp = fmt.Sprintf("Stovey hasn't given you any likes yet 😢\nDon't worry though, I still like you!")
				j.limiter.Wait(ctx)

				like := appbsky.FeedLike{
					CreatedAt: time.Now().Format(time.RFC3339),
					Subject: &comatproto.RepoStrongRef{
						Uri: fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actorDid, rkey),
						Cid: postCid.String(),
					},
				}

				j.clientMux.RLock()
				out, err := comatproto.RepoCreateRecord(ctx, j.Client, &comatproto.RepoCreateRecord_Input{
					Collection: "app.bsky.feed.like",
					Repo:       j.BotDid,
					Record:     &lexutil.LexiconTypeDecoder{Val: &like},
				})
				j.clientMux.RUnlock()

				if err != nil {
					postsFailedCounter.WithLabelValues("create_failed").Inc()
					return fmt.Errorf("failed to create record: %+v", err)
				}

				j.Logger.Infow("like published", "at_uri", out.Uri)
				break
			}

			resp = p.Sprintf("Stovey has given you a total of %d likes", likeCount)
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
			resp, facets, eventID, err = j.Challenge(ctx, actorDid, arg)
			if err != nil {
				j.Logger.Errorf("failed to challenge user (%s): %+v", actorDid, err)
			}
		case "accept":
			if parentURI == nil {
				failedCommandsReceivedCounter.WithLabelValues("invalid_parent_uri").Inc()
				resp = "You can only accept a challenge as a direct reply to the challenge message!"
				break
			}

			validCommandsReceivedCounter.WithLabelValues(command).Inc()

			var err error
			resp, facets, err = j.AcceptChallenge(ctx, actorDid, *parentURI)
			if err != nil {
				j.Logger.Errorf("failed to accept challenge from user (%s): %+v", actorDid, err)
			}
		case "points":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			var err error
			resp, err = j.GetPoints(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get points for user (%s): %+v", actorDid, err)
			}
		case "search":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			if len(parts) < 3 {
				resp = "Please provide a query to search for"
				break
			}

			query := strings.Join(parts[2:], " ")

			var err error
			resp, facets, err = j.SearchMyPosts(ctx, actorDid, query)
			if err != nil {
				j.Logger.Errorf("failed to search for posts for user (%s): %+v", actorDid, err)
			}
		case "leaderboard":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			var err error
			resp, facets, err = j.GetLeaderboard(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get leaderboard for user (%s): %+v", actorDid, err)
			}
		case "whereami":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			var err error
			resp, err = j.GetPDS(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get PDS for user (%s): %+v", actorDid, err)
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
		root := parent
		if rootURI != nil && rootCid != nil {
			root = comatproto.RepoStrongRef{
				LexiconTypeID: "app.bsky.feed.post",
				Uri:           *rootURI,
				Cid:           *rootCid,
			}
		}
		post := appbsky.FeedPost{
			Text:      resp,
			CreatedAt: time.Now().Format(time.RFC3339),
			Reply: &appbsky.FeedPost_ReplyRef{
				Parent: &parent,
				Root:   &root,
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

		if eventID != nil {
			err := j.Store.Queries.AddEventPost(ctx, store_queries.AddEventPostParams{
				ID:      *eventID,
				PostUri: sql.NullString{String: out.Uri, Valid: true},
			})
			if err != nil {
				j.Logger.Errorf("failed to add post URI to event: %+v", err)
			}
		}
	}

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

// insertLinks replaces all {link:n} links with an abbreviated link and adds a facet
func insertLinks(text string, urls []string, texts []string, facets []*appbsky.RichtextFacet) (string, []*appbsky.RichtextFacet, error) {
	placeholderPattern := regexp.MustCompile(`\{link:(\d+)\}`)
	matches := placeholderPattern.FindAllStringSubmatch(text, -1)

	for _, match := range matches {
		if len(match) != 2 {
			continue
		}

		index, err := strconv.Atoi(match[1])
		if err != nil || index >= len(urls) {
			continue
		}

		// Truncate if necessary
		truncatedText := texts[index]
		if len(truncatedText) > 25 {
			truncatedText = truncatedText[:22] + "..."
		}

		// Create the facet
		startIdx := int64(strings.Index(text, match[0]))
		endIdx := startIdx + int64(len(truncatedText)) + 1

		facet := &appbsky.RichtextFacet{
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Link: &appbsky.RichtextFacet_Link{
					Uri: urls[index],
				},
			}},
			Index: &appbsky.RichtextFacet_ByteSlice{
				ByteStart: startIdx,
				ByteEnd:   endIdx,
			},
		}

		facets = append(facets, facet)

		// Replace the placeholder with the truncated link in the text
		text = strings.Replace(text, match[0], truncatedText, 1)
	}

	return text, facets, nil
}
