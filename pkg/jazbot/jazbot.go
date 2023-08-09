package jazbot

import (
	"context"
	"fmt"
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
}

var SupportedCommands = []string{
	"help",
	"getlikecount",
	"getlikesgiven",
}

func NewJazbot(ctx context.Context, store *store.Store, botDid string) (*Jazbot, error) {
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

	return &j, nil
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

	// Check if the user is following the bot
	following, err := j.Store.Queries.CountFollowsByActorAndTarget(ctx, store_queries.CountFollowsByActorAndTargetParams{
		ActorDid:  actorDid,
		TargetDid: j.BotDid,
	})
	if err != nil {
		failedCommandsReceivedCounter.WithLabelValues("follow_check_failed").Inc()
		return fmt.Errorf("failed to check if user (%s) is following bot (%s): %+v", actorDid, j.BotDid, err)
	}

	if following == 0 {
		failedCommandsReceivedCounter.WithLabelValues("not_following").Inc()
		return fmt.Errorf("user (%s) is not following bot (%s)", actorDid, j.BotDid)
	}

	p := message.NewPrinter(language.English)

	resp := ""
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
			resp = p.Sprintf("I currently support the following commands:\n%s", strings.Join(SupportedCommands, "\n"))
		case "getlikecount":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			likeCount, err := j.Store.Queries.GetTotalLikesReceivedByActor(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get like count for user (%s): %+v", actorDid, err)
				resp = fmt.Sprintf("I had trouble getting your like count, please try again later")
				break
			}

			resp = p.Sprintf("You have received a total of %d likes", likeCount)
		case "getlikesgiven":
			validCommandsReceivedCounter.WithLabelValues(command).Inc()
			likeCount, err := j.Store.Queries.GetTotalLikesGivenByActor(ctx, actorDid)
			if err != nil {
				j.Logger.Errorf("failed to get like count for user (%s): %+v", actorDid, err)
				resp = fmt.Sprintf("I had trouble getting your like count, please try again later")
				break
			}

			resp = p.Sprintf("You have given a total of %d likes", likeCount)
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
