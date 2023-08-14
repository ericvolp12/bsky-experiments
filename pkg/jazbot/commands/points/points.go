package points

import (
	"context"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot"
	"github.com/ericvolp12/bsky-experiments/pkg/jazbot/util"
)

type Command struct {
	name string
	desc string
	args []string

	Ctx *util.CommandCtx

	Store     *store.Store
	PLCMirror string
}

func NewCommand(
	ctx *util.CommandCtx,
	store *store.Store,
	plcMirror string,
) *Command {
	return &Command{
		name: "points",
		desc: "Get your current points.\nPoints can be acquired by participating in challenges.",
		Ctx:  ctx,

		Store:     store,
		PLCMirror: plcMirror,
	}
}

func (c *Command) Name() string {
	return c.name
}

func (c *Command) Help() *jazbot.Help {
	return &jazbot.Help{
		Name: c.name,
		Desc: c.desc,
		Args: c.args,
	}
}

func (c *Command) Startup() error {
	return nil
}

func (c *Command) Shutdown() error {
	return nil
}

func (c *Command) Execute(
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
}
