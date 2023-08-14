package jazbot

import (
	"context"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

type Help struct {
	Name string
	Desc string
	Args []string
}

type Command interface {
	Name() string
	Help() *Help
	Startup() error
	Shutdown() error
	Execute(
		ctx context.Context,
		actorDid string,
		args []string,
		parentURI string,
	) (
		resp string,
		facets []*appbsky.RichtextFacet,
		cb *func(*comatproto.RepoCreateRecord_Output, error) error,
		err error,
	)
}
