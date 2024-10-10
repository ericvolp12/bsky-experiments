package rss

import (
	"context"
	"fmt"
	"sync"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"
	"golang.org/x/time/rate"
)

// Client is a Bluesky API Client
type Client struct {
	PDSHost  string
	ActorDID syntax.DID

	xrpcc     *xrpc.Client
	clientMux *sync.RWMutex
	dir       identity.Directory
	rl        *rate.Limiter
}

func (fc *FeedConsumer) NewBskyClient(ctx context.Context, rawDid, appPassword string) (*Client, error) {
	did, err := syntax.ParseDID(rawDid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse did: %w", err)
	}

	// Figure out what PDS the user is on
	ident, err := fc.dir.LookupDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup did: %w", err)
	}

	ua := fmt.Sprintf("skypub/%s", "0.0.1")

	c := &Client{
		PDSHost:  ident.PDSEndpoint(),
		ActorDID: did,
		xrpcc: &xrpc.Client{
			Client:    util.RobustHTTPClient(),
			UserAgent: &ua,
		},
		clientMux: &sync.RWMutex{},
		dir:       fc.dir,
		rl:        rate.NewLimiter(rate.Limit(5), 1),
	}

	c.PDSHost = ident.PDSEndpoint()
	c.xrpcc.Host = c.PDSHost
	c.ActorDID = did

	if c.rl != nil {
		c.rl.Wait(ctx)
	}
	ses, err := comatproto.ServerCreateSession(ctx, c.xrpcc, &comatproto.ServerCreateSession_Input{
		Identifier: rawDid,
		Password:   appPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	c.xrpcc.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	return c, nil
}

func (fc *FeedConsumer) ResumeClient(ctx context.Context, rawDid, accessJwt, refreshJwt string) (*Client, error) {
	did, err := syntax.ParseDID(rawDid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse did: %w", err)
	}

	ident, err := fc.dir.LookupDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup did: %w", err)
	}

	ua := fmt.Sprintf("skypub/%s", "0.0.1")

	c := &Client{
		PDSHost:  ident.PDSEndpoint(),
		ActorDID: did,
		xrpcc: &xrpc.Client{
			Client:    util.RobustHTTPClient(),
			UserAgent: &ua,
		},
		clientMux: &sync.RWMutex{},
		dir:       fc.dir,
		rl:        rate.NewLimiter(rate.Limit(5), 1),
	}

	c.PDSHost = ident.PDSEndpoint()
	c.xrpcc.Host = c.PDSHost
	c.ActorDID = did

	c.xrpcc.Auth = &xrpc.AuthInfo{
		Did:        rawDid,
		RefreshJwt: refreshJwt,
		AccessJwt:  accessJwt,
	}

	return c, nil
}

// RefreshAuth refreshes the auth token for the client
func (c *Client) RefreshAuth(ctx context.Context) error {
	c.clientMux.Lock()
	defer c.clientMux.Unlock()

	c.xrpcc.Auth.AccessJwt = c.xrpcc.Auth.RefreshJwt

	if c.rl != nil {
		c.rl.Wait(ctx)
	}
	refreshedSession, err := comatproto.ServerRefreshSession(ctx, c.xrpcc)
	if err != nil {
		return fmt.Errorf("failed to refresh session: %w", err)
	}

	c.xrpcc.Auth = &xrpc.AuthInfo{
		Handle:     refreshedSession.Handle,
		Did:        refreshedSession.Did,
		RefreshJwt: refreshedSession.RefreshJwt,
		AccessJwt:  refreshedSession.AccessJwt,
	}

	return nil
}
