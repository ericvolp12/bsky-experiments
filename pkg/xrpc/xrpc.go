package xrpc

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
)

func GetXRPCClient() (*xrpc.Client, error) {
	client := xrpc.Client{
		Client: &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		Host: "https://bsky.social",
	}

	ATPAuthString := os.Getenv("ATP_AUTH")
	if ATPAuthString == "" {
		return nil, fmt.Errorf("ATP_AUTH not set")
	}

	authParts := strings.Split(ATPAuthString, ":")
	if len(authParts) != 2 {
		return nil, fmt.Errorf("ATP_AUTH not set correctly: {email}:{app_password}")
	}

	ses, err := comatproto.ServerCreateSession(context.TODO(), &client, &comatproto.ServerCreateSession_Input{
		Identifier: authParts[0],
		Password:   authParts[1],
	})

	if err != nil {
		return nil, err
	}

	client.Auth = &xrpc.AuthInfo{
		Handle:     ses.Handle,
		Did:        ses.Did,
		RefreshJwt: ses.RefreshJwt,
		AccessJwt:  ses.AccessJwt,
	}

	return &client, nil
}
