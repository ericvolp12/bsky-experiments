package jazbot

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

func (j *Jazbot) GetPDS(ctx context.Context, actorDid string) (string, error) {
	ctx, span := tracer.Start(ctx, "GetPDS")
	defer span.End()

	resp := ""

	err := j.Directory.Purge(ctx, syntax.DID(actorDid).AtIdentifier())
	if err != nil {
		resp = "I had trouble getting your PDS 😢\nPlease try again later!"
		return resp, fmt.Errorf("failed to purge DID (%s): %+v", actorDid, err)
	}

	ident, err := j.Directory.LookupDID(ctx, syntax.DID(actorDid))
	if err != nil {
		resp = "I had trouble getting your PDS 😢\nPlease try again later!"
		return resp, fmt.Errorf("failed to get PDS for user (%s): %+v", actorDid, err)
	}

	pds := ""
	for _, service := range ident.Services {
		if service.Type == "AtprotoPersonalDataServer" {
			u, err := url.Parse(service.URL)
			if err != nil {
				resp = "I had trouble getting your PDS 😢\nPlease try again later!"
				return resp, fmt.Errorf("failed to parse PDS URL (%s) for user (%s): %+v", service.URL, actorDid, err)
			}
			pds = u.Hostname()
		}
	}

	resp = fmt.Sprintf("Your PDS is: %s!", pds)
	if strings.HasSuffix(pds, ".us-east.host.bsky.network") {
		resp += "\nYou're one of us! Welcome to the Mycosphere!🍄🍄🍄"
	} else if pds == "bsky.social" {
		resp += "\nDon't worry! The Great Federation Rapture will soon visit you and you'll join the rest of us in the big Mycosphere in the sky!😇😇😇"
	} else {
		resp += "\nI hope you're enjoying your time there!🌎🌍🌏"
	}

	return resp, nil
}
