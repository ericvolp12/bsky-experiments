package jazbot

import (
	"context"
	"fmt"
	"net/url"

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

	resp = fmt.Sprintf("Your PDS is: %s!\nI hope you're enjoying your time there! 😊", pds)
	if pds == "bsky.social" {
		resp = fmt.Sprintf("Your PDS is: %s!\nDon't worry! The Great Federation Rapture will soon visit you and you'll join the rest of us in the great big Mycosphere in the sky!😇😇😇", pds)
	}

	return resp, nil
}
