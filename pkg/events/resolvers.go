package events

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
)

// PostCacheEntry is a struct that holds a PostView and an expiration time
type PostCacheEntry struct {
	Post         *bsky.FeedDefs_PostView
	TimeoutCount int
}

// RefreshAuthToken refreshes the auth token for the client
func (bsky *BSky) RefreshAuthToken(ctx context.Context, workerID int) error {
	worker := bsky.Workers[workerID]
	ctx, span := tracer.Start(ctx, "RefreshAuthToken")
	defer span.End()
	err := intXRPC.RefreshAuth(ctx, worker.Client, &worker.ClientMux)
	return err
}

type didLookup struct {
	Did                 string `json:"did"`
	VerificationMethods struct {
		Atproto string `json:"atproto"`
	} `json:"verificationMethods"`
	RotationKeys []string `json:"rotationKeys"`
	AlsoKnownAs  []string `json:"alsoKnownAs"`
	Services     struct {
		AtprotoPds struct {
			Type     string `json:"type"`
			Endpoint string `json:"endpoint"`
		} `json:"atproto_pds"`
	} `json:"services"`
}

func (bsky *BSky) getHandleFromDirectory(ctx context.Context, did string) (handle string, err error) {
	ctx, span := tracer.Start(ctx, "getHandleFromDirectory")
	defer span.End()

	span.AddEvent("waiting for rate limiter")
	// Use rate limiter before each request
	err = bsky.directoryLimiter.Wait(ctx)
	span.AddEvent("rate limiter finished")
	if err != nil {
		span.SetAttributes(attribute.String("rate.limiter.error", err.Error()))
		return handle, fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	start := time.Now()

	// HTTP GET to https://plc.directory/{did}/data
	req, err := http.NewRequest("GET", fmt.Sprintf("https://plc.directory/%s/data", did), nil)
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return handle, fmt.Errorf("error creating request for %s: %w", did, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return handle, fmt.Errorf("error getting handle for %s: %w", did, err)
	}
	defer resp.Body.Close()

	// Read the response body into a didLookup
	didLookup := didLookup{}
	err = json.NewDecoder(resp.Body).Decode(&didLookup)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return handle, fmt.Errorf("error decoding response body for %s: %w", did, err)
	}

	// Record the duration of the request
	apiCallDurationHistogram.WithLabelValues("LookupDID").Observe(time.Since(start).Seconds())

	// If the didLookup has a handle, return it
	if len(didLookup.AlsoKnownAs) > 0 {
		// Handles from the DID service look like: "at://jaz.bsky.social", remove the "at://" prefix
		handle = strings.TrimPrefix(didLookup.AlsoKnownAs[0], "at://")
		span.SetAttributes(attribute.String("handle", handle))
	}

	return handle, nil
}

type plcMirrorResponse struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
	Error  string `json:"error"`
}

func (bsky *BSky) getHandleFromPLCMirror(ctx context.Context, did string) (handle string, err error) {
	ctx, span := tracer.Start(ctx, "getHandleFromPLCMirror")
	defer span.End()

	start := time.Now()

	// HTTP GET to http[s]://{mirror_root}/{did}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", bsky.PLCMirrorRoot, did), nil)
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return handle, fmt.Errorf("error creating request for %s: %w", did, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return handle, fmt.Errorf("error getting handle for %s: %w", did, err)
	}
	defer resp.Body.Close()

	// Read the response body into a mirrorResponse
	mirrorResponse := plcMirrorResponse{}
	err = json.NewDecoder(resp.Body).Decode(&mirrorResponse)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return handle, fmt.Errorf("error decoding response body for %s: %w", did, err)
	}

	// Record the duration of the request
	apiCallDurationHistogram.WithLabelValues("MirrorLookupDID").Observe(time.Since(start).Seconds())

	// If the didLookup has a handle, return it
	if mirrorResponse.Handle != "" {
		handle = mirrorResponse.Handle
		span.SetAttributes(attribute.String("handle", handle))
	}

	return handle, nil
}

func (bsky *BSky) ResolveDID(ctx context.Context, did string) (handle string, err error) {
	ctx, span := tracer.Start(ctx, "ResolveDID")
	defer span.End()

	// Get the handle from the DID service (PLC Mirror)
	handle, err = bsky.getHandleFromPLCMirror(ctx, did)
	if err != nil {
		span.SetAttributes(attribute.String("did.get_from_mirror.error", err.Error()))
		// Try the directory
		handle, err = bsky.getHandleFromDirectory(ctx, did)
		if err != nil {
			span.SetAttributes(attribute.String("did.get_from_directory.error", err.Error()))
			return handle, fmt.Errorf("error getting handle for %s: %w", did, err)
		}
	}

	return handle, nil
}
