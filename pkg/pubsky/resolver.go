package pubsky

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
)

type plcMirrorResponse struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
	Error  string `json:"error"`
}

var ActorNotFound = fmt.Errorf("actor not found")

func ResolveHandleOrDid(ctx context.Context, mirror, handleOrDid string) (did string, handle string, err error) {
	ctx, span := tracer.Start(ctx, "ResolveHandleOrDid")
	defer span.End()

	// HTTP GET to http[s]://{mirror}/{handleOrDid}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", mirror, handleOrDid), nil)
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return did, handle, fmt.Errorf("error creating request for %s: %w", handleOrDid, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return did, handle, fmt.Errorf("error getting did for %s: %w", handleOrDid, err)
	}
	defer resp.Body.Close()

	// Read the response body into a mirrorResponse
	mirrorResponse := plcMirrorResponse{}
	err = json.NewDecoder(resp.Body).Decode(&mirrorResponse)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return did, handle, fmt.Errorf("error decoding response body for %s: %w", handleOrDid, err)
	}

	// If the didLookup has a handle, return it
	if mirrorResponse.Did != "" && mirrorResponse.Handle != "" {
		did = mirrorResponse.Did
		handle = mirrorResponse.Handle
		span.SetAttributes(attribute.String("did", did))
		span.SetAttributes(attribute.String("handle", handle))
	} else if mirrorResponse.Error != "" {
		span.SetAttributes(attribute.String("error", mirrorResponse.Error))
		if mirrorResponse.Error == "redis: nil" {
			return did, handle, ActorNotFound
		}
		return did, handle, fmt.Errorf("error getting did for %s: %s", handleOrDid, mirrorResponse.Error)
	} else {
		span.SetAttributes(attribute.String("error", "no did or handle"))
		return did, handle, ActorNotFound
	}

	return did, handle, nil
}

type didLookup struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
}

func BatchResolveDids(ctx context.Context, mirror string, dids []string) ([]didLookup, error) {
	ctx, span := tracer.Start(ctx, "BatchResolveHandlesOrDids")
	defer span.End()

	didsAsBytes, err := json.Marshal(dids)
	if err != nil {
		span.SetAttributes(attribute.String("dids.marshal.error", err.Error()))
		return nil, fmt.Errorf("error marshaling dids: %w", err)
	}

	// HTTP GET to http[s]://{mirror}/{handleOrDid}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/batch/by_did", mirror), bytes.NewReader(didsAsBytes))
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return nil, fmt.Errorf("error creating request for %s: %w", dids, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return nil, fmt.Errorf("error getting did for %s: %w", dids, err)
	}
	defer resp.Body.Close()

	// Read the response body into a mirrorResponse
	mirrorResponse := []plcMirrorResponse{}
	err = json.NewDecoder(resp.Body).Decode(&mirrorResponse)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return nil, fmt.Errorf("error decoding response body for %s: %w", dids, err)
	}

	lookups := []didLookup{}

	// Iterate over the mirrorResponse and append the handles to the handles slice
	for _, response := range mirrorResponse {
		lookups = append(lookups, didLookup{
			Did:    response.Did,
			Handle: response.Handle,
		})
	}

	return lookups, nil
}
