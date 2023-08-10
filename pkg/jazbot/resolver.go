package jazbot

import (
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

func GetHandleFromPLCMirror(ctx context.Context, mirror, did string) (handle string, err error) {
	ctx, span := tracer.Start(ctx, "GetHandleFromPLCMirror")
	defer span.End()

	// HTTP GET to http[s]://{mirror}/{did}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", mirror, did), nil)
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

	// If the didLookup has a handle, return it
	if mirrorResponse.Handle != "" {
		handle = mirrorResponse.Handle
		span.SetAttributes(attribute.String("handle", handle))
	}

	return handle, nil
}

func GetDIDFromPLCMirror(ctx context.Context, mirror, handle string) (did string, err error) {
	ctx, span := tracer.Start(ctx, "GetHandleFromPLCMirror")
	defer span.End()

	// HTTP GET to http[s]://{mirror}/{handle}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", mirror, handle), nil)
	if err != nil {
		span.SetAttributes(attribute.String("request.create.error", err.Error()))
		return did, fmt.Errorf("error creating request for %s: %w", handle, err)
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		span.SetAttributes(attribute.String("request.do.error", err.Error()))
		return did, fmt.Errorf("error getting did for %s: %w", handle, err)
	}
	defer resp.Body.Close()

	// Read the response body into a mirrorResponse
	mirrorResponse := plcMirrorResponse{}
	err = json.NewDecoder(resp.Body).Decode(&mirrorResponse)
	if err != nil {
		span.SetAttributes(attribute.String("response.decode.error", err.Error()))
		return did, fmt.Errorf("error decoding response body for %s: %w", handle, err)
	}

	// If the didLookup has a handle, return it
	if mirrorResponse.Did != "" {
		did = mirrorResponse.Did
		span.SetAttributes(attribute.String("did", did))
	}

	return did, nil
}
