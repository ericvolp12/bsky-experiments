package layout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
)

type ThreadViewLayout struct {
	Post         search.Post `json:"post"`
	AuthorHandle string      `json:"author_handle"`
	Depth        int         `json:"depth"`
	X            float32     `json:"x"`
	Y            float32     `json:"y"`
}

func SendEdgeListRequestTS(ctx context.Context, serviceURL string, postViews []search.PostView) ([]ThreadViewLayout, error) {
	tracer := otel.Tracer("layout")
	ctx, span := tracer.Start(ctx, "SendEdgeListRequestTS")
	defer span.End()

	requestJSON, err := json.Marshal(postViews)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}

	layoutServicePath, err := url.Parse(fmt.Sprintf("%s/fa2_layout", serviceURL))
	if err != nil {
		return nil, fmt.Errorf("error parsing layout service URL: %w", err)
	}
	req := http.Request{
		Method: "POST",
		URL:    layoutServicePath,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: ioutil.NopCloser(bytes.NewBuffer(requestJSON)),
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("error sending request to layout service: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	var responseViews []ThreadViewLayout
	err = json.Unmarshal(body, &responseViews)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling response views: %w", err)
	}

	return responseViews, nil
}
