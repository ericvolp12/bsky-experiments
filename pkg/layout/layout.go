package layout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type Edge [2]int

type EdgeListRequest struct {
	Edges      []Edge  `json:"edges"`
	Ka         float32 `json:"ka"`
	Kg         float32 `json:"kg"`
	Kr         float32 `json:"kr"`
	NbNodes    int     `json:"nb_nodes"`
	Iterations int     `json:"iterations"`
	ChunkSize  int     `json:"chunk_size"`
}

type ResponsePoints struct {
	Points [][]float32 `json:"points"`
}

func SendEdgeListRequest(ctx context.Context, serviceURL string, edges []Edge) ([][]float32, error) {
	tracer := otel.Tracer("layout")
	ctx, span := tracer.Start(ctx, "SendEdgeListRequest")
	defer span.End()

	request := EdgeListRequest{
		Edges:      edges,
		Ka:         1,
		Kg:         1,
		Kr:         1,
		NbNodes:    len(edges) + 1,
		Iterations: 600,
		ChunkSize:  16,
	}

	span.SetAttributes(attribute.Int("edges.count", len(edges)))
	span.SetAttributes(attribute.Int("iterations.count", request.Iterations))

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}

	rustServicePath, err := url.Parse(fmt.Sprintf("%s/fa2_layout", serviceURL))
	if err != nil {
		return nil, fmt.Errorf("error parsing Rust service URL: %w", err)
	}
	req := http.Request{
		Method: "POST",
		URL:    rustServicePath,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: ioutil.NopCloser(bytes.NewBuffer(requestJSON)),
	}

	resp, err := otelhttp.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("error sending request to Rust service: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	var responsePoints ResponsePoints
	err = json.Unmarshal(body, &responsePoints)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling response points: %w", err)
	}

	return responsePoints.Points, nil
}
