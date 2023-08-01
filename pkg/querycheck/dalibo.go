package querycheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type DaliboPayload struct {
	Title string `json:"title"`
	Plan  string `json:"plan"`
	Query string `json:"query"`
}

type DaliboResponse struct {
	DeleteKey string `json:"deleteKey"`
	ID        string `json:"id"`
}

var daliboURL = "https://explain.dalibo.com/new.json"

func (q *Query) SendToDalibo(ctx context.Context) (string, string, error) {
	ctx, span := tracer.Start(ctx, "SendToDalibo")
	defer span.End()

	plans := QueryPlans{*q.LatestPlan}
	jsonPlan, err := json.Marshal(plans)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal plan: %w", err)
	}

	q.lk.RLock()
	payload := DaliboPayload{
		Title: "Querycheck: " + q.Name + " @ " + q.LastChecked.Format("2006-01-02 15:04:05"),
		Plan:  string(jsonPlan),
		Query: q.Query,
	}
	q.lk.RUnlock()

	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	marshalledPayload, err := json.Marshal(payload)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", daliboURL, bytes.NewReader(marshalledPayload))
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to send request: %w", err)
	}

	defer resp.Body.Close()

	var daliboResp DaliboResponse
	err = json.NewDecoder(resp.Body).Decode(&daliboResp)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode response: %w", err)
	}

	return daliboResp.ID, daliboResp.DeleteKey, nil
}
