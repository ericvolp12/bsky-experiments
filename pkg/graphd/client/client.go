package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
)

type Client struct {
	APIRoot string
	httpC   *http.Client
}

var tracer = otel.Tracer("graphd_client")

func NewClient(apiRoot string, httpClient *http.Client) *Client {
	return &Client{
		APIRoot: apiRoot,
		httpC:   httpClient,
	}
}

type Follow struct {
	ActorDid  string `json:"actorDid"`
	TargetDid string `json:"targetDid"`
}

func (c *Client) Follow(ctx context.Context, actorDid string, targetDid string) error {
	ctx, span := tracer.Start(ctx, "Follow")
	defer span.End()

	follow := Follow{
		ActorDid:  actorDid,
		TargetDid: targetDid,
	}

	body := new(bytes.Buffer)
	err := json.NewEncoder(body).Encode(follow)
	if err != nil {
		return fmt.Errorf("failed to encode follow: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.APIRoot+"/follow", body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpC.Do(req)
	if err != nil {
		return fmt.Errorf("failed to do request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		text := new(bytes.Buffer)
		_, err := text.ReadFrom(resp.Body)
		if err == nil {
			return fmt.Errorf("got status code %d: %s", resp.StatusCode, text.String())
		}
		return fmt.Errorf("got status code %d", resp.StatusCode)
	}

	return nil
}

type Unfollow struct {
	ActorDid  string `json:"actorDid"`
	TargetDid string `json:"targetDid"`
}

func (c *Client) Unfollow(ctx context.Context, actorDid string, targetDid string) error {
	ctx, span := tracer.Start(ctx, "Unfollow")
	defer span.End()

	unfollow := Unfollow{
		ActorDid:  actorDid,
		TargetDid: targetDid,
	}

	body := new(bytes.Buffer)
	err := json.NewEncoder(body).Encode(unfollow)
	if err != nil {
		return fmt.Errorf("failed to encode unfollow: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.APIRoot+"/unfollow", body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpC.Do(req)
	if err != nil {
		return fmt.Errorf("failed to do request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		text := new(bytes.Buffer)
		_, err := text.ReadFrom(resp.Body)
		if err == nil {
			return fmt.Errorf("got status code %d: %s", resp.StatusCode, text.String())
		}
		return fmt.Errorf("got status code %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) GetFollowers(ctx context.Context, did string) ([]string, error) {
	ctx, span := tracer.Start(ctx, "GetFollowers")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, "GET", c.APIRoot+"/followers?did="+did, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpC.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		text := new(bytes.Buffer)
		_, err := text.ReadFrom(resp.Body)
		if err == nil {
			return nil, fmt.Errorf("got status code %d: %s", resp.StatusCode, text.String())
		}
		return nil, fmt.Errorf("got status code %d", resp.StatusCode)
	}

	var followerDIDs []string
	err = json.NewDecoder(resp.Body).Decode(&followerDIDs)
	if err != nil {
		return nil, err
	}

	return followerDIDs, nil
}

func (c *Client) GetFollowersNotFollowing(ctx context.Context, did string) ([]string, error) {
	ctx, span := tracer.Start(ctx, "GetFollowersNotFollowing")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, "GET", c.APIRoot+"/followersNotFollowing?did="+did, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpC.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		text := new(bytes.Buffer)
		_, err := text.ReadFrom(resp.Body)
		if err == nil {
			return nil, fmt.Errorf("got status code %d: %s", resp.StatusCode, text.String())
		}
		return nil, fmt.Errorf("got status code %d", resp.StatusCode)
	}

	var followerDIDs []string
	err = json.NewDecoder(resp.Body).Decode(&followerDIDs)
	if err != nil {
		return nil, err
	}

	return followerDIDs, nil
}
