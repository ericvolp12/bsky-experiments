package events

import (
	"context"
	"fmt"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
)

type TimeoutError struct {
	error
}

func FeedGetPostThreadWithTimeout(
	ctx context.Context,
	client *xrpc.Client,
	depth int64,
	uri string,
	timeout time.Duration,
) (*appbsky.FeedGetPostThread_Output, error) {
	type result struct {
		thread *appbsky.FeedGetPostThread_Output
		err    error
	}

	resultChan := make(chan result, 1)

	go func() {
		thread, err := appbsky.FeedGetPostThread(ctx, client, depth, uri)
		resultChan <- result{thread: thread, err: err}
	}()

	select {
	case res := <-resultChan:
		return res.thread, res.err
	case <-time.After(timeout):
		return nil, TimeoutError{fmt.Errorf("FeedGetPostThread timed out after %v", timeout)}
	}
}
