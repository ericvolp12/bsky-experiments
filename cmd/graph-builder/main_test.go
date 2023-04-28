package main

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestGetNextBackoff(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	testCases := []struct {
		name       string
		current    time.Duration
		minBackoff time.Duration
		maxBackoff time.Duration
	}{
		{
			name:       "Initial backoff",
			current:    0,
			minBackoff: time.Second,
			maxBackoff: time.Second,
		},
		{
			name:       "Exponential backoff",
			current:    2 * time.Second,
			minBackoff: 2 * time.Second,
			maxBackoff: 6 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backoff := getNextBackoff(tc.current)
			assert.GreaterOrEqual(t, int64(backoff), int64(tc.minBackoff))
			assert.LessOrEqual(t, int64(backoff), int64(tc.maxBackoff))
		})
	}
}

type mockRepoStream struct {
	handleFunc func(context.Context, *websocket.Conn, *events.RepoStreamCallbacks) error
}

func (m *mockRepoStream) HandleRepoStream(ctx context.Context, c *websocket.Conn, callbacks *events.RepoStreamCallbacks) error {
	return m.handleFunc(ctx, c, callbacks)
}
