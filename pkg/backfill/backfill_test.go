package backfill_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/backfill"
	typegen "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

type testState struct {
	creates int
	updates int
	deletes int
	lk      sync.Mutex
}

func TestBackfill(t *testing.T) {
	ctx := context.Background()

	testRepos := []string{
		"did:plc:q6gjnaw2blty4crticxkmujt",
		"did:plc:f5f4diimystr7ima7nqvamhe",
		"did:plc:t7y4sud4dhptvzz7ibnv5cbt",
	}

	mem := backfill.NewMemstore()

	rawLog, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}

	logger = rawLog.Sugar()

	ts := &testState{}

	bf := backfill.NewBackfiller(
		"backfill-test",
		mem,
		ts.handleCreate,
		ts.handleUpdate,
		ts.handleDelete,
		10,
		100,
		"app.bsky.feed.follow/",
		logger,
		2,
		"https://bgs.bsky.social/xrpc/com.atproto.sync.getRepo")

	logger.Info("starting backfiller")

	go bf.Start()

	for _, repo := range testRepos {
		mem.EnqueueJob(repo)
	}

	// Wait until job 0 is in progress
	for {
		s, err := mem.GetJob(ctx, testRepos[0])
		if err != nil {
			t.Fatal(err)
		}
		if s.State() == backfill.StateInProgress {
			mem.BufferOp(ctx, testRepos[0], "delete", "app.bsky.feed.follow/1", nil)
			mem.BufferOp(ctx, testRepos[0], "delete", "app.bsky.feed.follow/2", nil)
			mem.BufferOp(ctx, testRepos[0], "delete", "app.bsky.feed.follow/3", nil)
			mem.BufferOp(ctx, testRepos[0], "delete", "app.bsky.feed.follow/4", nil)
			mem.BufferOp(ctx, testRepos[0], "delete", "app.bsky.feed.follow/5", nil)

			mem.BufferOp(ctx, testRepos[0], "create", "app.bsky.feed.follow/1", nil)

			mem.BufferOp(ctx, testRepos[0], "update", "app.bsky.feed.follow/1", nil)

			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	for {
		ts.lk.Lock()
		if ts.deletes >= 5 && ts.creates >= 1 && ts.updates >= 1 {
			ts.lk.Unlock()
			break
		}
		ts.lk.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	bf.Stop()

	logger.Infof("shutting down")
}

func (ts *testState) handleCreate(ctx context.Context, repo string, path string, rec *typegen.CBORMarshaler) error {
	logger.Infof("got create: %s %s", repo, path)
	ts.lk.Lock()
	ts.creates++
	ts.lk.Unlock()
	return nil
}

func (ts *testState) handleUpdate(ctx context.Context, repo string, path string, rec *typegen.CBORMarshaler) error {
	logger.Infof("got update: %s %s", repo, path)
	ts.lk.Lock()
	ts.updates++
	ts.lk.Unlock()
	return nil
}

func (ts *testState) handleDelete(ctx context.Context, repo string, path string) error {
	logger.Infof("got delete: %s %s", repo, path)
	ts.lk.Lock()
	ts.deletes++
	ts.lk.Unlock()
	return nil
}
