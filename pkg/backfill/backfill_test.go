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
		panic(err)
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
		"https://bsky.social/xrpc/com.atproto.sync.getCheckout")

	logger.Info("starting backfiller")

	go bf.Start()

	for _, repo := range testRepos {
		mem.EnqueueJob(repo)
	}

	job, err := mem.GetJob(ctx, testRepos[0])
	if err != nil {
		t.Fatal(err)
	}

	job.BufferOp(ctx, "delete", "app.bsky.feed.follow/1", nil)
	job.BufferOp(ctx, "delete", "app.bsky.feed.follow/2", nil)
	job.BufferOp(ctx, "delete", "app.bsky.feed.follow/3", nil)
	job.BufferOp(ctx, "delete", "app.bsky.feed.follow/4", nil)
	job.BufferOp(ctx, "delete", "app.bsky.feed.follow/5", nil)

	// Check if a create gets buffered when there is a pending delete
	shouldBuffer, err := job.ShouldBufferOp(ctx, "create", "app.bsky.feed.follow/1")
	if err != nil {
		t.Fatal(err)
	}
	if !shouldBuffer {
		t.Fatal("should buffer create after delete")
	}

	job.BufferOp(ctx, "create", "app.bsky.feed.follow/1", nil)

	job.BufferOp(ctx, "update", "app.bsky.feed.follow/1", nil)

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
