package main

import (
	"context"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/backfill"
	typegen "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

func main() {
	mem := backfill.NewMemstore()

	rawLog, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	logger = rawLog.Sugar()

	bf := backfill.NewBackfiller(
		"backfill-test",
		mem,
		handleCreate,
		handleDelete,
		10,
		100,
		"app.bsky.feed.post",
		logger,
		2,
		"https://bsky.social/xrpc/com.atproto.sync.getCheckout")

	logger.Info("starting backfiller")

	go bf.Start()

	mem.EnqueueJob("did:plc:q6gjnaw2blty4crticxkmujt")
	// mem.EnqueueJob("did:plc:f5f4diimystr7ima7nqvamhe")
	// mem.EnqueueJob("did:plc:t7y4sud4dhptvzz7ibnv5cbt")

	time.Sleep(time.Second * 30)

	bf.Stop()

	logger.Infof("shutting down")
}

func handleCreate(ctx context.Context, repo string, path string, rec typegen.CBORMarshaler) error {
	logger.Infof("got create: %s %s\n", repo, path)
	return nil
}
func handleDelete(ctx context.Context, repo string, path string) error {
	logger.Infof("got delete: %s %s\n", repo, path)
	return nil
}
