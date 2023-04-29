package events

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	lru "github.com/hashicorp/golang-lru"
)

// RepoRecord holds data needed for processing a RepoRecord
type RepoRecord struct {
	ctx context.Context
	rr  *repo.Repo
	op  *comatproto.SyncSubscribeRepos_RepoOp
	evt *comatproto.SyncSubscribeRepos_Commit
}

// BSky is a struct that holds the state of the social graph and the
// authenticated XRPC client
type BSky struct {
	IncludeLinks bool

	SocialGraph    graph.Graph
	SocialGraphMux sync.RWMutex

	// Generate a Profile Cache with a TTL
	profileCache    *lru.ARCCache
	profileCacheTTL time.Duration

	// Generate a Thread Cache with a TTL
	threadCache    *lru.ARCCache
	threadCacheTTL time.Duration

	RepoRecordQueue chan RepoRecord

	WorkerCount int
	Workers     []*Worker
}

// NewBSky creates a new BSky struct with an authenticated XRPC client
// and a social graph, initializing mutexes for cross-routine access
func NewBSky(ctx context.Context, includeLinks bool, workerCount int) (*BSky, error) {
	threadCache, err := lru.NewARC(5000)
	if err != nil {
		return nil, err
	}

	profileCache, err := lru.NewARC(15000)
	if err != nil {
		return nil, err
	}

	bsky := &BSky{
		IncludeLinks: includeLinks,

		SocialGraph:    graph.NewGraph(),
		SocialGraphMux: sync.RWMutex{},

		profileCache: profileCache,
		threadCache:  threadCache,

		// 60 minute Cache TTLs
		profileCacheTTL: time.Minute * 60,
		threadCacheTTL:  time.Minute * 60,

		RepoRecordQueue: make(chan RepoRecord, 100),
		WorkerCount:     workerCount,
		Workers:         make([]*Worker, workerCount),
	}

	// Initialize the workers, each with their own BSky Client and Mutex
	// Workers share a single WorkQueue and SocialGraph/Mutex
	for i := 0; i < workerCount; i++ {
		client, err := intXRPC.GetXRPCClient(ctx)
		if err != nil {
			return nil, err
		}

		bsky.Workers[i] = &Worker{
			WorkerID:  i,
			Client:    client,
			ClientMux: sync.RWMutex{},
		}

		go bsky.worker(i)
	}

	return bsky, nil
}

// HandleRepoCommit is called when a repo commit is received and prints its contents
func (bsky *BSky) HandleRepoCommit(evt *comatproto.SyncSubscribeRepos_Commit) error {
	ctx := context.Background()
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		fmt.Println(err)
	} else {
		for _, op := range evt.Ops {
			ek := repomgr.EventKind(op.Action)
			switch ek {
			case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
				bsky.RepoRecordQueue <- RepoRecord{
					ctx: ctx,
					rr:  rr,
					op:  op,
					evt: evt,
				}
			}
		}
	}
	return nil
}

func HandleRepoInfo(info *comatproto.SyncSubscribeRepos_Info) error {
	return nil
}

func HandleError(errf *events.ErrorFrame) error {
	return nil
}
