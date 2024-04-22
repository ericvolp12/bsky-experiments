package graphd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RoaringBitmap/roaring"
	"github.com/puzpuzpuz/xsync/v3"
)

const (
	// FollowAction is the action to follow a user (for the pending queue)
	FollowAction int = iota
	// UnfollowAction is the action to unfollow a user (for the pending queue)
	UnfollowAction
)

type QueueItem struct {
	Action int
	Actor  uint32
	Target uint32
}

type Graph struct {
	g      *xsync.MapOf[uint32, *FollowMap]
	logger *slog.Logger

	utd   map[uint32]string
	utdLk sync.RWMutex

	dtu   map[string]uint32
	dtuLk sync.RWMutex

	uidNext uint32
	nextLk  sync.Mutex

	followCount *xsync.Counter
	userCount   *xsync.Counter

	pendingQueue chan *QueueItem
	isLoaded     bool
	loadLk       sync.Mutex

	// For Persisting the graph
	dbPath        string
	db            *sql.DB
	updatedActors *roaring.Bitmap
	updatedLk     sync.RWMutex
	syncInterval  time.Duration
	shutdown      chan chan struct{}
}

type FollowMap struct {
	followingBM *roaring.Bitmap
	followingLk sync.RWMutex

	followersBM *roaring.Bitmap
	followersLk sync.RWMutex
}

func NewGraph(dbPath string, syncInterval time.Duration, expectedNodeCount int, logger *slog.Logger) (*Graph, error) {
	// Open the database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set pragmas
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	if _, err := db.Exec(`PRAGMA synchronous=normal;`); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	// Create the tables if they don't exist
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS actors (
		uid INTEGER PRIMARY KEY,
		did TEXT NOT NULL,
		following BLOB NOT NULL,
		followers BLOB NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create actors table: %w", err)
	}

	logger = logger.With("module", "graph")

	g := Graph{
		g:             xsync.NewMapOfPresized[uint32, *FollowMap](expectedNodeCount),
		logger:        logger,
		followCount:   xsync.NewCounter(),
		userCount:     xsync.NewCounter(),
		utd:           map[uint32]string{},
		dtu:           map[string]uint32{},
		pendingQueue:  make(chan *QueueItem, 100_000),
		dbPath:        dbPath,
		db:            db,
		updatedActors: roaring.NewBitmap(),
		syncInterval:  syncInterval,
		shutdown:      make(chan chan struct{}),
	}

	// Kick off the sync loop
	go func() {
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()
		logger := g.logger.With("routine", "graph_sync")
		for {
			select {
			case finished := <-g.shutdown:
				logger.Info("flushing updates on exit")
				if err := g.FlushUpdates(context.Background()); err != nil {
					logger.Error("failed to flush updates on exit", "error", err)
				} else {
					logger.Info("successfully flushed updates on exit")
				}
				close(finished)
				return
			case <-ticker.C:
				if err := g.FlushUpdates(context.Background()); err != nil {
					logger.Error("failed to flush updates", "error", err)
				}
			}
		}
	}()

	return &g, nil
}

func (g *Graph) Shutdown() {
	g.logger.Info("shutting down graph")
	finished := make(chan struct{})
	g.shutdown <- finished
	<-finished
	g.logger.Info("graph shut down")
}

func (g *Graph) IsLoaded() bool {
	g.loadLk.Lock()
	defer g.loadLk.Unlock()
	return g.isLoaded
}

func (g *Graph) GetUsercount() uint32 {
	return uint32(g.userCount.Value())
}

func (g *Graph) GetFollowcount() uint32 {
	return uint32(g.followCount.Value())
}

func (g *Graph) GetDID(uid uint32) (string, bool) {
	g.utdLk.RLock()
	defer g.utdLk.RUnlock()
	did, ok := g.utd[uid]
	return did, ok
}

func (g *Graph) GetDIDs(uids []uint32) ([]string, error) {
	g.utdLk.RLock()
	defer g.utdLk.RUnlock()
	dids := make([]string, len(uids))
	for i, uid := range uids {
		did, ok := g.utd[uid]
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		dids[i] = did
	}
	return dids, nil
}

func (g *Graph) GetUID(did string) (uint32, bool) {
	g.dtuLk.RLock()
	defer g.dtuLk.RUnlock()
	uid, ok := g.dtu[did]
	return uid, ok
}

func (g *Graph) GetUIDs(dids []string) ([]uint32, error) {
	g.dtuLk.RLock()
	defer g.dtuLk.RUnlock()
	uids := make([]uint32, len(dids))
	for i, did := range dids {
		uid, ok := g.dtu[did]
		if !ok {
			return nil, fmt.Errorf("did %s not found", did)
		}
		uids[i] = uid
	}
	return uids, nil
}

func (g *Graph) setUID(did string, uid uint32) {
	g.dtuLk.Lock()
	defer g.dtuLk.Unlock()
	g.dtu[did] = uid
}

func (g *Graph) setDID(uid uint32, did string) {
	g.utdLk.Lock()
	defer g.utdLk.Unlock()
	g.utd[uid] = did
}

// AcquireDID links a DID to a UID, creating a new UID if necessary.
// If the DID is already linked to a UID, that UID is returned
func (g *Graph) AcquireDID(did string) uint32 {
	g.nextLk.Lock()
	defer g.nextLk.Unlock()

	uid, ok := g.GetUID(did)
	if ok {
		return uid
	}

	uid = g.uidNext
	g.setUID(did, uid)
	g.setDID(uid, did)

	// Initialize the follow maps
	initMap := &FollowMap{
		followingLk: sync.RWMutex{},
		followersLk: sync.RWMutex{},
		followingBM: roaring.NewBitmap(),
		followersBM: roaring.NewBitmap(),
	}
	g.g.Store(uid, initMap)

	g.userCount.Add(1)

	g.uidNext++

	return uid
}

func (g *Graph) AddFollow(actorUID, targetUID uint32) {
	if g.IsLoaded() {
		g.addFollow(actorUID, targetUID)
		return
	}
	g.pendingQueue <- &QueueItem{Action: FollowAction, Actor: actorUID, Target: targetUID}
	return
}

func (g *Graph) addFollow(actorUID, targetUID uint32) {
	actorMap, _ := g.g.Load(actorUID)
	actorMap.followingLk.Lock()
	actorMap.followingBM.Add(targetUID)
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Add(actorUID)
	targetMap.followersLk.Unlock()

	g.followCount.Inc()

	g.updatedLk.Lock()
	g.updatedActors.Add(actorUID)
	g.updatedActors.Add(targetUID)
	g.updatedLk.Unlock()
}

func (g *Graph) RemoveFollow(actorUID, targetUID uint32) {
	if g.IsLoaded() {
		g.removeFollow(actorUID, targetUID)
		return
	}
	g.pendingQueue <- &QueueItem{Action: UnfollowAction, Actor: actorUID, Target: targetUID}
	return
}

func (g *Graph) removeFollow(actorUID, targetUID uint32) {
	actorMap, _ := g.g.Load(actorUID)
	actorMap.followingLk.Lock()
	actorMap.followingBM.Remove(targetUID)
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Remove(actorUID)
	targetMap.followersLk.Unlock()

	g.followCount.Dec()

	g.updatedLk.Lock()
	g.updatedActors.Add(actorUID)
	g.updatedActors.Add(targetUID)
	g.updatedLk.Unlock()
}

func (g *Graph) GetFollowers(uid uint32) ([]uint32, error) {
	followMap, ok := g.g.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.followersLk.RLock()
	defer followMap.followersLk.RUnlock()

	return followMap.followersBM.ToArray(), nil
}

func (g *Graph) GetFollowing(uid uint32) ([]uint32, error) {
	followMap, ok := g.g.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.followingLk.RLock()
	defer followMap.followingLk.RUnlock()

	return followMap.followingBM.ToArray(), nil
}

func (g *Graph) GetMoots(uid uint32) ([]uint32, error) {
	followMap, ok := g.g.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}

	followMap.followingLk.RLock()
	followMap.followersLk.RLock()
	defer followMap.followingLk.RUnlock()
	defer followMap.followersLk.RUnlock()

	mootMap := roaring.ParAnd(4, followMap.followingBM, followMap.followersBM)

	return mootMap.ToArray(), nil
}

func (g *Graph) IntersectFollowers(uids []uint32) ([]uint32, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowers(uids[0])
	}

	followerMaps := make([]*roaring.Bitmap, len(uids))
	for i, uid := range uids {
		followMap, ok := g.g.Load(uid)
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		followMap.followersLk.RLock()
		defer followMap.followersLk.RUnlock()
		followerMaps[i] = followMap.followersBM
	}

	intersectMap := roaring.ParAnd(4, followerMaps...)

	return intersectMap.ToArray(), nil
}

func (g *Graph) IntersectFollowing(uids []uint32) ([]uint32, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowing(uids[0])
	}

	followingMaps := make([]*roaring.Bitmap, len(uids))
	for i, uid := range uids {
		followMap, ok := g.g.Load(uid)
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		followMap.followingLk.RLock()
		defer followMap.followingLk.RUnlock()
		followingMaps[i] = followMap.followingBM
	}

	intersectMap := roaring.ParAnd(4, followingMaps...)

	return intersectMap.ToArray(), nil
}

// GetFollowersNotFollowing returns a list of followers of the given UID that
// the given UID is not following
func (g *Graph) GetFollowersNotFollowing(uid uint32) ([]uint32, error) {
	followMap, ok := g.g.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}

	followMap.followersLk.RLock()
	followMap.followingLk.RLock()
	defer followMap.followersLk.RUnlock()
	defer followMap.followingLk.RUnlock()

	notFollowingMap := roaring.AndNot(followMap.followersBM, followMap.followingBM)

	return notFollowingMap.ToArray(), nil

}

// IntersectFollowingAndFollowers returns the intersection of the following of the actorUID
// and the followers of the targetUID
func (g *Graph) IntersectFollowingAndFollowers(actorUID, targetUID uint32) ([]uint32, error) {
	actorMap, ok := g.g.Load(actorUID)
	if !ok {
		return nil, fmt.Errorf("actor uid %d not found", actorUID)
	}

	targetMap, ok := g.g.Load(targetUID)
	if !ok {
		return nil, fmt.Errorf("target uid %d not found", targetUID)
	}

	actorMap.followingLk.RLock()
	targetMap.followersLk.RLock()
	defer actorMap.followingLk.RUnlock()
	defer targetMap.followersLk.RUnlock()

	intersectMap := roaring.ParAnd(4, actorMap.followingBM, targetMap.followersBM)

	return intersectMap.ToArray(), nil
}

func (g *Graph) DoesFollow(actorUID, targetUID uint32) (bool, error) {
	actorMap, ok := g.g.Load(actorUID)
	if !ok {
		return false, fmt.Errorf("actor uid %d not found", actorUID)
	}

	actorMap.followingLk.RLock()
	defer actorMap.followingLk.RUnlock()

	return actorMap.followingBM.Contains(targetUID), nil
}
