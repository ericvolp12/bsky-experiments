package graph

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ericvolp12/bsky-experiments/pkg/graphd/bitmapper"
	_ "github.com/mattn/go-sqlite3"

	"github.com/RoaringBitmap/roaring"
)

type Graph struct {
	logger *slog.Logger

	isLoaded bool
	loadLk   sync.Mutex

	// For Persisting the graph
	dbPath   string
	shutdown chan chan struct{}

	bm        *bitmapper.Bitmapper
	following *bitmapper.Group
	followers *bitmapper.Group
}

var followingGKey = "following"
var followersGKey = "followers"

func NewGraph(ctx context.Context, dbPath string, logger *slog.Logger) (*Graph, error) {
	logger = logger.With("module", "graph")

	followingConfig := bitmapper.GroupConfig{
		Name:      followingGKey,
		ShardSize: 100_000,
		CacheSize: 100_000,
	}

	followersConfig := bitmapper.GroupConfig{
		Name:      followersGKey,
		ShardSize: 100_000,
		CacheSize: 100_000,
	}

	groupConfigs := []bitmapper.GroupConfig{followingConfig, followersConfig}
	bmConfig := bitmapper.BitmapperConfig{
		DBDir:  dbPath,
		Groups: groupConfigs,
	}

	bm, err := bitmapper.NewBitmapper(ctx, bmConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize bitmapper: %w", err)
	}

	following, err := bm.GetGroup(ctx, followingGKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get following group: %w", err)
	}

	followers, err := bm.GetGroup(ctx, followersGKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers group: %w", err)
	}

	g := Graph{
		logger:    logger,
		dbPath:    dbPath,
		shutdown:  make(chan chan struct{}),
		bm:        bm,
		following: following,
		followers: followers,
	}

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

func (g *Graph) AddFollow(ctx context.Context, actorUID, targetUID uint32) error {
	actorFollowing, err := g.following.GetEntity(ctx, actorUID)
	if err != nil {
		return fmt.Errorf("failed to get following entity: %w", err)
	}

	targetFollowers, err := g.followers.GetEntity(ctx, targetUID)
	if err != nil {
		return fmt.Errorf("failed to get followers entity: %w", err)
	}

	actorFollowing.LK.Lock()
	actorFollowing.BM.Add(targetUID)
	g.following.UpdateEntity(ctx, actorUID, actorFollowing.BM)
	actorFollowing.LK.Unlock()

	targetFollowers.LK.Lock()
	targetFollowers.BM.Add(actorUID)
	g.followers.UpdateEntity(ctx, targetUID, targetFollowers.BM)
	targetFollowers.LK.Unlock()

	return nil
}

func (g *Graph) RemoveFollow(ctx context.Context, actorUID, targetUID uint32) error {
	actorFollowing, err := g.following.GetEntity(ctx, actorUID)
	if err != nil {
		return fmt.Errorf("failed to get following entity: %w", err)
	}

	targetFollowers, err := g.followers.GetEntity(ctx, targetUID)
	if err != nil {
		return fmt.Errorf("failed to get followers entity: %w", err)
	}

	actorFollowing.LK.Lock()
	actorFollowing.BM.Remove(targetUID)
	g.following.UpdateEntity(ctx, actorUID, actorFollowing.BM)
	actorFollowing.LK.Unlock()

	targetFollowers.LK.Lock()
	targetFollowers.BM.Remove(actorUID)
	g.followers.UpdateEntity(ctx, targetUID, targetFollowers.BM)
	targetFollowers.LK.Unlock()

	return nil
}

func (g *Graph) GetFollowers(ctx context.Context, uid uint32) ([]uint32, error) {
	followers, err := g.followers.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers entity: %w", err)
	}

	followers.LK.RLock()
	defer followers.LK.RUnlock()
	return followers.BM.ToArray(), nil
}

func (g *Graph) GetFollowing(ctx context.Context, uid uint32) ([]uint32, error) {
	following, err := g.following.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get following entity: %w", err)
	}

	following.LK.RLock()
	defer following.LK.RUnlock()
	return following.BM.ToArray(), nil
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
