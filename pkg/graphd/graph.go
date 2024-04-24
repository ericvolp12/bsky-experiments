package graphd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ericvolp12/bsky-experiments/pkg/graphd/bitmapper"
	_ "github.com/mattn/go-sqlite3"

	"github.com/RoaringBitmap/roaring"
)

type Graph struct {
	logger *slog.Logger

	// For Persisting the graph
	dbPath   string
	shutdown chan chan struct{}

	bm        *bitmapper.Bitmapper
	following *bitmapper.Group
	followers *bitmapper.Group
}

var followingGKey = "following"
var followersGKey = "followers"

type GraphConfig struct {
	DBPath    string
	ShardSize uint32
	CacheSize int
}

func DefaultGraphConfig() *GraphConfig {
	return &GraphConfig{
		DBPath:    "data/graphd/",
		ShardSize: 100_000,
		CacheSize: 100_000,
	}
}

func NewGraph(ctx context.Context, logger *slog.Logger, cfg *GraphConfig) (*Graph, error) {
	logger = logger.With("module", "graph")

	if cfg == nil {
		cfg = DefaultGraphConfig()
	}

	followingConfig := bitmapper.GroupConfig{
		Name:      followingGKey,
		ShardSize: cfg.ShardSize,
		CacheSize: cfg.CacheSize,
	}

	followersConfig := bitmapper.GroupConfig{
		Name:      followersGKey,
		ShardSize: cfg.ShardSize,
		CacheSize: cfg.CacheSize,
	}

	groupConfigs := []bitmapper.GroupConfig{followingConfig, followersConfig}
	bmConfig := bitmapper.Config{
		DBDir:  cfg.DBPath,
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
		dbPath:    cfg.DBPath,
		shutdown:  make(chan chan struct{}),
		bm:        bm,
		following: following,
		followers: followers,
	}

	return &g, nil
}

func (g *Graph) Shutdown() {
	g.logger.Info("shutting down graph")
	// finished := make(chan struct{})
	// g.shutdown <- finished
	// <-finished
	g.logger.Info("graph shut down")
}

func (g *Graph) GetDID(ctx context.Context, uid uint32) (string, bool, error) {
	followersID, err := g.followers.GetStringID(ctx, uid)
	if err != nil {
		return "", false, fmt.Errorf("failed to get followers string id: %w", err)
	}

	followingID, err := g.following.GetStringID(ctx, uid)
	if err != nil {
		return "", false, fmt.Errorf("failed to get following string id: %w", err)
	}

	if followersID != followingID {
		return "", false, fmt.Errorf("followers and following string ids do not match")
	}

	return followersID, true, nil
}

func (g *Graph) GetDIDs(ctx context.Context, uids []uint32) ([]string, error) {
	dids := make([]string, len(uids))
	for i, uid := range uids {
		did, ok, err := g.GetDID(ctx, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to get did %q: %w", uid, err)
		}
		if !ok {
			return nil, fmt.Errorf("did not found")
		}
		dids[i] = did
	}
	return dids, nil
}

func (g *Graph) GetUID(ctx context.Context, did string) (uint32, bool, error) {
	followersUID, _, err := g.followers.GetUID(ctx, did, false, false)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get followers uid: %w", err)
	}

	followingUID, _, err := g.following.GetUID(ctx, did, false, false)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get following uid: %w", err)
	}

	if followersUID != followingUID {
		return 0, false, fmt.Errorf("followers and following uids do not match")
	}

	return followersUID, true, nil

}

func (g *Graph) GetUIDs(ctx context.Context, dids []string) ([]uint32, error) {
	uids := make([]uint32, len(dids))
	for i, did := range dids {
		uid, ok, err := g.GetUID(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("failed to get uid %q: %w", did, err)
		}
		if !ok {
			return nil, fmt.Errorf("uid not found")
		}
		uids[i] = uid
	}
	return uids, nil
}

// AcquireDID links a DID to a UID, creating a new UID if necessary.
// If the DID is already linked to a UID, that UID is returned
// If the DID is not linked to a UID, a new UID is created and linked to the DID
// The function returns the UID and a boolean indicating if the UID was created
func (g *Graph) AcquireDID(ctx context.Context, did string, flush bool) (uint32, bool, error) {
	// Locking the cross group lock to avoid races between the following and followers groups
	g.bm.CrossGroupLk.Lock()
	defer g.bm.CrossGroupLk.Unlock()

	followingUID, followingNew, err := g.following.GetUID(ctx, did, true, flush)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get following uid: %w", err)
	}

	followersUID, followersNew, err := g.followers.GetUID(ctx, did, true, flush)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get followers uid: %w", err)
	}

	if followingUID != followersUID {
		return 0, false, fmt.Errorf("following and followers uids do not match")
	}

	return followingUID, followingNew || followersNew, nil
}

// AddFollow adds a follow relationship between the actorUID and the targetUID
func (g *Graph) AddFollow(ctx context.Context, actorUID, targetUID uint32, flush bool) error {
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
	if flush {
		g.following.UpdateEntity(ctx, actorUID, actorFollowing)
	}
	actorFollowing.LK.Unlock()

	targetFollowers.LK.Lock()
	targetFollowers.BM.Add(actorUID)
	if flush {
		g.followers.UpdateEntity(ctx, targetUID, targetFollowers)
	}
	targetFollowers.LK.Unlock()

	return nil
}

// RemoveFollow removes the follow relationship between the actorUID and the targetUID
func (g *Graph) RemoveFollow(ctx context.Context, actorUID, targetUID uint32, flush bool) error {
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
	if flush {
		g.following.UpdateEntity(ctx, actorUID, actorFollowing)
	}
	actorFollowing.LK.Unlock()

	targetFollowers.LK.Lock()
	targetFollowers.BM.Remove(actorUID)
	if flush {
		g.followers.UpdateEntity(ctx, targetUID, targetFollowers)
	}
	targetFollowers.LK.Unlock()

	return nil
}

// GetFollowers returns the accounts that are following the given UID
func (g *Graph) GetFollowers(ctx context.Context, uid uint32) ([]uint32, error) {
	followers, err := g.followers.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers entity: %w", err)
	}

	followers.LK.RLock()
	defer followers.LK.RUnlock()
	return followers.BM.ToArray(), nil
}

// GetFollowing returns the accounts that the given UID is following
func (g *Graph) GetFollowing(ctx context.Context, uid uint32) ([]uint32, error) {
	following, err := g.following.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get following entity: %w", err)
	}

	following.LK.RLock()
	defer following.LK.RUnlock()
	return following.BM.ToArray(), nil
}

// GetMoots returns the accounts that the given UID is following and that are following the given UID back
func (g *Graph) GetMoots(ctx context.Context, uid uint32) ([]uint32, error) {
	following, err := g.following.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get following entity: %w", err)
	}

	followers, err := g.followers.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers entity: %w", err)
	}

	following.LK.RLock()
	defer following.LK.RUnlock()

	followers.LK.RLock()
	defer followers.LK.RUnlock()

	mootMap := roaring.ParAnd(4, following.BM, followers.BM)

	return mootMap.ToArray(), nil
}

// IntersectFollowers returns the intersection of the followers of the given UIDs
func (g *Graph) IntersectFollowers(ctx context.Context, uids []uint32) ([]uint32, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowers(ctx, uids[0])
	}

	followerMaps := make([]*roaring.Bitmap, len(uids))
	for i, uid := range uids {
		followers, err := g.followers.GetEntity(ctx, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to get followers entity: %w", err)
		}
		followers.LK.RLock()
		defer followers.LK.RUnlock()
		followerMaps[i] = followers.BM
	}

	intersectMap := roaring.ParAnd(4, followerMaps...)

	return intersectMap.ToArray(), nil
}

// IntersectFollowing returns the intersection of the accounts that the given UIDs are following
func (g *Graph) IntersectFollowing(ctx context.Context, uids []uint32) ([]uint32, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowing(ctx, uids[0])
	}

	followingMaps := make([]*roaring.Bitmap, len(uids))
	for i, uid := range uids {
		following, err := g.following.GetEntity(ctx, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to get following entity: %w", err)
		}
		following.LK.RLock()
		defer following.LK.RUnlock()
		followingMaps[i] = following.BM
	}

	intersectMap := roaring.ParAnd(4, followingMaps...)

	return intersectMap.ToArray(), nil
}

// GetFollowersNotFollowing returns a list of followers of the given UID that
// the given UID is not following
func (g *Graph) GetFollowersNotFollowing(ctx context.Context, uid uint32) ([]uint32, error) {
	following, err := g.following.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get following entity: %w", err)
	}

	followers, err := g.followers.GetEntity(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers entity: %w", err)
	}

	following.LK.RLock()
	defer following.LK.RUnlock()

	followers.LK.RLock()
	defer followers.LK.RUnlock()

	notFollowingMap := roaring.AndNot(followers.BM, following.BM)

	return notFollowingMap.ToArray(), nil

}

// IntersectFollowingAndFollowers returns the intersection of the following of the actorUID
// and the followers of the targetUID
func (g *Graph) IntersectFollowingAndFollowers(ctx context.Context, actorUID, targetUID uint32) ([]uint32, error) {
	actorFollowing, err := g.following.GetEntity(ctx, actorUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get following entity: %w", err)
	}

	targetFollowers, err := g.followers.GetEntity(ctx, targetUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers entity: %w", err)
	}

	actorFollowing.LK.RLock()
	defer actorFollowing.LK.RUnlock()

	targetFollowers.LK.RLock()
	defer targetFollowers.LK.RUnlock()

	intersectMap := roaring.ParAnd(4, actorFollowing.BM, targetFollowers.BM)

	return intersectMap.ToArray(), nil
}

// DoesFollow returns true if the actorUID is following the targetUID
func (g *Graph) DoesFollow(ctx context.Context, actorUID, targetUID uint32) (bool, error) {
	actorFollowing, err := g.following.GetEntity(ctx, actorUID)
	if err != nil {
		return false, fmt.Errorf("failed to get following entity: %w", err)
	}

	actorFollowing.LK.RLock()
	defer actorFollowing.LK.RUnlock()

	return actorFollowing.BM.Contains(targetUID), nil
}
