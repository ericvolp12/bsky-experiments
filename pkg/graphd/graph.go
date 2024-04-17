package graphd

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/puzpuzpuz/xsync/v3"
)

type Graph struct {
	g *xsync.MapOf[uint32, *FollowMap]

	utd   map[uint32]string
	utdLk sync.RWMutex

	dtu   map[string]uint32
	dtuLk sync.RWMutex

	uidNext uint32
	nextLk  sync.RWMutex

	followCount *xsync.Counter

	userCount *xsync.Counter
}

type FollowMap struct {
	followingBM *roaring.Bitmap
	followingLk sync.RWMutex

	followersBM *roaring.Bitmap
	followersLk sync.RWMutex
}

func NewGraph() *Graph {
	return &Graph{
		g:           xsync.NewMapOf[uint32, *FollowMap](),
		followCount: xsync.NewCounter(),
		userCount:   xsync.NewCounter(),
		utd:         map[uint32]string{},
		dtu:         map[string]uint32{},
	}
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

func (g *Graph) nextUID() uint32 {
	uid := g.uidNext
	g.uidNext++
	return uid
}

func (g *Graph) setDID(uid uint32, did string) {
	g.utdLk.Lock()
	defer g.utdLk.Unlock()
	g.utd[uid] = did
}

// AcquireDID links a DID to a UID, creating a new UID if necessary.
// If the DID is already linked to a UID, that UID is returned
func (g *Graph) AcquireDID(did string) uint32 {
	g.nextLk.RLock()
	uid, ok := g.GetUID(did)
	g.nextLk.RUnlock()
	if !ok {
		g.nextLk.Lock()
		defer g.nextLk.Unlock()

		uid = g.nextUID()
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
	}
	return uid
}

func (g *Graph) AddFollow(actorUID, targetUID uint32) {
	actorMap, _ := g.g.Load(actorUID)
	actorMap.followingLk.Lock()
	actorMap.followingBM.Add(uint32(targetUID))
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Add(uint32(actorUID))
	targetMap.followersLk.Unlock()

	g.followCount.Inc()
}

// RemoveFollow removes a follow from the graph if it exists
func (g *Graph) RemoveFollow(actorUID, targetUID uint32) {
	actorMap, _ := g.g.Load(actorUID)
	actorMap.followingLk.Lock()
	actorMap.followingBM.Remove(uint32(targetUID))
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Remove(uint32(actorUID))
	targetMap.followersLk.Unlock()

	g.followCount.Dec()
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

	return actorMap.followingBM.Contains(uint32(targetUID)), nil
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func (g *Graph) LoadFromCSV(csvFile string) error {
	log := slog.With("source", "graph_load")
	start := time.Now()
	totalFollows := 0

	// Check if the graph CSV exists
	_, err := os.Stat(csvFile)
	if os.IsNotExist(err) {
		log.Info("graph CSV does not exist, skipping load", "path", csvFile)
		return nil
	}

	f, err := os.Open(csvFile)
	if err != nil {
		log.Error("failed to open graph CSV", "path", csvFile, "error", err)
		return err
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	wg := sync.WaitGroup{}
	bufs := make(chan *bytes.Buffer, 10_000)

	// Start 10 workers to process the lines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for buf := range bufs {
				if err := g.processCSVLine(buf); err != nil {
					log.Error("failed to process CSV line", "line", buf, "error", err)
				}
			}
		}()
	}

	for fileScanner.Scan() {
		if totalFollows%1_000_000 == 0 {
			log.Info("loaded follows", "total", totalFollows, "duration", time.Since(start))
		}
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Write(fileScanner.Bytes())
		bufs <- buf
		totalFollows++
	}

	close(bufs)
	wg.Wait()

	log.Info("total follows", "total", totalFollows, "duration", time.Since(start))
	return nil
}

func (g *Graph) processCSVLine(b *bytes.Buffer) error {
	defer func() {
		b.Reset()
		bufPool.Put(b)
	}()

	line := string(b.Bytes())
	parts := strings.SplitN(line, ",", 2) // Use SplitN for efficiency
	if len(parts) < 2 {
		return fmt.Errorf("invalid follow: %s", line)
	}

	actorUID := g.AcquireDID(parts[0])
	targetUID := g.AcquireDID(parts[1])

	g.AddFollow(actorUID, targetUID)

	return nil
}
