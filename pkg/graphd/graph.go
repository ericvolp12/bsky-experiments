package graphd

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

type Graph struct {
	follows   sync.Map
	followers sync.Map

	utd   map[uint64]string
	utdLk sync.RWMutex

	dtu   map[string]uint64
	dtuLk sync.RWMutex

	uidNext uint64
	nextLk  sync.Mutex

	followCount   uint64
	followCountLk sync.RWMutex

	userCount   uint64
	userCountLk sync.RWMutex

	graphFilePath string
}

type FollowMap struct {
	data map[uint64]struct{}
	lk   sync.RWMutex
}

func NewGraph(filePath string) *Graph {
	return &Graph{
		utd:           map[uint64]string{},
		dtu:           map[string]uint64{},
		graphFilePath: filePath,
	}
}

func (g *Graph) LoadFromFile() error {
	log := slog.With("source", "graph_load")
	start := time.Now()
	totalFollows := 0

	// Check if the graph CSV exists
	_, err := os.Stat(g.graphFilePath)
	if os.IsNotExist(err) {
		log.Info("graph CSV does not exist, skipping load", "path", g.graphFilePath)
		return nil
	}

	f, err := os.Open(g.graphFilePath)
	if err != nil {
		log.Error("failed to open graph CSV", "path", g.graphFilePath, "error", err)
		return err
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	// Use this buffer to pre-allocate slice capacities when possible
	buffer := make([]string, 2)

	for fileScanner.Scan() {
		if totalFollows%1_000_000 == 0 {
			log.Info("loaded follows", "total", totalFollows, "duration", time.Since(start))
		}

		followTxt := fileScanner.Text()
		parts := strings.SplitN(followTxt, ",", 2) // Use SplitN for efficiency
		if len(parts) < 2 {
			log.Error("invalid follow", "follow", followTxt)
			continue
		}

		copy(buffer, parts)
		actorUID := g.AcquireDID(buffer[0])
		targetUID := g.AcquireDID(buffer[1])

		g.AddFollow(actorUID, targetUID)

		totalFollows++
	}

	log.Info("total follows", "total", totalFollows, "duration", time.Since(start))
	return nil
}

func (g *Graph) SaveToFile() error {
	log := slog.With("source", "graph_save")

	// Write a new revision of the graph CSV and delete the old one once done
	f, err := os.Create(g.graphFilePath + ".new")
	if err != nil {
		return err
	}
	defer f.Close()

	// Use buffered writing
	writer := bufio.NewWriter(f)

	g.utdLk.RLock()
	defer g.utdLk.RUnlock()

	// Write all the follows to the graph CSV
	g.follows.Range(func(key, value interface{}) bool {
		actorUID := key.(uint64)
		followMap := value.(*FollowMap)
		entries := make([]string, 0, len(followMap.data))

		followMap.lk.RLock()
		for targetUID := range followMap.data {
			didA, ok := g.utd[actorUID]
			if !ok {
				log.Error("failed to find did for uid", "uid", actorUID)
				continue
			}
			didB, ok := g.utd[targetUID]
			if !ok {
				log.Error("failed to find did for uid", "uid", targetUID)
				continue
			}
			entries = append(entries, didA+","+didB+"\n")
		}
		followMap.lk.RUnlock()

		for _, entry := range entries {
			writer.WriteString(entry)
		}

		return true
	})

	// Flush the buffer to ensure all data is written to disk
	writer.Flush()

	// Delete the old graph CSV
	err = os.Remove(g.graphFilePath)
	if err != nil {
		return err
	}

	// Rename the new graph CSV to the old graph CSV
	err = os.Rename(g.graphFilePath+".new", g.graphFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (g *Graph) GetUsercount() uint64 {
	g.userCountLk.RLock()
	defer g.userCountLk.RUnlock()
	return g.userCount
}

func (g *Graph) GetFollowcount() uint64 {
	g.followCountLk.RLock()
	defer g.followCountLk.RUnlock()
	return g.followCount
}

func (g *Graph) GetDID(uid uint64) (string, bool) {
	g.utdLk.RLock()
	defer g.utdLk.RUnlock()
	did, ok := g.utd[uid]
	return did, ok
}

func (g *Graph) GetDIDs(uids []uint64) ([]string, error) {
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

func (g *Graph) GetUID(did string) (uint64, bool) {
	g.dtuLk.RLock()
	defer g.dtuLk.RUnlock()
	uid, ok := g.dtu[did]
	return uid, ok
}

func (g *Graph) GetUIDs(dids []string) ([]uint64, error) {
	g.dtuLk.RLock()
	defer g.dtuLk.RUnlock()
	uids := make([]uint64, len(dids))
	for i, did := range dids {
		uid, ok := g.dtu[did]
		if !ok {
			return nil, fmt.Errorf("did %s not found", did)
		}
		uids[i] = uid
	}
	return uids, nil
}

func (g *Graph) setUID(did string, uid uint64) {
	g.dtuLk.Lock()
	defer g.dtuLk.Unlock()
	g.dtu[did] = uid
}

func (g *Graph) nextUID() uint64 {
	g.nextLk.Lock()
	defer g.nextLk.Unlock()
	uid := g.uidNext
	g.uidNext++
	return uid
}

func (g *Graph) setDID(uid uint64, did string) {
	g.utdLk.Lock()
	defer g.utdLk.Unlock()
	g.utd[uid] = did
}

// AcquireDID links a DID to a UID, creating a new UID if necessary.
// If the DID is already linked to a UID, that UID is returned
func (g *Graph) AcquireDID(did string) uint64 {
	uid, ok := g.GetUID(did)
	if !ok {
		uid = g.nextUID()
		g.setUID(did, uid)
		g.setDID(uid, did)

		// Initialize the follow maps
		g.follows.Store(uid, &FollowMap{
			data: map[uint64]struct{}{},
		})
		g.followers.Store(uid, &FollowMap{
			data: map[uint64]struct{}{},
		})

		g.userCountLk.Lock()
		g.userCount++
		g.userCountLk.Unlock()
	}
	return uid
}

func (g *Graph) AddFollow(actorUID, targetUID uint64) {
	followMap, ok := g.follows.Load(actorUID)
	if !ok {
		followMap = &FollowMap{
			data: map[uint64]struct{}{},
		}
		g.follows.Store(actorUID, followMap)
	}
	followMap.(*FollowMap).lk.Lock()
	followMap.(*FollowMap).data[targetUID] = struct{}{}
	followMap.(*FollowMap).lk.Unlock()

	followMap, ok = g.followers.Load(targetUID)
	if !ok {
		followMap = &FollowMap{
			data: map[uint64]struct{}{},
		}
		g.followers.Store(targetUID, followMap)
	}
	followMap.(*FollowMap).lk.Lock()
	followMap.(*FollowMap).data[actorUID] = struct{}{}
	followMap.(*FollowMap).lk.Unlock()

	g.followCountLk.Lock()
	g.followCount++
	g.followCountLk.Unlock()
}

// RemoveFollow removes a follow from the graph if it exists
func (g *Graph) RemoveFollow(actorUID, targetUID uint64) {
	followMap, ok := g.follows.Load(actorUID)
	if ok {
		followMap.(*FollowMap).lk.Lock()
		delete(followMap.(*FollowMap).data, targetUID)
		followMap.(*FollowMap).lk.Unlock()
	}

	followMap, ok = g.followers.Load(targetUID)
	if !ok {
		followMap.(*FollowMap).lk.Lock()
		delete(followMap.(*FollowMap).data, actorUID)
		followMap.(*FollowMap).lk.Unlock()
	}
}

func (g *Graph) GetFollowers(uid uint64) ([]uint64, error) {
	followMap, ok := g.followers.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	followers := make([]uint64, len(followMap.(*FollowMap).data))
	i := 0
	for follower := range followMap.(*FollowMap).data {
		followers[i] = follower
		i++
	}

	return followers, nil
}

func (g *Graph) IntersectFollowers(uids []uint64) ([]uint64, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowers(uids[0])
	}

	followMaps := make([]*FollowMap, len(uids))
	for i, uid := range uids {
		followMap, ok := g.followers.Load(uid)
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		followMap.(*FollowMap).lk.RLock()
		defer followMap.(*FollowMap).lk.RUnlock()
		followMaps[i] = followMap.(*FollowMap)
	}

	// Find the smallest map
	smallest := followMaps[0]
	for _, followMap := range followMaps {
		if len(followMap.data) < len(smallest.data) {
			smallest = followMap
		}
	}

	// Remove the smallest map from the list of maps to intersect
	followMaps = followMaps[1:]

	intersection := make([]uint64, 0)
	for follower := range smallest.data {
		found := true
		for _, followMap := range followMaps {
			if _, ok := followMap.data[follower]; !ok {
				found = false
				break
			}
		}
		if found {
			intersection = append(intersection, follower)
		}
	}

	return intersection, nil
}

func (g *Graph) IntersectFollowing(uids []uint64) ([]uint64, error) {
	if len(uids) == 0 {
		return nil, fmt.Errorf("uids must have at least one element")
	}

	if len(uids) == 1 {
		return g.GetFollowing(uids[0])
	}

	followMaps := make([]*FollowMap, len(uids))
	for i, uid := range uids {
		followMap, ok := g.follows.Load(uid)
		if !ok {
			return nil, fmt.Errorf("uid %d not found", uid)
		}
		followMap.(*FollowMap).lk.RLock()
		defer followMap.(*FollowMap).lk.RUnlock()
		followMaps[i] = followMap.(*FollowMap)
	}

	// Find the smallest map
	smallest := followMaps[0]
	for _, followMap := range followMaps {
		if len(followMap.data) < len(smallest.data) {
			smallest = followMap
		}
	}

	// Remove the smallest map from the list of maps to intersect
	followMaps = followMaps[1:]

	intersection := make([]uint64, 0)
	for follower := range smallest.data {
		found := true
		for _, followMap := range followMaps {
			if _, ok := followMap.data[follower]; !ok {
				found = false
				break
			}
		}
		if found {
			intersection = append(intersection, follower)
		}
	}

	return intersection, nil
}

func (g *Graph) GetFollowing(uid uint64) ([]uint64, error) {
	followMap, ok := g.follows.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	following := make([]uint64, len(followMap.(*FollowMap).data))
	i := 0
	for follower := range followMap.(*FollowMap).data {
		following[i] = follower
		i++
	}

	return following, nil
}

// GetFollowersNotFollowing returns a list of followers of the given UID that
// the given UID is not following
func (g *Graph) GetFollowersNotFollowing(uid uint64) ([]uint64, error) {
	followers, err := g.GetFollowers(uid)
	if err != nil {
		return nil, err
	}

	following, err := g.GetFollowing(uid)
	if err != nil {
		return nil, err
	}

	notFollowing := make([]uint64, 0)
	for _, follower := range followers {
		found := false
		for _, followee := range following {
			if follower == followee {
				found = true
				break
			}
		}
		if !found {
			notFollowing = append(notFollowing, follower)
		}
	}

	return notFollowing, nil
}

func (g *Graph) DoesFollow(actorUID, targetUID uint64) (bool, error) {
	followMap, ok := g.follows.Load(actorUID)
	if !ok {
		return false, fmt.Errorf("actor uid %d not found", actorUID)
	}
	followMap.(*FollowMap).lk.RLock()
	defer followMap.(*FollowMap).lk.RUnlock()

	_, ok = followMap.(*FollowMap).data[targetUID]
	return ok, nil
}
