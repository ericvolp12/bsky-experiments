package graphd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"golang.org/x/exp/maps"
)

type Graph struct {
	follows   *xsync.MapOf[uint64, *FollowMap]
	followers *xsync.MapOf[uint64, *FollowMap]

	utd   map[uint64]string
	utdLk sync.RWMutex

	dtu   map[string]uint64
	dtuLk sync.RWMutex

	uidNext uint64
	nextLk  sync.Mutex

	followCount *xsync.Counter

	userCount *xsync.Counter

	graphFilePath  string
	binaryFilePath string
}

type FollowMap struct {
	data map[uint64]struct{}
	lk   sync.RWMutex
}

func NewGraph(filePath string, binaryFilePath string) *Graph {
	return &Graph{
		follows:        xsync.NewMapOf[uint64, *FollowMap](),
		followers:      xsync.NewMapOf[uint64, *FollowMap](),
		followCount:    xsync.NewCounter(),
		userCount:      xsync.NewCounter(),
		utd:            map[uint64]string{},
		dtu:            map[string]uint64{},
		graphFilePath:  filePath,
		binaryFilePath: binaryFilePath,
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
	utd := maps.Clone(g.utd)
	g.utdLk.RUnlock()

	// Write all the follows to the graph CSV
	g.follows.Range(func(actorUID uint64, followMap *FollowMap) bool {
		entries := make([]string, 0, len(followMap.data))

		followMap.lk.RLock()
		for targetUID := range followMap.data {
			didA, ok := utd[actorUID]
			if !ok {
				log.Error("failed to find did for uid", "uid", actorUID)
				continue
			}
			didB, ok := utd[targetUID]
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

func (g *Graph) SaveToBinaryFile() error {
	log := slog.With("source", "graph_save")

	log.Info("saving graph to binary file", "path", g.binaryFilePath)

	start := time.Now()

	f, err := os.Create(g.binaryFilePath + ".new")
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	// Clone the UID to DID map
	g.utdLk.RLock()
	utd := maps.Clone(g.utd)

	// Convert follows to a map[uint64][]uint64
	followsCopy := make(map[uint64][]uint64)
	g.follows.Range(func(actorUID uint64, followMap *FollowMap) bool {
		followMap.lk.RLock()
		for targetUID := range followMap.data {
			followsCopy[actorUID] = append(followsCopy[actorUID], targetUID)
		}
		followMap.lk.RUnlock()
		return true
	})
	g.utdLk.RUnlock()

	err = binary.Write(writer, binary.LittleEndian, uint64(len(utd)))
	if err != nil {
		return err
	}

	for uid, did := range utd {
		err = binary.Write(writer, binary.LittleEndian, uid)
		if err != nil {
			return err
		}

		err = binary.Write(writer, binary.LittleEndian, uint64(len(did)))
		if err != nil {
			return err
		}
		writer.WriteString(did)
	}

	// Write total actors
	err = binary.Write(writer, binary.LittleEndian, uint64(len(followsCopy)))
	if err != nil {
		return err
	}

	// Write each actor and their follows
	for actorUID, targets := range followsCopy {
		err = binary.Write(writer, binary.LittleEndian, actorUID)
		if err != nil {
			return err
		}
		err = binary.Write(writer, binary.LittleEndian, uint64(len(targets)))
		if err != nil {
			return err
		}
		for _, targetUID := range targets {
			err = binary.Write(writer, binary.LittleEndian, targetUID)
			if err != nil {
				return err
			}
		}
	}

	writer.Flush()

	log.Info("saved graph to binary file", "path", g.binaryFilePath, "duration", time.Since(start))

	// Rename the new graph file to the old graph file
	err = os.Rename(g.binaryFilePath+".new", g.binaryFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (g *Graph) LoadGraphFromBinaryFile() error {
	log := slog.With("source", "graph_load")

	_, err := os.Stat(g.binaryFilePath)
	if os.IsNotExist(err) {
		log.Info("graph binary file does not exist, skipping load", "path", g.binaryFilePath)
		return nil
	}

	f, err := os.Open(g.binaryFilePath)
	if err != nil {
		log.Error("failed to open graph binary file", "path", g.binaryFilePath, "error", err)
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	// Read and load UID mappings
	var totalUIDs uint64
	err = binary.Read(reader, binary.LittleEndian, &totalUIDs)
	if err != nil {
		return err
	}

	for i := uint64(0); i < totalUIDs; i++ {
		var uid uint64
		err = binary.Read(reader, binary.LittleEndian, &uid)
		if err != nil {
			return err
		}

		var didLength uint64
		err = binary.Read(reader, binary.LittleEndian, &didLength)
		if err != nil {
			return err
		}

		didBytes := make([]byte, didLength)
		_, err = io.ReadFull(reader, didBytes)
		if err != nil {
			return err
		}

		g.utd[uid] = string(didBytes)
		g.dtu[string(didBytes)] = uid
	}

	// Read total actors
	var totalActors uint64
	err = binary.Read(reader, binary.LittleEndian, &totalActors)
	if err != nil {
		return err
	}

	totalFollows := 0

	// Read each actor and their follows
	for i := uint64(0); i < totalActors; i++ {
		var actorUID uint64
		err = binary.Read(reader, binary.LittleEndian, &actorUID)
		if err != nil {
			return err
		}

		var numTargets uint64
		err = binary.Read(reader, binary.LittleEndian, &numTargets)
		if err != nil {
			return err
		}

		for j := uint64(0); j < numTargets; j++ {
			var targetUID uint64
			err = binary.Read(reader, binary.LittleEndian, &targetUID)
			if err != nil {
				return err
			}

			g.AddFollow(actorUID, targetUID)
			totalFollows++
		}
	}

	log.Info("loaded graph from binary file", "path", g.binaryFilePath, "total_actors", totalActors, "total_follows", totalFollows)

	return nil
}

func (g *Graph) GetUsercount() uint64 {
	return uint64(g.userCount.Value())
}

func (g *Graph) GetFollowcount() uint64 {
	return uint64(g.followCount.Value())
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

		g.userCount.Add(1)
	}
	return uid
}

func (g *Graph) AddFollow(actorUID, targetUID uint64) {
	initMap := &FollowMap{
		data: map[uint64]struct{}{},
	}
	followMap, _ := g.follows.LoadOrStore(actorUID, initMap)
	followMap.lk.Lock()
	followMap.data[targetUID] = struct{}{}
	followMap.lk.Unlock()

	initMap = &FollowMap{
		data: map[uint64]struct{}{},
	}
	followMap, _ = g.followers.LoadOrStore(targetUID, initMap)
	followMap.lk.Lock()
	followMap.data[actorUID] = struct{}{}
	followMap.lk.Unlock()

	g.followCount.Add(1)
}

// RemoveFollow removes a follow from the graph if it exists
func (g *Graph) RemoveFollow(actorUID, targetUID uint64) {
	followMap, ok := g.follows.Load(actorUID)
	if ok {
		followMap.lk.Lock()
		delete(followMap.data, targetUID)
		followMap.lk.Unlock()
	}

	followMap, ok = g.followers.Load(targetUID)
	if ok {
		followMap.lk.Lock()
		delete(followMap.data, actorUID)
		followMap.lk.Unlock()
	}
}

func (g *Graph) GetFollowers(uid uint64) ([]uint64, error) {
	followMap, ok := g.followers.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.lk.RLock()
	defer followMap.lk.RUnlock()

	followers := make([]uint64, len(followMap.data))
	i := 0
	for follower := range followMap.data {
		followers[i] = follower
		i++
	}

	return followers, nil
}

func (g *Graph) GetFollowersMap(uid uint64) (map[uint64]struct{}, error) {
	followMap, ok := g.followers.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.lk.RLock()
	defer followMap.lk.RUnlock()

	mapCopy := make(map[uint64]struct{}, len(followMap.data))
	for follower := range followMap.data {
		mapCopy[follower] = struct{}{}
	}

	return mapCopy, nil
}

func (g *Graph) GetFollowingMap(uid uint64) (map[uint64]struct{}, error) {
	followMap, ok := g.follows.Load(uid)
	if !ok {
		return nil, fmt.Errorf("uid %d not found", uid)
	}
	followMap.lk.RLock()
	defer followMap.lk.RUnlock()

	mapCopy := make(map[uint64]struct{}, len(followMap.data))
	for follower := range followMap.data {
		mapCopy[follower] = struct{}{}
	}

	return mapCopy, nil
}

func (g *Graph) GetMoots(uid uint64) ([]uint64, error) {
	followers, err := g.GetFollowersMap(uid)
	if err != nil {
		return nil, err
	}

	following, err := g.GetFollowingMap(uid)
	if err != nil {
		return nil, err
	}

	moots := make([]uint64, 0)
	if len(followers) < len(following) {
		for follower := range followers {
			if _, ok := following[follower]; ok {
				moots = append(moots, follower)
			}
		}
	} else {
		for follower := range following {
			if _, ok := followers[follower]; ok {
				moots = append(moots, follower)
			}
		}
	}

	return moots, nil
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
		followMap.lk.RLock()
		defer followMap.lk.RUnlock()
		followMaps[i] = followMap
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
		followMap.lk.RLock()
		defer followMap.lk.RUnlock()
		followMaps[i] = followMap
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
	followMap.lk.RLock()
	defer followMap.lk.RUnlock()

	following := make([]uint64, len(followMap.data))
	i := 0
	for follower := range followMap.data {
		following[i] = follower
		i++
	}

	return following, nil
}

// GetFollowersNotFollowing returns a list of followers of the given UID that
// the given UID is not following
func (g *Graph) GetFollowersNotFollowing(uid uint64) ([]uint64, error) {
	followers, err := g.GetFollowersMap(uid)
	if err != nil {
		return nil, err
	}

	following, err := g.GetFollowingMap(uid)
	if err != nil {
		return nil, err
	}

	notFollowing := make([]uint64, 0)
	for follower := range followers {
		if _, ok := following[follower]; !ok {
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
	followMap.lk.RLock()
	defer followMap.lk.RUnlock()

	_, ok = followMap.data[targetUID]
	return ok, nil
}
