package graphfd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
)

type Graph struct {
	db *badger.DB

	dbPath  string
	csvPath string
}

type FollowMap struct {
	// Members is a map of UID -> URI for the members of the follow map
	Members map[uint64]string `json:"members"`
}

func NewGraph(dbPath, csvPath string) (*Graph, error) {
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return nil, err
	}
	return &Graph{
		db:      db,
		dbPath:  dbPath,
		csvPath: csvPath,
	}, nil
}

func BytesToString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

func (g *Graph) LoadFromFile() error {
	log := slog.With("source", "graph_load")
	start := time.Now()
	totalFollows := 0

	// Check if the graph CSV exists
	_, err := os.Stat(g.csvPath)
	if os.IsNotExist(err) {
		log.Info("graph CSV does not exist, skipping load", "path", g.csvPath)
		return nil
	}

	f, err := os.Open(g.csvPath)
	if err != nil {
		log.Error("failed to open graph CSV", "path", g.csvPath, "error", err)
		return err
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	followerMaps := make(map[uint64]*FollowMap)

	// CSV is ordered by actor, target
	currentFollowsMap := FollowMap{
		Members: make(map[uint64]string, 0),
	}
	currentDid := ""
	followTxt := make([]byte, 1000)
	parts := make([]string, 3)
	var followerMap *FollowMap
	var ok bool
	var actorUID uint64

	for fileScanner.Scan() {
		if totalFollows%100_000 == 0 {
			log.Info("loaded follows", "total", totalFollows, "duration", time.Since(start))
		}

		// Zero-allocation file reading (no string conversion)
		followTxt = fileScanner.Bytes()
		parts = strings.SplitN(BytesToString(followTxt), ",", 3) // Use SplitN for efficiency
		if len(parts) < 3 {
			log.Error("invalid follow", "follow", followTxt)
			continue
		}

		if currentDid == "" {
			currentDid = parts[0]
		}

		followerMap, ok = followerMaps[g.AcquireDID(parts[1])]
		if !ok {
			followerMap = &FollowMap{
				Members: make(map[uint64]string),
			}
			followerMaps[g.AcquireDID(parts[1])] = followerMap
			followerMap.Members[g.AcquireDID(parts[0])] = parts[2]
		} else {
			followerMap.Members[g.AcquireDID(parts[0])] = parts[2]
		}

		if parts[0] == currentDid {
			currentFollowsMap.Members[g.AcquireDID(parts[1])] = parts[2]
			totalFollows++
			continue
		}

		actorUID = g.AcquireDID(currentDid)
		err = g.db.Update(func(txn *badger.Txn) error {
			// Flush the current follow map
			followingBytes, err := json.Marshal(currentFollowsMap)
			if err != nil {
				return fmt.Errorf("failed to get following map: %w", err)
			}
			err = txn.Set([]byte(fmt.Sprintf("following:%d", actorUID)), followingBytes)
			if err != nil {
				return fmt.Errorf("failed to set following map: %w", err)
			}
			return nil
		})
		if err != nil {
			log.Error("failed to flush follow map", "error", err)
		}

		// Reset the current follow map
		currentDid = parts[0]
		currentFollowsMap = FollowMap{
			Members: make(map[uint64]string),
		}
		currentFollowsMap.Members[g.AcquireDID(parts[1])] = parts[2]
		totalFollows++
	}

	for uid, followerMap := range followerMaps {
		err = g.db.Update(func(txn *badger.Txn) error {
			// Flush the current follow map
			followerBytes, err := json.Marshal(followerMap)
			if err != nil {
				return fmt.Errorf("failed to get followers map: %w", err)
			}
			err = txn.Set([]byte(fmt.Sprintf("followers:%d", uid)), followerBytes)
			if err != nil {
				return fmt.Errorf("failed to set followers map: %w", err)
			}
			return nil
		})
		if err != nil {
			log.Error("failed to flush follower map", "error", err)
		}
	}

	log.Info("total follows", "total", totalFollows, "duration", time.Since(start))
	return nil
}

func (g *Graph) GetDID(uid uint64) (string, bool) {
	did := ""
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("utd:%d", uid)))
		if err != nil {
			return err
		}
		didBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		did = string(didBytes)
		return nil
	})
	if err != nil {
		return "", false
	}
	if len(did) == 0 {
		return "", false
	}
	return did, true
}

func (g *Graph) GetDIDs(uids []uint64) ([]string, error) {
	dids := []string{}
	err := g.db.View(func(txn *badger.Txn) error {
		for _, uid := range uids {
			item, err := txn.Get([]byte(fmt.Sprintf("utd:%d", uid)))
			if err != nil {
				return err
			}
			didBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			dids = append(dids, string(didBytes))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dids, nil
}

func (g *Graph) GetUID(did string) (uint64, bool) {
	var uid *uint64
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("dtu:%s", did)))
		if err != nil {
			return err
		}
		uidBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		uidVal, err := strconv.ParseUint(string(uidBytes), 10, 64)
		if err != nil {
			return err
		}
		uid = &uidVal
		return nil
	})
	if err != nil {
		return 0, false
	}
	if uid == nil {
		return 0, false
	}
	return *uid, true
}

func (g *Graph) GetUIDs(dids []string) ([]uint64, error) {
	uids := []uint64{}
	err := g.db.View(func(txn *badger.Txn) error {
		for _, did := range dids {
			item, err := txn.Get([]byte(fmt.Sprintf("dtu:%s", did)))
			if err != nil {
				return err
			}
			uidBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			uidVal, err := strconv.ParseUint(string(uidBytes), 10, 64)
			if err != nil {
				return err
			}
			uids = append(uids, uidVal)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uids, nil
}

func (g *Graph) setUID(did string, uid uint64) {
	err := g.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(fmt.Sprintf("dtu:%s", did)), []byte(fmt.Sprintf("%d", uid)))
		if err != nil {
			return err
		}
		err = txn.Set([]byte(fmt.Sprintf("utd:%d", uid)), []byte(did))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		slog.Error("failed to set uid", "did", did, "uid", uid, "error", err)
	}
}

func (g *Graph) nextUID() uint64 {
	nextUID := uint64(0)
	err := g.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("next_uid"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				nextUID = 1
				return txn.Set([]byte("next_uid"), []byte("1"))
			}
			return err
		}
		nextUIDBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		nextUIDVal, err := strconv.ParseUint(string(nextUIDBytes), 10, 64)
		if err != nil {
			return err
		}
		nextUID = nextUIDVal + 1
		return txn.Set([]byte("next_uid"), []byte(fmt.Sprintf("%d", nextUID)))
	})
	if err != nil {
		slog.Error("failed to get next uid", "error", err)
	}
	return nextUID
}

func (g *Graph) setDID(uid uint64, did string) {
	err := g.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(fmt.Sprintf("utd:%d", uid)), []byte(did))
		if err != nil {
			return err
		}
		err = txn.Set([]byte(fmt.Sprintf("dtu:%s", did)), []byte(fmt.Sprintf("%d", uid)))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		slog.Error("failed to set did", "uid", uid, "did", did, "error", err)
	}
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
		err := g.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(fmt.Sprintf("following:%d", uid)))
			if err != nil {
				if err != badger.ErrKeyNotFound {
					return fmt.Errorf("failed to initialize following map: %w", err)
				}
				followingMap := &FollowMap{
					Members: make(map[uint64]string),
				}
				followingBytes, err := json.Marshal(followingMap)
				if err != nil {
					return fmt.Errorf("failed to initialize following map: %w", err)
				}
				err = txn.Set([]byte(fmt.Sprintf("following:%d", uid)), followingBytes)
				if err != nil {
					return fmt.Errorf("failed to initialize following map: %w", err)
				}
			}
			_, err = txn.Get([]byte(fmt.Sprintf("followers:%d", uid)))
			if err != nil {
				if err != badger.ErrKeyNotFound {
					return fmt.Errorf("failed to initialize followers map: %w", err)
				}
				followersMap := &FollowMap{
					Members: make(map[uint64]string),
				}
				followersBytes, err := json.Marshal(followersMap)
				if err != nil {
					return fmt.Errorf("failed to initialize followers map: %w", err)
				}
				err = txn.Set([]byte(fmt.Sprintf("followers:%d", uid)), followersBytes)
				if err != nil {
					return fmt.Errorf("failed to initialize followers map: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			slog.Error("failed to initialize follow map", "uid", uid, "error", err)
		}
	}
	return uid
}

func (g *Graph) AddFollow(actorUID, targetUID uint64, rkey string) {
	err := g.db.Update(func(txn *badger.Txn) error {
		// Add the follow to the actor's following map
		item, err := txn.Get([]byte(fmt.Sprintf("following:%d", actorUID)))
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followingBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followMap := &FollowMap{}
		err = json.Unmarshal(followingBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followMap.Members[targetUID] = rkey
		followingBytes, err = json.Marshal(followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		err = txn.Set([]byte(fmt.Sprintf("following:%d", actorUID)), followingBytes)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}

		// Add the follow to the target's followers map
		item, err = txn.Get([]byte(fmt.Sprintf("followers:%d", targetUID)))
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followersBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followMap = &FollowMap{}
		err = json.Unmarshal(followersBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followMap.Members[actorUID] = rkey
		followersBytes, err = json.Marshal(followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		err = txn.Set([]byte(fmt.Sprintf("followers:%d", targetUID)), followersBytes)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}

		return nil
	})
	if err != nil {
		slog.Error("failed to add follow", "actor_uid", actorUID, "target_uid", targetUID, "error", err)
	}
}

// RemoveFollow removes a follow from the graph if it exists
func (g *Graph) RemoveFollow(actorUID, targetUID uint64) {
	err := g.db.Update(func(txn *badger.Txn) error {
		// Remove the follow from the actor's following map
		item, err := txn.Get([]byte(fmt.Sprintf("following:%d", actorUID)))
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followingBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followMap := &FollowMap{}
		err = json.Unmarshal(followingBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		delete(followMap.Members, targetUID)
		followingBytes, err = json.Marshal(followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		err = txn.Set([]byte(fmt.Sprintf("following:%d", actorUID)), followingBytes)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}

		// Remove the follow from the target's followers map
		item, err = txn.Get([]byte(fmt.Sprintf("followers:%d", targetUID)))
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followersBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followMap = &FollowMap{}
		err = json.Unmarshal(followersBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		delete(followMap.Members, actorUID)
		followersBytes, err = json.Marshal(followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		err = txn.Set([]byte(fmt.Sprintf("followers:%d", targetUID)), followersBytes)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}

		return nil
	})
	if err != nil {
		slog.Error("failed to remove follow", "actor_uid", actorUID, "target_uid", targetUID, "error", err)
	}
}

func (g *Graph) GetFollowers(uid uint64) ([]uint64, error) {
	followMap := &FollowMap{}
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("followers:%d", uid)))
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followersBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		err = json.Unmarshal(followersBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	followers := []uint64{}
	for follower := range followMap.Members {
		followers = append(followers, follower)
	}

	return followers, nil
}

func (g *Graph) GetFollowersMap(uid uint64) (map[uint64]string, error) {
	followMap := &FollowMap{}
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("followers:%d", uid)))
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		followersBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		err = json.Unmarshal(followersBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get followers map: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return followMap.Members, nil
}

func (g *Graph) GetFollowingMap(uid uint64) (map[uint64]string, error) {
	followMap := &FollowMap{}
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("following:%d", uid)))
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followingBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		err = json.Unmarshal(followingBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return followMap.Members, nil
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

	if len(followers) == 0 || len(following) == 0 {
		return nil, nil
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

	followMaps := make([]map[uint64]string, len(uids))
	for i, uid := range uids {
		followMap, err := g.GetFollowersMap(uid)
		if err != nil {
			return nil, err
		}
		followMaps[i] = followMap
	}

	// Find the smallest map
	smallest := followMaps[0]
	for _, followMap := range followMaps {
		if len(followMap) < len(smallest) {
			smallest = followMap
		}
	}

	// Remove the smallest map from the list of maps to intersect
	followMaps = followMaps[1:]

	intersection := make([]uint64, 0)
	for follower := range smallest {
		found := true
		for _, followMap := range followMaps {
			if _, ok := followMap[follower]; !ok {
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
		return g.GetFollowers(uids[0])
	}

	followMaps := make([]map[uint64]string, len(uids))
	for i, uid := range uids {
		followMap, err := g.GetFollowingMap(uid)
		if err != nil {
			return nil, err
		}
		followMaps[i] = followMap
	}

	// Find the smallest map
	smallest := followMaps[0]
	for _, followMap := range followMaps {
		if len(followMap) < len(smallest) {
			smallest = followMap
		}
	}

	// Remove the smallest map from the list of maps to intersect
	followMaps = followMaps[1:]

	intersection := make([]uint64, 0)
	for following := range smallest {
		found := true
		for _, followMap := range followMaps {
			if _, ok := followMap[following]; !ok {
				found = false
				break
			}
		}
		if found {
			intersection = append(intersection, following)
		}
	}

	return intersection, nil
}

func (g *Graph) GetFollowing(uid uint64) ([]uint64, error) {
	followMap := &FollowMap{}
	err := g.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("following:%d", uid)))
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		followingBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		err = json.Unmarshal(followingBytes, followMap)
		if err != nil {
			return fmt.Errorf("failed to get following map: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	following := []uint64{}
	for follower := range followMap.Members {
		following = append(following, follower)
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
	following, err := g.GetFollowingMap(actorUID)
	if err != nil {
		return false, err
	}

	_, ok := following[targetUID]
	return ok, nil
}
