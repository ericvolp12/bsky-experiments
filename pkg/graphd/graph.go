package graphd

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

func NewGraph(dbPath string, syncInterval time.Duration, logger *slog.Logger) (*Graph, error) {
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
		g:             xsync.NewMapOfPresized[uint32, *FollowMap](5_500_000),
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
	actorMap.followingBM.Add(uint32(targetUID))
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Add(uint32(actorUID))
	targetMap.followersLk.Unlock()

	g.followCount.Inc()

	g.updatedLk.Lock()
	g.updatedActors.Add(uint32(actorUID))
	g.updatedActors.Add(uint32(targetUID))
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
	actorMap.followingBM.Remove(uint32(targetUID))
	actorMap.followingLk.Unlock()

	targetMap, _ := g.g.Load(targetUID)
	targetMap.followersLk.Lock()
	targetMap.followersBM.Remove(uint32(actorUID))
	targetMap.followersLk.Unlock()

	g.followCount.Dec()

	g.updatedLk.Lock()
	g.updatedActors.Add(uint32(actorUID))
	g.updatedActors.Add(uint32(targetUID))
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

	return actorMap.followingBM.Contains(uint32(targetUID)), nil
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func (g *Graph) LoadFromCSV(csvFile string) error {
	log := g.logger.With("routine", "graph_csv_load")
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

	// Start 6 workers to process the lines
	for i := 0; i < 6; i++ {
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

	// Play the pending queue
	g.loadLk.Lock()
	close(g.pendingQueue)
	g.isLoaded = true
	for item := range g.pendingQueue {
		switch item.Action {
		case FollowAction:
			g.addFollow(item.Actor, item.Target)
		case UnfollowAction:
			g.removeFollow(item.Actor, item.Target)
		}
	}
	g.loadLk.Unlock()
	log.Info("loaded follows from CSV", "total", totalFollows, "duration", time.Since(start))

	return nil
}

func (g *Graph) processCSVLine(b *bytes.Buffer) error {
	defer func() {
		b.Reset()
		bufPool.Put(b)
	}()

	line := string(b.Bytes())
	actorDID, targetDID, found := strings.Cut(line, ",")
	if !found {
		return fmt.Errorf("invalid follow: %s", line)
	}

	actorUID := g.AcquireDID(actorDID)
	targetUID := g.AcquireDID(targetDID)

	g.addFollow(actorUID, targetUID)

	return nil
}

func (g *Graph) LoadFromSQLite(ctx context.Context) error {
	log := g.logger.With("routine", "graph_sqlite_load")
	log.Info("loading graph from SQLite")

	start := time.Now()

	rows, err := g.db.Query(`SELECT uid, did, following, followers FROM actors;`)
	if err != nil {
		return fmt.Errorf("failed to query actors: %w", err)
	}
	defer rows.Close()

	nextUID := uint32(0)
	totalActors := 0
	for rows.Next() {
		var uid uint32
		var did string
		var followingBytes []byte
		var followersBytes []byte

		if err := rows.Scan(&uid, &did, &followingBytes, &followersBytes); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		followingBM := roaring.NewBitmap()
		_, err := followingBM.FromBuffer(followingBytes)
		if err != nil {
			return fmt.Errorf("failed to deserialize following bitmap: %w", err)
		}

		followersBM := roaring.NewBitmap()
		_, err = followersBM.FromBuffer(followersBytes)
		if err != nil {
			return fmt.Errorf("failed to deserialize followers bitmap: %w", err)
		}

		followMap := &FollowMap{
			followingBM: followingBM,
			followersBM: followersBM,
			followingLk: sync.RWMutex{},
			followersLk: sync.RWMutex{},
		}

		g.g.Store(uid, followMap)
		g.setUID(did, uid)
		g.setDID(uid, did)

		if uid > nextUID {
			nextUID = uid
		}

		totalActors++
	}

	// Play the pending queue
	g.loadLk.Lock()
	g.isLoaded = true
	close(g.pendingQueue)
	for item := range g.pendingQueue {
		switch item.Action {
		case FollowAction:
			g.addFollow(item.Actor, item.Target)
		case UnfollowAction:
			g.removeFollow(item.Actor, item.Target)
		}
	}
	g.loadLk.Unlock()
	log.Info("loaded graph from SQLite", "num_actors", totalActors, "duration", time.Since(start))

	return nil

}

func (g *Graph) FlushUpdates(ctx context.Context) error {
	log := g.logger.With("routine", "graph_flush")
	start := time.Now()

	if !g.IsLoaded() {
		log.Info("graph not finished loading, skipping flush")
		return nil
	}

	// Insert the updated actors
	g.updatedLk.Lock()
	updatedActors := g.updatedActors.ToArray()
	g.updatedActors.Clear()
	g.updatedLk.Unlock()

	numEnqueued := len(updatedActors)
	numSucceeded := 0

	if numEnqueued == 0 {
		log.Info("no graph updates to flush")
		return nil
	}

	log.Info("flushing graph updates to disk", "enqueued", numEnqueued)

	stmt, err := g.db.Prepare(`INSERT OR REPLACE INTO actors (uid, did, following, followers) VALUES (?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	for _, uid := range updatedActors {
		followMap, ok := g.g.Load(uint32(uid))
		if !ok {
			return fmt.Errorf("uid %d not found", uid)
		}

		followingBM := followMap.followingBM
		followersBM := followMap.followersBM

		followMap.followersLk.RLock()
		followMap.followingLk.RLock()

		followingBytes, err := followingBM.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize following bitmap: %w", err)
		}

		followersBytes, err := followersBM.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize followers bitmap: %w", err)
		}

		followMap.followersLk.RUnlock()
		followMap.followingLk.RUnlock()

		did, ok := g.GetDID(uint32(uid))
		if !ok {
			return fmt.Errorf("did not found for uid %d", uid)
		}

		if _, err := stmt.Exec(uid, did, followingBytes, followersBytes); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}

		numSucceeded++
	}

	log.Info("flushed graph updates", "duration", time.Since(start), "enqueued", numEnqueued, "succeeded", numSucceeded)

	return nil
}
