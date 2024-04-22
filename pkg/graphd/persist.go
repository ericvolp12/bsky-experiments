package graphd

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
)

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// PartitionSize is the number of actors per SQLite partition
var PartitionSize = 100_000
var MetaDBName = "meta.db"
var ShardDBPattern = "actors_%d.db"

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

	// First, load the actor map
	rows, err := g.metaDB.Query(`SELECT uid, did FROM actor_map;`)
	if err != nil {
		return fmt.Errorf("failed to query actor map: %w", err)
	}
	defer rows.Close()

	totalActors := 0
	nextUID := uint32(0)
	for rows.Next() {
		var uid uint32
		var did string

		if err := rows.Scan(&uid, &did); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		g.setUID(did, uid)
		g.setDID(uid, did)

		if uid > nextUID {
			nextUID = uid
		}

		totalActors++
	}

	numParts := int(math.Ceil(float64(totalActors) / float64(PartitionSize)))

	log.Info("loaded actor map from SQLite", "num_actors", totalActors, "duration", time.Since(start), "num_parts", numParts)

	// Next, load the actor bitmaps from the sharded SQLites
	wg := sync.WaitGroup{}
	partErrs := make([]error, numParts)
	for i := 0; i < numParts; i++ {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			if err := g.loadShardedSQLite(ctx, part); err != nil {
				log.Error("failed to load sharded SQLite", "part", part, "error", err)
				partErrs[part] = err
			}
		}(i)
	}
	wg.Wait()

	for i, err := range partErrs {
		if err != nil {
			return fmt.Errorf("failed to load sharded SQLite (part: %d): %w", i, err)
		}
	}

	log.Info("loaded actor bitmaps from SQLite", "duration", time.Since(start))

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
	log.Info("loaded graph from SQLite and flushed pending queue", "num_actors", totalActors, "duration", time.Since(start))

	return nil
}

func (g *Graph) loadShardedSQLite(ctx context.Context, part int) error {
	dbName := fmt.Sprintf(ShardDBPattern, part)
	db, err := initActorDB(fmt.Sprintf("%s/%s", g.dbPath, dbName))
	if err != nil {
		return fmt.Errorf("failed to initialize actor db for partition (%d): %w", part, err)
	}

	rows, err := db.QueryContext(ctx, `SELECT uid, did, following, followers FROM actors;`)
	if err != nil {
		return fmt.Errorf("failed to query actors: %w", err)
	}
	defer rows.Close()

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
	}

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
	if numEnqueued == 0 {
		log.Info("no graph updates to flush")
		return nil
	}

	log.Info("flushing graph updates to actor_map DB", "enqueued", numEnqueued)

	// First update the actor map
	stmt, err := g.metaDB.Prepare(`INSERT OR REPLACE INTO actor_map (uid, did) VALUES (?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	for _, uid := range updatedActors {
		did, ok := g.GetDID(uint32(uid))
		if !ok {
			return fmt.Errorf("did not found for uid %d", uid)
		}

		if _, err := stmt.Exec(uid, did); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	// Next, update the actor bitmaps
	updatedShards := make(map[int]struct{})
	for _, uid := range updatedActors {
		part := int(uid) / PartitionSize
		updatedShards[part] = struct{}{}
	}

	shardQueues := make([][]uint32, len(updatedShards))
	for _, uid := range updatedActors {
		part := int(uid) / PartitionSize
		shardQueues[part] = append(shardQueues[part], uid)
	}

	log.Info("flushing graph updates to shard DBs", "num_dirty_shards", len(updatedShards))

	wg := sync.WaitGroup{}
	partErrs := make([]error, len(updatedShards))
	for i, queue := range shardQueues {
		wg.Add(1)
		go func(part int, queue []uint32) {
			defer wg.Done()
			if err := g.flushShardSQLite(ctx, part, queue); err != nil {
				log.Error("failed to flush shard SQLite", "part", part, "error", err)
				partErrs[part] = err
			}
		}(i, queue)
	}
	wg.Wait()

	for i, err := range partErrs {
		if err != nil {
			return fmt.Errorf("failed to flush shard SQLite (part: %d): %w", i, err)
		}
	}

	log.Info("flushed graph updates", "duration", time.Since(start), "enqueued", numEnqueued)

	return nil
}

func (g *Graph) flushShardSQLite(ctx context.Context, part int, queue []uint32) error {
	dbName := fmt.Sprintf(ShardDBPattern, part)
	db, err := initActorDB(fmt.Sprintf("%s/%s", g.dbPath, dbName))
	if err != nil {
		return fmt.Errorf("failed to initialize actor db for partition (%d): %w", part, err)
	}

	stmt, err := db.Prepare(`INSERT OR REPLACE INTO actors (uid, did, following, followers) VALUES (?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	for _, uid := range queue {
		followMap, ok := g.g.Load(uid)
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

		did, ok := g.GetDID(uid)
		if !ok {
			return fmt.Errorf("did not found for uid %d", uid)
		}

		if _, err := stmt.ExecContext(ctx, uid, did, followingBytes, followersBytes); err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	return nil
}

func initMetaDB(dbPath string) (*sql.DB, error) {
	dbFullPath := fmt.Sprintf("%s/%s", dbPath, MetaDBName)
	// Open the database
	db, err := sql.Open("sqlite3", dbFullPath)
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

	// Create the table if it doesn't exist
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS actor_map (
		uid INTEGER PRIMARY KEY,
		did TEXT NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create actors table: %w", err)
	}

	// Initialize the DID index if it doesn't exist
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS did_index ON actor_map (did);`); err != nil {
		return nil, fmt.Errorf("failed to create DID index: %w", err)
	}

	return db, nil
}

func initActorDB(dbFullPath string) (*sql.DB, error) {
	// Open the database
	db, err := sql.Open("sqlite3", dbFullPath)
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

	// Create the table if it doesn't exist
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS actors (
		uid INTEGER PRIMARY KEY,
		did TEXT NOT NULL,
		following BLOB NOT NULL,
		followers BLOB NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("failed to create actors table: %w", err)
	}

	return db, nil
}
