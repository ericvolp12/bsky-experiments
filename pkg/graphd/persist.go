package graphd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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
