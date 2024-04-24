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
)

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// PartitionSize is the number of actors per SQLite partition
var PartitionSize = 100_000

// MetaDBName is the filename of the meta database
var MetaDBName = "meta.db"

// ShardDBPattern is the pattern for the shard database filenames
var ShardDBPattern = "actors_%d.db"

func (g *Graph) LoadFromCSV(csvFile string) error {
	log := g.logger.With("routine", "graph_csv_load")
	start := time.Now()
	totalFollows := 0

	ctx := context.Background()

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
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for buf := range bufs {
				ctx := context.Background()
				if err := g.processCSVLine(ctx, buf); err != nil {
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

	newWG := sync.WaitGroup{}
	maxUID := g.followers.PeekNextUID()
	// First flush the mapping in a separate goroutine
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		for i := 0; i < int(maxUID); i++ {
			uid := uint32(i)
			did, _, err := g.GetDID(ctx, uid)
			if err != nil {
				log.Error("failed to get DID", "error", err)
				continue
			}

			err = g.followers.UpdateUIDMapping(ctx, uid, did)
			if err != nil {
				log.Error("failed to update followers UID mapping", "error", err)
			}

			err = g.following.UpdateUIDMapping(ctx, uid, did)
			if err != nil {
				log.Error("failed to update following UID mapping", "error", err)
			}
		}
	}()

	// Then walk the graph and flush the bitmaps
	for i := 0; i <= int(maxUID); i += PartitionSize {
		newWG.Add(1)
		go func(i int) {
			defer newWG.Done()
			for j := i; j < i+PartitionSize; j++ {
				uid := uint32(j)

				followers, err := g.followers.GetEntity(ctx, uid)
				if err != nil {
					log.Error("failed to get followers", "error", err)
					continue
				}

				following, err := g.following.GetEntity(ctx, uid)
				if err != nil {
					log.Error("failed to get following", "error", err)
					continue
				}

				err = g.followers.UpdateEntity(ctx, uid, followers)
				if err != nil {
					log.Error("failed to update followers", "error", err)
				}

				err = g.following.UpdateEntity(ctx, uid, following)
				if err != nil {
					log.Error("failed to update following", "error", err)
				}
			}
		}(i)
	}

	newWG.Wait()

	log.Info("loaded follows", "total", totalFollows, "duration", time.Since(start))

	return nil
}

func (g *Graph) processCSVLine(ctx context.Context, b *bytes.Buffer) error {
	defer func() {
		b.Reset()
		bufPool.Put(b)
	}()

	line := string(b.Bytes())
	actorDID, targetDID, found := strings.Cut(line, ",")
	if !found {
		return fmt.Errorf("invalid follow: %s", line)
	}

	actorUID, _, err := g.AcquireDID(ctx, actorDID, false)
	if err != nil {
		return fmt.Errorf("failed to acquire actor UID: %w", err)
	}
	targetUID, _, err := g.AcquireDID(ctx, targetDID, false)
	if err != nil {
		return fmt.Errorf("failed to acquire target UID: %w", err)
	}

	g.AddFollow(ctx, actorUID, targetUID, false)

	return nil
}
