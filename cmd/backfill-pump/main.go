package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	app := cli.App{
		Name:    "backfill-pump",
		Usage:   "atproto backfill pump",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "redis-address",
			Usage:   "redis address for storing progress",
			Value:   "localhost:6379",
			EnvVars: []string{"REDIS_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "redis-prefix",
			Usage:   "redis prefix for PLC entries",
			Value:   "plc_directory",
			EnvVars: []string{"REDIS_PREFIX"},
		},
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing backfill progress events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
	}

	app.Action = Pump

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// Pump is the main function for the pump
func Pump(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v\n", err)
	}
	defer func() {
		log.Printf("main function teardown\n")
		err := rawlog.Sync()
		if err != nil {
			log.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar().With("source", "backfill_pump")

	log.Info("starting backfill pump")

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cctx.String("redis-address"),
		Password: "",
		DB:       0,
	})
	if err != nil {
		log.Fatalf("failed to create redis client: %+v\n", err)
	}

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to connect to redis: %+v\n", err)
	}

	// Create a Store
	store, err := store.NewStore(cctx.String("postgres-url"))
	if err != nil {
		log.Fatalf("failed to create store: %+v\n", err)
	}

	dids := []string{}
	cursor := uint64(0)
	var keys []string

	for {
		cmd := redisClient.Scan(ctx, cursor, cctx.String("redis-prefix")+":by_did:*", 5000)
		keys, cursor, err = cmd.Result()
		if err != nil {
			panic(err)
		}
		dids = append(dids, keys...)
		if cursor == 0 || len(keys) == 0 {
			break
		}

		log.Infof("Cursor %d, found %d DIDs\n", cursor, len(dids))
	}

	log.Infof("Found %d DIDs\n", len(dids))

	numRoutines := 10
	backfillsCreated := 0
	errorCh := make(chan error, len(dids))
	processedCh := make(chan int, len(dids))
	doneCh := make(chan bool, numRoutines)

	didCh := make(chan string, len(dids))

	// Send all DIDs to channel
	go func() {
		for _, did := range dids {
			didCh <- did
		}
		close(didCh)
	}()

	for i := 0; i < numRoutines; i++ {
		go func() {
			for did := range didCh {
				did = strings.TrimPrefix(did, "plc_directory:by_did:")
				_, err := store.Queries.GetRepoBackfillRecord(ctx, did)
				if err == nil {
					continue
				}

				if err != sql.ErrNoRows {
					errorCh <- fmt.Errorf("Error getting backfill for %s: %s", did, err)
					continue
				}

				err = store.Queries.CreateRepoBackfillRecord(ctx, store_queries.CreateRepoBackfillRecordParams{
					Repo:         did,
					LastBackfill: time.Now(),
					SeqStarted:   136726301,
					State:        "enqueued",
				})

				if err != nil {
					errorCh <- fmt.Errorf("Error creating backfill for %s: %s", did, err)
					continue
				}

				processedCh <- 1
			}
			doneCh <- true
		}()
	}

	progress := 0
	for completed := 0; completed < numRoutines; {
		select {
		case err := <-errorCh:
			log.Info(err)
		case <-processedCh:
			backfillsCreated++
			progress++
			if progress%10000 == 0 {
				log.Infof("Processed %d dids\n", progress)
			}
		case <-doneCh:
			completed++
		}
	}

	close(errorCh)
	close(processedCh)
	close(doneCh)

	log.Infof("Created %d backfills\n", backfillsCreated)

	return nil
}
