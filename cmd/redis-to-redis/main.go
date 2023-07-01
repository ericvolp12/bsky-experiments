package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

const (
	hashKey1            = "social-graph:nodes"
	hashKey2            = "social-graph:edges"
	sourceRedisUrl      = "100.65.22.74:6379"
	destinationRedisUrl = "localhost:6379"
	sourcePassword      = ""
	destinationPassword = ""
	batchSize           = 1000
)

var ctx = context.Background()

func main() {
	sourceClient := redis.NewClient(&redis.Options{
		Addr:     sourceRedisUrl,
		Password: sourcePassword,
		DB:       0,
	})

	destinationClient := redis.NewClient(&redis.Options{
		Addr:     destinationRedisUrl,
		Password: destinationPassword,
		DB:       0,
	})

	hashes := []string{hashKey1, hashKey2}

	for _, hash := range hashes {
		err := copyHash(sourceClient, destinationClient, hash)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Finished copying hashes")
}

func copyHash(sourceClient *redis.Client, destinationClient *redis.Client, hashKey string) error {
	fmt.Printf("Starting to copy hash: %s\n", hashKey)

	cursor := uint64(0)

	for {
		var keys []string
		var err error

		keys, cursor, err = sourceClient.HScan(ctx, hashKey, cursor, "*", batchSize).Result()
		if err != nil {
			return fmt.Errorf("failed to HScan keys, err: %v", err)
		}

		pipe := destinationClient.Pipeline()
		for i := 0; i < len(keys); i += 2 {
			err = pipe.HSet(ctx, hashKey, keys[i], keys[i+1]).Err()
			if err != nil {
				return fmt.Errorf("failed to set key: %v", err)
			}
		}

		_, err = pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to execute pipeline: %v", err)
		}

		if cursor == 0 {
			break
		}
	}

	fmt.Printf("Finished copying hash: %s\n", hashKey)
	return nil
}
