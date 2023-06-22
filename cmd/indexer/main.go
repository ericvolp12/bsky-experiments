package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"

	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/meilisearch/meilisearch-go"
)

func main() {
	ctx := context.Background()

	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		panic("REGISTRY_DB_CONNECTION_STRING is not set")
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		panic(err)
	}

	client := meilisearch.NewClient(meilisearch.ClientConfig{
		Host: "http://localhost:7700",
	})

	// Define the shared offset
	offset := int32(0)
	mutex := &sync.Mutex{}

	// Define the wait group
	wg := &sync.WaitGroup{}

	// We want to use 5 concurrent goroutines for indexing
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Lock mutex, get and increment offset
				mutex.Lock()
				localOffset := offset
				offset += 10000
				mutex.Unlock()

				posts, err := postRegistry.GetPostPage(ctx, 10000, localOffset)
				if err != nil {
					// Check if error is a search.NotFoundError
					var notFoundErr *search.NotFoundError
					if errors.As(err, &notFoundErr) {
						return
					}
					log.Printf("error getting posts: %v", err)
					continue
				}

				log.Printf("indexing %d posts...", len(posts))

				ti, err := client.Index("posts").UpdateDocuments(posts, "id")
				if err != nil {
					log.Printf("error indexing posts: %v", err)
					continue
				}

				log.Printf("...indexed %d posts: %d", len(posts), ti.TaskUID)

				if localOffset%100000 == 0 {
					log.Printf("PROGRESS:\tindexed %d posts\n", localOffset)
				}
			}
		}()
	}

	wg.Wait()

	log.Printf("Finished indexing %d posts\n", offset)
}
