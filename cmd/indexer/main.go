package main

import (
	"context"
	"errors"
	"log"
	"os"

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

	offset := int32(0)

	for {
		posts, err := postRegistry.GetPostPage(ctx, 10000, offset)
		if err != nil {
			// Check if error is a search.NotFoundError
			var notFoundErr *search.NotFoundError
			if errors.As(err, &notFoundErr) {
				break
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

		offset += int32(len(posts))

		if offset%100000 == 0 {
			log.Printf("PROGRESS:\tindexed %d posts\n", offset)
		}
	}
}
