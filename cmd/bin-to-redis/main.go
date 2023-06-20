package main

import (
	"context"
	"fmt"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rw := graph.BinaryGraphReaderWriter{}

	g, err := rw.ReadGraph(ctx, "data/social-graph.bin")
	if err != nil {
		panic(err)
	}

	pipeline := redisClient.Pipeline()

	for nodeID, node := range g.Nodes {
		pipeline.HSet(ctx, "social-graph:nodes", string(nodeID), node.Handle)
	}

	// Write edges in chunks of 10k
	count := 0
	for fromID, edges := range g.Edges {
		for toID, weight := range edges {
			if count%10000 == 0 {
				_, err = pipeline.Exec(ctx)
				if err != nil {
					fmt.Printf("Error on pipeline exec: %v\n", err)
				}
				pipeline = redisClient.Pipeline()
			}
			edge := graph.Edge{From: fromID, To: toID, Weight: weight}
			pipeline.HSet(ctx, "social-graph:edges", string(fromID)+"-"+string(toID), edge.Weight)
			count++
		}
	}

	_, err = pipeline.Exec(ctx)
	if err != nil {
		panic(err)
	}
}
