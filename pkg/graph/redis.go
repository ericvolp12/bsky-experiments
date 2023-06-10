package graph

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisReaderWriter struct {
	Client *redis.Client
}

// NewRedisReaderWriter creates a new RedisReaderWriter.
func NewRedisReaderWriter(client *redis.Client) *RedisReaderWriter {
	return &RedisReaderWriter{
		Client: client,
	}
}

// WriteGraph exports the graph to Redis.
func (rw *RedisReaderWriter) WriteGraph(ctx context.Context, g Graph, keyPrefix string) error {
	pipeline := rw.Client.Pipeline()

	// Write nodes
	nodeKey := keyPrefix + ":nodes"
	for nodeID, node := range g.Nodes {
		nodeBytes, err := json.Marshal(node)
		if err != nil {
			return err
		}
		pipeline.HSet(ctx, nodeKey, string(nodeID), nodeBytes)
	}

	// Write edges
	edgeKey := keyPrefix + ":edges"
	for fromID, edges := range g.Edges {
		for toID, weight := range edges {
			edge := Edge{From: fromID, To: toID, Weight: weight}
			edgeBytes, err := json.Marshal(edge)
			if err != nil {
				return err
			}
			pipeline.HSet(ctx, edgeKey, string(fromID)+"-"+string(toID), edgeBytes)
		}
	}

	return nil
}

// ReadGraph imports a graph from Redis.
func (rw *RedisReaderWriter) ReadGraph(ctx context.Context, keyPrefix string) (Graph, error) {
	// Read nodes
	nodeKey := keyPrefix + ":nodes"
	nodeMap, err := rw.Client.HGetAll(ctx, nodeKey).Result()
	if err != nil {
		return Graph{}, err
	}

	nodes := make(map[NodeID]Node)
	for nodeID, nodeBytes := range nodeMap {
		var node Node
		if err := json.Unmarshal([]byte(nodeBytes), &node); err != nil {
			return Graph{}, err
		}
		nodes[NodeID(nodeID)] = node
	}

	// Read edges
	edgeKey := keyPrefix + ":edges"
	edgeMap, err := rw.Client.HGetAll(ctx, edgeKey).Result()
	if err != nil {
		return Graph{}, err
	}

	edges := make(map[NodeID]map[NodeID]int)
	for _, edgeBytes := range edgeMap {
		var edge Edge
		if err := json.Unmarshal([]byte(edgeBytes), &edge); err != nil {
			return Graph{}, err
		}
		if _, ok := edges[edge.From]; !ok {
			edges[edge.From] = make(map[NodeID]int)
		}
		edges[edge.From][edge.To] = edge.Weight
	}

	return Graph{
		Nodes: nodes,
		Edges: edges,
	}, nil
}
