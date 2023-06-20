package persistedgraph

import (
	"context"
	"fmt"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/redis/go-redis/v9"
)

type PersistedGraph struct {
	Client *redis.Client

	LastUpdated time.Time
	Cursor      string

	Prefix         string
	NodeKey        string
	EdgeKey        string
	LastUpdatedKey string
	CursorKey      string
}

func NewPersistedGraph(ctx context.Context, client *redis.Client, prefix string) (*PersistedGraph, error) {
	// Check if the client is connected
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %w", err)
	}

	nodeKey := prefix + ":nodes"
	edgeKey := prefix + ":edges"
	lastUpdatedKey := prefix + ":last-updated"
	cursorKey := prefix + ":cursor"

	// Look for the last updated time and cursor
	lastUpdated, err := client.Get(ctx, lastUpdatedKey).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, fmt.Errorf("error getting last updated time from Redis: %w", err)
		}

		// If the last updated time doesn't exist, set it to the current time
		lastUpdated = time.Now().Format(time.RFC3339)
	}

	// Try to parse the last updated time
	lastUpdatedTime, err := time.Parse(time.RFC3339, lastUpdated)
	if err != nil {
		return nil, fmt.Errorf("error parsing last updated time from Redis: %w", err)
	}

	cursor, err := client.Get(ctx, cursorKey).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, fmt.Errorf("error getting cursor from Redis: %w", err)
		}

		// If the cursor doesn't exist, set it to the empty string
		cursor = ""
	}

	return &PersistedGraph{
		Client:         client,
		Prefix:         prefix,
		NodeKey:        nodeKey,
		EdgeKey:        edgeKey,
		LastUpdatedKey: lastUpdatedKey,
		CursorKey:      cursorKey,
		LastUpdated:    lastUpdatedTime,
		Cursor:         cursor,
	}, nil
}

// AddNode adds a new node with the given NodeID to the graph.
// The method takes a NodeID as an argument and inserts it into the Nodes map.
func (g *PersistedGraph) AddNode(ctx context.Context, node graph.Node) error {
	cmd := g.Client.HSet(ctx, g.NodeKey, string(node.DID), node.Handle)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting node in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
	return nil
}

// AddEdge adds a directed edge between two nodes in the graph with the specified weight.
// The method takes two NodeIDs (from and to) and an integer weight as arguments.
// If the edge already exists, the weight is updated with the new value.
func (g *PersistedGraph) AddEdge(ctx context.Context, from, to graph.Node, weight int) error {
	// Set the nodes in the graph to ensure they exist
	err := g.AddNode(ctx, from)
	if err != nil {
		return err
	}

	err = g.AddNode(ctx, to)
	if err != nil {
		return err
	}

	// Get the current weight of the edge if it exists
	edgeIdentifier := string(from.DID) + "-" + string(to.DID)
	currentWeight, err := g.Client.HGet(ctx, g.EdgeKey, edgeIdentifier).Int()
	if err != nil {
		// If the edge does not exist, set the weight to 0
		if err != redis.Nil {
			return fmt.Errorf("error getting edge from Redis: %w", err)
		}
		currentWeight = 0
	}

	// Set the new weight of the edge
	cmd := g.Client.HSet(ctx, g.EdgeKey, edgeIdentifier, currentWeight+weight)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting edge in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)

	return nil
}

// GetNodeCount returns the number of nodes in the graph.
func (g *PersistedGraph) GetNodeCount(ctx context.Context) int {
	// Get the number of nodes in the graph
	count, err := g.Client.HLen(ctx, g.NodeKey).Result()
	if err != nil {
		return 0
	}

	return int(count)
}

// GetEdgeCount returns the total number of directed edges in the graph.
func (g *PersistedGraph) GetEdgeCount(ctx context.Context) int {
	// Get the number of edges in the graph
	count, err := g.Client.HLen(ctx, g.EdgeKey).Result()
	if err != nil {
		return 0
	}

	return int(count)
}

// IncrementEdge increments the weight of an edge between two nodes by the specified value.
// If the edge does not exist, it is created with the given weight.
func (g *PersistedGraph) IncrementEdge(ctx context.Context, from, to graph.Node, weight int) error {
	// Set the nodes in the graph to ensure they exist
	err := g.AddNode(ctx, from)
	if err != nil {
		return err
	}

	err = g.AddNode(ctx, to)
	if err != nil {
		return err
	}

	// Get the current weight of the edge if it exists
	edgeIdentifier := string(from.DID) + "-" + string(to.DID)
	currentWeight, err := g.Client.HGet(ctx, g.EdgeKey, edgeIdentifier).Int()
	if err != nil {
		// If the edge does not exist, set the weight to 0
		if err != redis.Nil {
			return fmt.Errorf("error getting edge from Redis: %w", err)
		}
		currentWeight = 0
	}

	// Set the new weight of the edge
	cmd := g.Client.HSet(ctx, g.EdgeKey, edgeIdentifier, currentWeight+weight)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting edge in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)

	return nil
}

// SetCursor sets the cursor for the graph.
func (g *PersistedGraph) SetCursor(ctx context.Context, cursor string) error {
	// Set the cursor
	cmd := g.Client.Set(ctx, g.CursorKey, cursor, 0)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting cursor in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)

	return nil
}
