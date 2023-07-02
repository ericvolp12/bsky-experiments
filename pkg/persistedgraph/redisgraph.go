package persistedgraph

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

type PersistedGraph struct {
	Client *redis.Client

	CursorMux   sync.RWMutex
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
		CursorMux:      sync.RWMutex{},
	}, nil
}

// AddNode adds a new node with the given NodeID to the graph.
// The method takes a NodeID as an argument and inserts it into the Nodes map.
func (g *PersistedGraph) AddNode(ctx context.Context, node graph.Node) error {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "AddNode")
	defer span.End()
	cmd := g.Client.HSet(ctx, g.NodeKey, string(node.DID), node.Handle)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting node in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.CursorMux.Lock()
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
	g.CursorMux.Unlock()
	return nil
}

// AddEdge adds a directed edge between two nodes in the graph with the specified weight.
// The method takes two NodeIDs (from and to) and an integer weight as arguments.
// If the edge already exists, the weight is updated with the new value.
func (g *PersistedGraph) AddEdge(ctx context.Context, from, to graph.Node, weight int) error {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "AddEdge")
	defer span.End()
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
	g.CursorMux.Lock()
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
	g.CursorMux.Unlock()

	return nil
}

// GetNodeCount returns the number of nodes in the graph.
func (g *PersistedGraph) GetNodeCount(ctx context.Context) int {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "GetNodeCount")
	defer span.End()
	// Get the number of nodes in the graph
	count, err := g.Client.HLen(ctx, g.NodeKey).Result()
	if err != nil {
		return 0
	}

	return int(count)
}

// GetEdgeCount returns the total number of directed edges in the graph.
func (g *PersistedGraph) GetEdgeCount(ctx context.Context) int {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "GetEdgeCount")
	defer span.End()
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
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "IncrementEdge")
	defer span.End()
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
	g.CursorMux.Lock()
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
	g.CursorMux.Unlock()

	return nil
}

// SetCursor sets the cursor for the graph.
func (g *PersistedGraph) SetCursor(ctx context.Context, cursor string) error {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "SetCursor")
	defer span.End()
	// Set the cursor
	cmd := g.Client.Set(ctx, g.CursorKey, cursor, 0)
	if cmd.Err() != nil {
		return fmt.Errorf("error setting cursor in Redis: %w", cmd.Err())
	}

	// Update the last updated time
	g.CursorMux.Lock()
	g.Cursor = cursor
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
	g.CursorMux.Unlock()

	return nil
}

// GetCursor returns the cursor for the graph.
func (g *PersistedGraph) GetCursor(ctx context.Context) string {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "GetCursor")
	defer span.End()
	// Get the cursor
	cursor, err := g.Client.Get(ctx, g.CursorKey).Result()
	if err != nil {
		return ""
	}

	return cursor
}

// Write exports the graph structure to a Golang Writer interface
// The method takes a Writer interface as an argument and writes the graph data to the writer.
// Each line of the writer contains the source node, destination node, and weight of an edge.
func (g *PersistedGraph) Write(ctx context.Context, writer io.Writer) error {
	tracer := otel.Tracer("persistentgraph")
	ctx, span := tracer.Start(ctx, "Write")
	defer span.End()

	// Get all nodes from Redis in chunks of 10000
	nodes := make(map[string]string)
	iter := g.Client.HScan(ctx, g.NodeKey, 0, "*", 10000).Iterator()
	for iter.Next(ctx) {
		did := iter.Val()
		hasVal := iter.Next(ctx)
		if !hasVal {
			log.Printf("Iterator stopped mid-node: %s", did)
			continue
		}
		handle := iter.Val()
		nodes[did] = handle
	}

	// Get all edges from Redis in chunks of 100000
	// Edges have a key of "from-to" and a value of the weight
	edges := make(map[string]string)
	iter = g.Client.HScan(ctx, g.EdgeKey, 0, "*", 100000).Iterator()
	for iter.Next(ctx) {
		edgeName := iter.Val()
		hasVal := iter.Next(ctx)
		if !hasVal {
			log.Printf("Iterator stopped mid-edge: %s", edgeName)
			continue
		}
		weight := iter.Val()
		edges[edgeName] = weight
	}

	// Write each edge to the writer
	for edgeIdentifier, weight := range edges {
		edge := strings.Split(edgeIdentifier, "-")
		if len(edge) != 2 {
			log.Printf("Invalid edge identifier: %s", edgeIdentifier)
			continue
		}
		from := edge[0]
		to := edge[1]
		fmt.Fprintf(writer, "%s %s %s %s %s\n", from, nodes[from], to, nodes[to], weight)
	}

	return nil
}
