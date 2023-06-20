package persistedgraph

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// NodeID is a string type representing a unique identifier for a node in the graph.
// It is used to identify nodes when adding edges and performing other graph operations.
type NodeID string

// Node is a struct representing a node in the graph.
type Node struct {
	DID    NodeID
	Handle string
}

// Edge is a struct representing a directed edge between two nodes.
// It contains the source node (From), the destination node (To), and the associated weight.
type Edge struct {
	From   NodeID
	To     NodeID
	Weight int
}

type PersistedGraph struct {
	Client         *redis.Client
	Prefix         string
	LastUpdated    time.Time
	NodeKey        string
	EdgeKey        string
	LastUpdatedKey string
}

func NewPersistedGraph(ctx context.Context, client *redis.Client, prefix string) (*PersistedGraph, error) {
	// Check if the client is connected
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return &PersistedGraph{
		Client:         client,
		Prefix:         prefix,
		NodeKey:        prefix + ":nodes",
		EdgeKey:        prefix + ":edges",
		LastUpdatedKey: prefix + ":last-updated",
	}, nil
}

// AddNode adds a new node with the given NodeID to the graph.
// The method takes a NodeID as an argument and inserts it into the Nodes map.
func (g *PersistedGraph) AddNode(ctx context.Context, node Node) {
	g.Client.HSet(ctx, g.NodeKey, node.DID, node.Handle)

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
}

// AddEdge adds a directed edge between two nodes in the graph with the specified weight.
// The method takes two NodeIDs (from and to) and an integer weight as arguments.
// If the edge already exists, the weight is updated with the new value.
func (g *PersistedGraph) AddEdge(ctx context.Context, from, to Node, weight int) {
	// Set the nodes in the graph to ensure they exist
	g.AddNode(ctx, from)
	g.AddNode(ctx, to)

	// Get the current weight of the edge if it exists
	edgeIdentifier := string(from.DID) + ":" + string(to.DID)
	currentWeight, err := g.Client.HGet(ctx, g.EdgeKey, edgeIdentifier).Int()
	if err != nil {
		// If the edge does not exist, set the weight to 0
		currentWeight = 0
	}

	// Set the new weight of the edge
	g.Client.HSet(ctx, g.EdgeKey, edgeIdentifier, currentWeight+weight)

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
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
func (g *PersistedGraph) IncrementEdge(ctx context.Context, from, to Node, weight int) {
	// Set the nodes in the graph to ensure they exist
	g.AddNode(ctx, from)
	g.AddNode(ctx, to)

	// Get the current weight of the edge if it exists
	edgeIdentifier := string(from.DID) + ":" + string(to.DID)
	currentWeight, err := g.Client.HGet(ctx, g.EdgeKey, edgeIdentifier).Int()
	if err != nil {
		// If the edge does not exist, set the weight to 0
		currentWeight = 0
	}

	// Set the new weight of the edge
	g.Client.HSet(ctx, g.EdgeKey, edgeIdentifier, currentWeight+weight)

	// Update the last updated time
	g.LastUpdated = time.Now()
	g.Client.Set(ctx, g.LastUpdatedKey, g.LastUpdated, 0)
}
