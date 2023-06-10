// Package graph provides a simple graph implementation with methods for creating,
// manipulating, and exporting/importing graph structures in Go.
//
// The package defines the following types:
//
// - NodeID: A string type representing a unique identifier for a node in the graph.
// - Edge: A struct representing a directed edge between two nodes with an associated weight.
// - Graph: A struct representing a graph, containing nodes and directed edges with weights.
// - GraphReaderWriter: An interface for reading and writing graphs to and from files.
//
// Additionally, the package provides several methods for working with Graph objects,
// including adding nodes and edges, incrementing edge weights, and getting node and edge counts.
package graph

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"time"
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

// EdgeDiff is a struct representing a directed edge between two nodes and the weight difference.
type EdgeDiff struct {
	From   NodeID
	To     NodeID
	Weight int
}

// Graph is a struct representing a graph structure.
// It contains nodes, directed edges with weights, and a next node identifier.
// Nodes are stored in a map, with NodeID keys and Node values.
// Edges are stored in a nested map, with NodeID keys for the source node and a second
// map with NodeID keys for the destination node and integer values for the weights.
type Graph struct {
	Nodes      map[NodeID]Node
	Edges      map[NodeID]map[NodeID]int
	LastUpdate time.Time
	NextID     NodeID
}

// ReaderWriter is an interface for reading and writing graph structures to and from files.
// Implementations should provide methods for exporting a graph to a file and importing a graph
// from a file.
type ReaderWriter interface {
	WriteGraph(ctx context.Context, g Graph, filename string) error
	ReadGraph(ctx context.Context, filename string) (Graph, error)
}

// NewGraph initializes a new Graph object and returns it.
// It creates empty maps for storing nodes and edges.
func NewGraph() Graph {
	return Graph{
		Nodes: make(map[NodeID]Node),
		Edges: make(map[NodeID]map[NodeID]int),
	}
}

// AddNode adds a new node with the given NodeID to the graph.
// The method takes a NodeID as an argument and inserts it into the Nodes map.
func (g *Graph) AddNode(node Node) {
	g.Nodes[node.DID] = node
	g.LastUpdate = time.Now()
}

// AddEdge adds a directed edge between two nodes in the graph with the specified weight.
// The method takes two NodeIDs (from and to) and an integer weight as arguments.
// If the edge already exists, the weight is updated with the new value.
func (g *Graph) AddEdge(from, to Node, weight int) {
	if _, ok := g.Edges[from.DID]; !ok {
		g.Edges[from.DID] = make(map[NodeID]int)
	}
	g.Edges[from.DID][to.DID] = weight
	g.Nodes[from.DID] = from
	g.Nodes[to.DID] = to
	g.LastUpdate = time.Now()
}

// GetNodeCount returns the number of nodes in the graph.
func (g *Graph) GetNodeCount() int {
	return len(g.Nodes)
}

// GetEdgeCount returns the total number of directed edges in the graph.
func (g *Graph) GetEdgeCount() int {
	count := 0
	for _, edges := range g.Edges {
		count += len(edges)
	}
	return count
}

// IncrementEdge increments the weight of an edge between two nodes by the specified value.
// If the edge does not exist, it is created with the given weight.
func (g *Graph) IncrementEdge(from, to Node, weight int) {
	if _, ok := g.Edges[from.DID]; !ok {
		g.Edges[from.DID] = make(map[NodeID]int)
	}
	g.Edges[from.DID][to.DID] += weight
	g.Nodes[from.DID] = from
	g.Nodes[to.DID] = to
	g.LastUpdate = time.Now()
}

// DeepCopy creates a new Graph that is a deep copy of the current graph.
func (g *Graph) DeepCopy() *Graph {
	newGraph := NewGraph()

	for nodeID, node := range g.Nodes {
		newGraph.Nodes[nodeID] = node
	}

	for from, edges := range g.Edges {
		newGraph.Edges[from] = make(map[NodeID]int)
		for to, weight := range edges {
			newGraph.Edges[from][to] = weight
		}
	}

	newGraph.LastUpdate = g.LastUpdate
	newGraph.NextID = g.NextID

	return &newGraph
}

// Write exports the graph structure to a Golang Writer interface
// The method takes a Writer interface as an argument and writes the graph data to the writer.
// Each line of the writer contains the source node, destination node, and weight of an edge.
func (g *Graph) Write(writer io.Writer) error {
	for from, edges := range g.Edges {
		fromNode := g.Nodes[from]
		for to, weight := range edges {
			toNode := g.Nodes[to]
			fmt.Fprintf(writer, "%s %s %s %s %d\n", fromNode.DID, fromNode.Handle, toNode.DID, toNode.Handle, weight)
		}
	}
	return nil
}

// Diff computes the difference between two graphs and returns a list of EdgeDiff.
func Diff(g1, g2 *Graph) ([]Node, []EdgeDiff) {
	diff := []EdgeDiff{}

	nodes := []Node{}

	// Find nodes that are in g1 but not in g2
	for nodeID, node := range g1.Nodes {
		_, ok := g2.Nodes[nodeID]
		if !ok {
			nodes = append(nodes, node)
		}
	}

	for from, edges := range g1.Edges {
		for to, weight := range edges {
			weight2, ok := g2.Edges[from][to]
			if ok {
				weightDiff := weight - weight2
				if weightDiff != 0 {
					diff = append(diff, EdgeDiff{From: from, To: to, Weight: weightDiff})
				}
			} else {
				diff = append(diff, EdgeDiff{From: from, To: to, Weight: weight})
			}
		}
	}

	return nodes, diff
}

// ApplyDiff updates the graph by applying the given diff to its edges.
func (g *Graph) ApplyDiff(diff []EdgeDiff) {
	for _, edgeDiff := range diff {
		fromNode := g.Nodes[edgeDiff.From]
		toNode := g.Nodes[edgeDiff.To]
		g.IncrementEdge(fromNode, toNode, edgeDiff.Weight)
	}
}

// WriteGraph exports the graph structure to a file with the given filename.
// The method takes a string filename as an argument and writes the graph data to the file.
// Each line of the file contains the source node, destination node, and weight of an edge.
func (g *Graph) WriteGraph(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	err = g.Write(writer)
	if err != nil {
		return fmt.Errorf("error writing graph: %w", err)
	}

	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing writer: %w", err)
	}

	return nil
}

// ReadGraph reads a graph structure from a file with the given filename.
// The method takes a string filename as an argument and returns a Graph object and an error.
// Each line of the file is expected to contain the source node, destination node, and weight of an edge.
func ReadGraph(filename string) (Graph, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Graph{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	g := NewGraph()

	for scanner.Scan() {
		var fromDID, fromHandle, toDID, toHandle string
		var weight int
		_, err := fmt.Sscanf(scanner.Text(), "%s %s %s %s %d", &fromDID, &fromHandle, &toDID, &toHandle, &weight)
		if err != nil {
			return Graph{}, err
		}
		fromNode := Node{DID: NodeID(fromDID), Handle: fromHandle}
		toNode := Node{DID: NodeID(toDID), Handle: toHandle}
		g.AddEdge(fromNode, toNode, weight)
	}

	if err := scanner.Err(); err != nil {
		return Graph{}, err
	}

	g.LastUpdate = time.Now()

	return g, nil
}
