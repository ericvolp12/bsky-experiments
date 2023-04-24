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
	"fmt"
	"os"
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

// Graph is a struct representing a graph structure.
// It contains nodes, directed edges with weights, and a next node identifier.
// Nodes are stored in a map, with NodeID keys and Node values.
// Edges are stored in a nested map, with NodeID keys for the source node and a second
// map with NodeID keys for the destination node and integer values for the weights.
type Graph struct {
	Nodes  map[NodeID]Node
	Edges  map[NodeID]map[NodeID]int
	NextID NodeID
}

// ReaderWriter is an interface for reading and writing graph structures to and from files.
// Implementations should provide methods for exporting a graph to a file and importing a graph
// from a file.
type ReaderWriter interface {
	WriteGraph(g Graph, filename string) error
	ReadGraph(filename string) (Graph, error)
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
	for from, edges := range g.Edges {
		fromNode := g.Nodes[from]
		for to, weight := range edges {
			toNode := g.Nodes[to]
			fmt.Fprintf(writer, "%s %s %s %s %d\n", fromNode.DID, fromNode.Handle, toNode.DID, toNode.Handle, weight)
		}
	}
	return writer.Flush()
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

	return g, nil
}
