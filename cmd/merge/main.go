// package main merges two graphs into a single graph.
package main

import (
	"fmt"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
)

func main() {
	graph1Filename := "graph1.txt"
	graph2Filename := "graph2.txt"
	mergedGraphFilename := "merged_graph.txt"

	graph1, err := graph.ReadGraph(graph1Filename)
	if err != nil {
		fmt.Printf("Error reading graph1: %v\n", err)
		return
	}

	graph2, err := graph.ReadGraph(graph2Filename)
	if err != nil {
		fmt.Printf("Error reading graph2: %v\n", err)
		return
	}

	mergedGraph := graph.NewGraph()

	// Merge graph1 into mergedGraph
	for fromDID, edges := range graph1.Edges {
		fromNode := graph1.Nodes[fromDID]
		for toDID, weight := range edges {
			toNode := graph1.Nodes[toDID]
			mergedGraph.AddEdge(fromNode, toNode, weight)
		}
	}

	// Merge graph2 into mergedGraph
	for fromDID, edges := range graph2.Edges {
		fromNode := graph2.Nodes[fromDID]
		for toDID, weight := range edges {
			toNode := graph2.Nodes[toDID]
			mergedGraph.IncrementEdge(fromNode, toNode, weight)
		}
	}

	// Write the mergedGraph to a new text file
	err = mergedGraph.WriteGraph(mergedGraphFilename)
	if err != nil {
		fmt.Printf("Error writing merged graph: %v\n", err)
	}
}
