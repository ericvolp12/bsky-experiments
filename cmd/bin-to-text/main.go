package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
)

func main() {
	ctx := context.Background()
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go inputfile outputfile")
		return
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	binReaderWriter := graph.BinaryGraphReaderWriter{}

	// Read the graph from the text file
	g, err := binReaderWriter.ReadGraph(ctx, inputFile)
	if err != nil {
		log.Fatalf("Error reading graph from binary file: %v", err)
	}

	// Write the graph to the Plaintext database
	err = g.WriteGraph(outputFile)
	if err != nil {
		log.Fatalf("Error writing graph to plaintext database: %v", err)
	}

	fmt.Println("Graph successfully written to plaintext database")
}
