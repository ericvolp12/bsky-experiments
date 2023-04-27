package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ericvolp12/bsky-experiments/pkg/graph"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go inputfile outputfile")
		return
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	// Read the graph from the text file
	g, err := graph.ReadGraph(inputFile)
	if err != nil {
		log.Fatalf("Error reading graph from file: %v", err)
	}

	// Create a new SQLiteReaderWriter instance
	rw, err := graph.NewSQLiteReaderWriter(outputFile)
	if err != nil {
		log.Fatalf("Error initializing SQLiteReaderWriter: %v", err)
	}
	defer rw.DB.Close()

	// Write the graph to the SQLite database
	err = rw.WriteGraph(g)
	if err != nil {
		log.Fatalf("Error writing graph to SQLite database: %v", err)
	}

	fmt.Println("Graph successfully written to SQLite database")
}
