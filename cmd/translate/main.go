// package main translates a graph of handles to a graph of DID/handle node objects
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/cheggaaa/pb/v3"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"golang.org/x/time/rate"
)

type didResolver struct {
	client   *xrpc.Client
	limiter  *rate.Limiter
	progress *pb.ProgressBar
	cache    map[string]string
}

func (r *didResolver) resolveDIDFromHandle(ctx context.Context, handle string) (graph.NodeID, error) {
	// If the DID is in the cache, return it
	if did, ok := r.cache[handle]; ok {
		if did == "failed" {
			return "", fmt.Errorf("serving cached failure for handle: %s", handle)
		}
		return graph.NodeID(did), nil
	}

	// Use rate limiter before each request
	err := r.limiter.Wait(ctx)
	if err != nil {
		return "", fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	didResponse, err := comatproto.IdentityResolveHandle(ctx, r.client, handle)
	if err != nil {
		// Add DID to cache
		r.cache[handle] = "failed"
		return "", fmt.Errorf("error resolving handle for %s: %w", handle, err)
	}

	// Add DID to cache
	r.cache[handle] = didResponse.Did

	// Increment the progress bar
	r.progress.Increment()

	return graph.NodeID(didResponse.Did), nil
}

func main() {
	ctx := context.Background()

	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		panic(err)
	}

	// Set up a rate limiter to limit requests to 50 per second
	limiter := rate.NewLimiter(rate.Limit(10), 1)

	// Initialize the progress bar
	progress := pb.New(0)
	progress.SetTemplateString(`{{counters .}} {{bar .}} {{percent .}} {{etime .}}`)
	progress.SetMaxWidth(80)
	progress.Start()
	progress.SetTotal(14891)

	// Initialize the DID resolver
	resolver := &didResolver{
		client:   client,
		limiter:  limiter,
		progress: progress,
		cache:    map[string]string{},
	}

	inputFilename := "old_format.txt"
	outputFilename := "new_format.txt"

	inputFile, err := os.Open(inputFilename)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	g := graph.NewGraph()

	for scanner.Scan() {
		var fromHandle, toHandle string
		var weight int
		_, err := fmt.Sscanf(scanner.Text(), "%s %s %d", &fromHandle, &toHandle, &weight)
		if err != nil {
			fmt.Printf("Error reading line: %v\n", err)
			return
		}

		fromDID, err := resolver.resolveDIDFromHandle(ctx, fromHandle)
		if err != nil {
			fmt.Printf("Error resolving DID for %s: %v\n", fromHandle, err)
			continue
		}

		toDID, err := resolver.resolveDIDFromHandle(ctx, toHandle)
		if err != nil {
			fmt.Printf("Error resolving DID for %s: %v\n", toHandle, err)
			continue
		}

		g.AddNode(graph.Node{DID: fromDID, Handle: fromHandle})
		g.AddNode(graph.Node{DID: toDID, Handle: toHandle})
		g.AddEdge(g.Nodes[fromDID], g.Nodes[toDID], weight)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning input file: %v\n", err)
		return
	}

	if err := g.WriteGraph(outputFilename); err != nil {
		fmt.Printf("Error writing output file: %v\n", err)
	}
}
