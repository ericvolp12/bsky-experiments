package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/cheggaaa/pb/v3"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"golang.org/x/time/rate"
)

func getProfile(ctx context.Context, client *xrpc.Client, did string, limiter *rate.Limiter, progress *pb.ProgressBar) (string, error) {
	// Use rate limiter before each request
	err := limiter.Wait(ctx)
	if err != nil {
		return "", fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	profile, err := bsky.ActorGetProfile(ctx, client, did)
	if err != nil {
		return "", fmt.Errorf("error getting profile for %s: %w", did, err)
	}

	// Increment the progress bar
	progress.Increment()

	return profile.Handle, nil
}

func main() {
	ctx := context.Background()
	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		panic(err)
	}

	// Set up a rate limiter to limit requests to 50 per second
	limiter := rate.NewLimiter(rate.Limit(50), 5)

	cursor := ""

	handles := []string{}

	// Initialize the progress bar
	progress := pb.New(0)
	progress.SetTemplateString(`{{counters .}} {{bar .}} {{percent .}} {{etime .}}`)
	progress.SetMaxWidth(80)
	progress.Start()

	dids := []string{}

	for {
		// Use rate limiter before each request
		err := limiter.Wait(ctx)
		if err != nil {
			fmt.Printf("error waiting for rate limiter: %v", err)
			return
		}

		repoOutput, err := comatproto.SyncListRepos(ctx, client, cursor, 1000)
		if err != nil {
			fmt.Printf("error listing repos: %s\n", err)
		}

		for _, repo := range repoOutput.Repos {
			dids = append(dids, repo.Did)
		}

		if repoOutput.Cursor == nil {
			break
		}

		cursor = *repoOutput.Cursor

		progress.AddTotal(int64(len(repoOutput.Repos)))
	}

	// Create a wait group and a channel to receive handles
	var wg sync.WaitGroup
	handleChan := make(chan string, len(dids))

	for _, did := range dids {
		wg.Add(1)

		go func(did string) {
			defer wg.Done()
			handle, err := getProfile(ctx, client, did, limiter, progress)
			if err != nil {
				fmt.Println(err)
				return
			}
			handleChan <- handle
		}(did)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(handleChan)

	// Collect handles from the channel
	for handle := range handleChan {
		handles = append(handles, handle)
	}

	// Finish the progress bar
	progress.Finish()

	// Write all handles to a text file
	file, err := os.OpenFile("handles.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, handle := range handles {
		_, _ = datawriter.WriteString(handle + "\n")
	}

	datawriter.Flush()
	file.Close()
}
