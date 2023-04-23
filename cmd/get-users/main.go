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
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"

	"github.com/cheggaaa/pb/v3"
	"golang.org/x/time/rate"
)

func getProfiles(ctx context.Context, client *xrpc.Client, dids []string, limiter *rate.Limiter, progress *pb.ProgressBar) ([]string, error) {
	// Use rate limiter before each request
	err := limiter.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	outProfiles, err := bsky.ActorGetProfiles(ctx, client, dids)
	if err != nil {
		return nil, fmt.Errorf("error getting profile for %+v: %w", dids, err)
	}

	if outProfiles == nil {
		return nil, fmt.Errorf("nil profiles for %+v", dids)
	}

	handles := []string{}

	for _, profile := range outProfiles.Profiles {
		handles = append(handles, profile.Handle)
	}

	// Increment the progress bar
	progress.Add(len(dids))

	return handles, nil
}

func main() {
	ctx := context.Background()
	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		panic(err)
	}

	// Set up a rate limiter to limit requests to 50 per second
	limiter := rate.NewLimiter(rate.Limit(50), 1)

	cursor := ""

	handles := []string{}

	// Initialize the progress bar
	progress := pb.New(0)
	progress.SetTemplateString(`{{counters .}} {{bar .}} {{percent .}} {{etime .}}`)
	progress.SetMaxWidth(80)
	progress.Start()

	// Update the progress bar total count
	progress.SetTotal(35000)

	for {
		repoOutput, err := comatproto.SyncListRepos(ctx, client, cursor, 1000)
		if err != nil {
			fmt.Printf("error listing repos: %s\n", err)
		}

		// Create a wait group and a channel to receive handles
		var wg sync.WaitGroup
		handleChan := make(chan []string, len(repoOutput.Repos)/25+1)

		for i := 0; i < len(repoOutput.Repos); i += 25 {
			wg.Add(1)
			end := i + 25
			if end > len(repoOutput.Repos) {
				end = len(repoOutput.Repos)
			}

			dids := make([]string, end-i)
			for j := i; j < end; j++ {
				dids[j-i] = repoOutput.Repos[j].Did
			}

			go func(dids []string) {
				defer wg.Done()
				batchHandles, err := getProfiles(ctx, client, dids, limiter, progress)
				if err != nil {
					fmt.Println(err)
					return
				}
				handleChan <- batchHandles
			}(dids)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(handleChan)

		// Collect handles from the channel
		for handleSlice := range handleChan {
			handles = append(handles, handleSlice...)
		}

		if repoOutput.Cursor == nil {
			break
		}
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
