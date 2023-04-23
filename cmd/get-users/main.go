package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"

	"github.com/cheggaaa/pb/v3"
	"github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"golang.org/x/time/rate"
)

func main() {
	ctx := context.Background()
	client, err := xrpc.GetXRPCClient(ctx)
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

	for {
		repoOutput, err := comatproto.SyncListRepos(ctx, client, cursor, 1000)
		if err != nil {
			fmt.Printf("error listing repos: %s\n", err)
		}

		// Update the progress bar total count
		progress.SetTotal(1000)

		for _, repo := range repoOutput.Repos {
			// Use rate limiter before each request
			err := limiter.Wait(ctx)
			if err != nil {
				fmt.Printf("error waiting for rate limiter: %s\n", err)
				continue
			}

			profile, err := bsky.ActorGetProfile(ctx, client, repo.Did)
			if err != nil {
				fmt.Printf("error getting profile for %s: %s\n", repo.Did, err)
				continue
			}
			handles = append(handles, profile.Handle)

			// Increment the progress bar
			progress.Increment()
		}

		if repoOutput.Cursor == nil {
			break
		}
	}

	// Finish the progress bar
	progress.Finish()

	// Write all handles to a text file
	file, err := os.OpenFile("test.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

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
