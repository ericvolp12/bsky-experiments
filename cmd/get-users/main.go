package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"github.com/cheggaaa/pb/v3"
	intXRPC "github.com/ericvolp12/bsky-experiments/pkg/xrpc"
	"golang.org/x/time/rate"
)

type didLookup struct {
	Did                 string `json:"did"`
	VerificationMethods struct {
		Atproto string `json:"atproto"`
	} `json:"verificationMethods"`
	RotationKeys []string `json:"rotationKeys"`
	AlsoKnownAs  []string `json:"alsoKnownAs"`
	Services     struct {
		AtprotoPds struct {
			Type     string `json:"type"`
			Endpoint string `json:"endpoint"`
		} `json:"atproto_pds"`
	} `json:"services"`
}

func getHandle(ctx context.Context, did string, limiter *rate.Limiter, progress *pb.ProgressBar) (string, error) {
	// Use rate limiter before each request
	err := limiter.Wait(ctx)
	if err != nil {
		return "", fmt.Errorf("error waiting for rate limiter: %w", err)
	}

	handle := ""

	// HTTP GET to https://plc.directory/{did}/data
	resp, err := http.Get(fmt.Sprintf("https://plc.directory/%s/data", did))
	if err != nil {
		return "", fmt.Errorf("error getting handle for %s: %w", did, err)
	}

	// Read the response body into a didLookup
	didLookup := didLookup{}
	err = json.NewDecoder(resp.Body).Decode(&didLookup)
	if err != nil {
		return "", fmt.Errorf("error decoding response body for %s: %w", did, err)
	}

	// If the didLookup has a handle, return it
	if len(didLookup.AlsoKnownAs) > 0 {
		// Handles from the DID service look like: "at://jaz.bsky.social", replace the at:// with an @
		handle = strings.Replace(didLookup.AlsoKnownAs[0], "at://", "@", 1)
	}

	// Increment the progress bar
	progress.Increment()

	return handle, nil
}

func main() {
	ctx := context.Background()
	client, err := intXRPC.GetXRPCClient(ctx)
	if err != nil {
		panic(err)
	}

	// Set up a rate limiter to limit requests to 50 per second
	limiter := rate.NewLimiter(rate.Limit(10), 1)

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
			handle, err := getHandle(ctx, did, limiter, progress)
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
