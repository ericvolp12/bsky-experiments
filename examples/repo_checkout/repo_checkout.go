package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
)

func main() {
	ctx := context.Background()

	repoDID := "did:plc:p7gxyfr5vii5ntpwo7f6dhe2"

	var url = "https://bsky.social/xrpc/com.atproto.sync.getCheckout?did=" + repoDID

	// GET and CAR decode the body
	client := &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   120 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "jaz-repo-checkout-demo/0.0.1")

	// Do your rate limit wait here

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request: %v", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error response: %v", resp.StatusCode)
		return
	}

	r, err := repo.ReadRepoFromCar(ctx, resp.Body)
	if err != nil {
		log.Printf("Error reading repo: %v", err)
		return
	}

	err = resp.Body.Close()
	if err != nil {
		log.Printf("Error closing response body: %v", err)
		return
	}

	r.ForEach(ctx, "app.bsky.feed.post", func(path string, nodeCid cid.Cid) error {
		recordCid, rec, err := r.GetRecord(ctx, path)
		if err != nil {
			log.Printf("Error getting record: %v", err)
			return nil
		}

		// Verify that the record cid matches the cid in the event
		if recordCid != nodeCid {
			log.Printf("mismatch in record and op cid: %s != %s", recordCid, nodeCid)
			return nil
		}

		// Do something with the record `rec`
		// collection := strings.Split(path, "/")[0]
		// rkey := strings.Split(path, "/")[1]

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
			if parseError != nil {
				log.Printf("Error parsing date: %v", parseError)
				return nil
			}
			fmt.Printf("(%s@%s): %s\n", repoDID, recCreatedAt, rec.Text)
		}
		return nil
	})

}
