package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
)

func main() {
	ctx := context.Background()

	repoDID := "did:plc:q6gjnaw2blty4crticxkmujt"

	var url = "https://bsky.network/xrpc/com.atproto.sync.getRepo?did=" + repoDID

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

	repoJSON := ""
	posts := []string{}
	likes := []string{}
	reposts := []string{}
	follows := []string{}
	blocks := []string{}
	profile := ""

	r.ForEach(ctx, "", func(path string, nodeCid cid.Cid) error {
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
			rec.CreatedAt = recCreatedAt.Format(time.RFC3339)
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			posts = append(posts, string(recBytes))
		case *bsky.FeedLike:
			recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
			if parseError != nil {
				log.Printf("Error parsing date: %v", parseError)
				return nil
			}
			rec.CreatedAt = recCreatedAt.Format(time.RFC3339)
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			likes = append(likes, string(recBytes))
		case *bsky.FeedRepost:
			recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
			if parseError != nil {
				log.Printf("Error parsing date: %v", parseError)
				return nil
			}
			rec.CreatedAt = recCreatedAt.Format(time.RFC3339)
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			reposts = append(reposts, string(recBytes))
		case *bsky.GraphFollow:
			recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
			if parseError != nil {
				log.Printf("Error parsing date: %v", parseError)
				return nil
			}
			rec.CreatedAt = recCreatedAt.Format(time.RFC3339)
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			follows = append(follows, string(recBytes))
		case *bsky.GraphBlock:
			recCreatedAt, parseError := dateparse.ParseAny(rec.CreatedAt)
			if parseError != nil {
				log.Printf("Error parsing date: %v", parseError)
				return nil
			}
			rec.CreatedAt = recCreatedAt.Format(time.RFC3339)
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			blocks = append(blocks, string(recBytes))
		case *bsky.ActorProfile:
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			profile = string(recBytes)
		}
		return nil
	})

	repoJSON = fmt.Sprintf(`{"posts": [%s],"likes": [%s],"reposts": [%s],"follows": [%s],"blocks": [%s],"profile": %s}`,
		strings.Join(posts, ","),
		strings.Join(likes, ","),
		strings.Join(reposts, ","),
		strings.Join(follows, ","),
		strings.Join(blocks, ","),
		profile,
	)

	fmt.Println(repoJSON)
}
