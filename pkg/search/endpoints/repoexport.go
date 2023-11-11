package endpoints

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
)

func (api *API) GetRepoAsJSON(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetRepoAsJSON")
	defer span.End()

	// Get the repo DID from the query string
	repoDID := c.Param("did")
	if repoDID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "did must be provided in path"})
		return
	}

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
	req.Header.Set("User-Agent", "jaz-repo-checkout-search-API/0.0.1")

	// Do your rate limit wait here
	api.CheckoutLimiter.Wait(ctx)

	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("Error getting repo from BSky: %w", err).Error()})
		return
	}

	if resp.StatusCode != http.StatusOK {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("Error getting repo from BSky: %s", resp.Status).Error()})
		return
	}

	r, err := repo.ReadRepoFromCar(ctx, resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("Error reading repo CAR: %w", err).Error()})
		return
	}

	err = resp.Body.Close()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("Error closing response body: %w", err).Error()})
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
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			posts = append(posts, string(entity))
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
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			likes = append(likes, string(entity))
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
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			reposts = append(reposts, string(entity))
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
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			follows = append(follows, string(entity))
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
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			blocks = append(blocks, string(entity))
		case *bsky.ActorProfile:
			recBytes, err := json.Marshal(rec)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				return nil
			}
			entity := fmt.Sprintf(`{"uri":"at://%s/%s","content":%s}`, repoDID, path, string(recBytes))
			profile = string(entity)
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

	c.Data(http.StatusOK, "application/json", []byte(repoJSON))
}
