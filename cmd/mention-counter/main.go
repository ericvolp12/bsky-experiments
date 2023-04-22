package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/bluesky-social/indigo/events"
	intEvents "github.com/ericvolp12/bsky-experiments/pkg/events"
	"github.com/gorilla/websocket"
)

// Count is used for sorting and storing mention counts
type Count struct {
	Handle string
	Count  int
}

func main() {
	ctx := context.Background()
	// Replace with the WebSocket URL you want to connect to.
	u := url.URL{Scheme: "wss", Host: "bsky.social", Path: "/xrpc/com.atproto.sync.subscribeRepos"}

	bsky, err := intEvents.NewBSky()
	if err != nil {
		log.Fatal(err)
	}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	ticker := time.NewTicker(30 * time.Second)
	quit := make(chan struct{})

	// Run a routine that dumps mention counts to a file every 30 seconds
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("\u001b[90m[%s]\u001b[32m writing mention and reply counts to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				mentionFile := os.Getenv("MENTIONS_FILE")
				if mentionFile == "" {
					mentionFile = "mention-counts.txt"
				}
				mentionFileContents := ""
				// Sort mentions by count, descending
				sortedCounts := []Count{}
				for k, v := range bsky.MentionCounters {
					sortedCounts = append(sortedCounts, Count{k, v})
				}

				sort.Slice(sortedCounts, func(i, j int) bool {
					return sortedCounts[i].Count > sortedCounts[j].Count
				})

				for _, count := range sortedCounts {
					mentionFileContents += fmt.Sprintf("%s: %d\n", count.Handle, count.Count)
				}

				err := ioutil.WriteFile(mentionFile, []byte(mentionFileContents), 0644)
				if err != nil {
					fmt.Printf("error writing mention counts to file: %s", err)
				}

				replyFile := os.Getenv("REPLY_FILE")
				if replyFile == "" {
					replyFile = "reply-counts.txt"
				}
				replyFileContents := ""
				// Sort replies by count, descending
				sortedReplyCounts := []Count{}
				for k, v := range bsky.ReplyCounters {
					sortedReplyCounts = append(sortedReplyCounts, Count{k, v})
				}

				sort.Slice(sortedReplyCounts, func(i, j int) bool {
					return sortedReplyCounts[i].Count > sortedReplyCounts[j].Count
				})

				for _, count := range sortedReplyCounts {
					replyFileContents += fmt.Sprintf("%s: %d\n", count.Handle, count.Count)
				}

				err = ioutil.WriteFile(replyFile, []byte(replyFileContents), 0644)
				if err != nil {
					fmt.Printf("error writing reply counts to file: %s", err)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	events.HandleRepoStream(ctx, c, &events.RepoStreamCallbacks{
		RepoCommit: bsky.HandleRepoCommit,
		RepoInfo:   intEvents.HandleRepoInfo,
		Error:      intEvents.HandleError,
	})
}
