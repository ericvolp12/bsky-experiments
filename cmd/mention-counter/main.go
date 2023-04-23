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
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
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

	graphFile := os.Getenv("GRAPH_FILE")
	if graphFile == "" {
		graphFile = "social-graph.txt"
	}

	resumedGraph, err := graph.ReadGraph(graphFile)
	if err != nil {
		fmt.Printf("error reading social graph: %s\n", err)
	} else {
		bsky.SocialGraph = resumedGraph
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
				fmt.Printf("\u001b[90m[%s]\u001b[32m writing mention counts to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				mentionFile := os.Getenv("MENTIONS_FILE")
				if mentionFile == "" {
					mentionFile = "mention-counts.txt"
				}
				writeCountsToFile(bsky.MentionCounters, mentionFile, "mention")

				fmt.Printf("\u001b[90m[%s]\u001b[32m writing reply counts to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				replyFile := os.Getenv("REPLY_FILE")
				if replyFile == "" {
					replyFile = "reply-counts.txt"
				}
				writeCountsToFile(bsky.ReplyCounters, replyFile, "reply")

				fmt.Printf("\u001b[90m[%s]\u001b[32m writing social graph to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				graphFile := os.Getenv("GRAPH_FILE")
				if graphFile == "" {
					graphFile = "social-graph.txt"
				}
				err := bsky.SocialGraph.WriteGraph(graphFile)
				if err != nil {
					fmt.Printf("error writing social graph to file: %s", err)
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

func writeCountsToFile(counters map[string]int, filename, label string) {
	contents := ""

	// Sort counts by value, descending
	sortedCounts := []Count{}
	for k, v := range counters {
		sortedCounts = append(sortedCounts, Count{k, v})
	}

	sort.Slice(sortedCounts, func(i, j int) bool {
		return sortedCounts[i].Count > sortedCounts[j].Count
	})

	for _, count := range sortedCounts {
		contents += fmt.Sprintf("%s: %d\n", count.Handle, count.Count)
	}

	err := ioutil.WriteFile(filename, []byte(contents), 0644)
	if err != nil {
		fmt.Printf("error writing %s counts to file: %s", label, err)
	}
}
