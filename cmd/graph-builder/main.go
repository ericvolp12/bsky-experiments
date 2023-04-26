package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/events"
	intEvents "github.com/ericvolp12/bsky-experiments/pkg/events"
	"github.com/ericvolp12/bsky-experiments/pkg/graph"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	includeLinks := os.Getenv("INCLUDE_LINKS") == "true"

	bsky, err := intEvents.NewBSky(ctx, includeLinks)
	if err != nil {
		log.Fatal(err)
	}

	mentionFile := os.Getenv("MENTIONS_FILE")
	if mentionFile == "" {
		mentionFile = "mention-counts.txt"
	}

	replyFile := os.Getenv("REPLY_FILE")
	if replyFile == "" {
		replyFile = "reply-counts.txt"
	}

	graphFile := os.Getenv("GRAPH_FILE")
	if graphFile == "" {
		graphFile = "social-graph.txt"
	}

	resumedGraph, err := graph.ReadGraph(graphFile)
	if err != nil {
		log.Printf("error reading social graph: %s\n", err)
	} else {
		bsky.SocialGraph = resumedGraph
	}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	graphTicker := time.NewTicker(30 * time.Second)
	quit := make(chan struct{})

	// Run a routine that dumps graph data to a file every 30 seconds
	go func() {
		binReaderWriter := graph.BinaryGraphReaderWriter{}
		for {
			select {
			case <-graphTicker.C:
				graphTracker(bsky, &binReaderWriter, mentionFile, replyFile, graphFile)
			case <-quit:
				graphTicker.Stop()
				return
			}
		}
	}()

	authTicker := time.NewTicker(10 * time.Minute)

	// Run a routine that refreshes the auth token every 10 minutes
	go func() {
		for {
			select {
			case <-authTicker.C:
				fmt.Printf("\u001b[90m[%s]\u001b[32m refreshing auth token...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				err := bsky.RefreshAuthToken(ctx)
				if err != nil {
					log.Printf("error refreshing auth token: %s", err)
				} else {
					fmt.Printf("\u001b[90m[%s]\u001b[32m auth token refreshed successfully\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
				}
			case <-quit:
				authTicker.Stop()
				return
			}
		}
	}()

	// Server for pprof and prometheus via promhttp
	go func() {
		// Create a handler to write out the plaintext graph
		http.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
			fmt.Printf("\u001b[90m[%s]\u001b[32m writing graph to HTTP Response...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
			bsky.SocialGraphMux.Lock()
			defer bsky.SocialGraphMux.Unlock()

			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Disposition", "attachment; filename=social-graph.txt")
			w.Header().Set("Content-Transfer-Encoding", "binary")
			w.Header().Set("Expires", "0")
			w.Header().Set("Cache-Control", "must-revalidate")
			w.Header().Set("Pragma", "public")

			err := bsky.SocialGraph.Write(w)
			if err != nil {
				log.Printf("error writing graph: %s", err)
			} else {
				fmt.Printf("\u001b[90m[%s]\u001b[32m graph written to HTTP Response successfully\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
			}
		})

		http.Handle("/metrics", promhttp.Handler())
		fmt.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	events.HandleRepoStream(ctx, c, &events.RepoStreamCallbacks{
		RepoCommit: bsky.HandleRepoCommit,
		RepoInfo:   intEvents.HandleRepoInfo,
		Error:      intEvents.HandleError,
	})
}

func getHalfHourFileName(baseName string) string {
	now := time.Now()
	min := now.Minute()
	halfHourSuffix := "00"
	if min >= 30 {
		halfHourSuffix = "30"
	}

	fileExt := filepath.Ext(baseName)
	fileName := strings.TrimSuffix(baseName, fileExt)

	return fmt.Sprintf("%s-%s_%s%s", fileName, now.Format("2006_01_02_15"), halfHourSuffix, fileExt)
}

func graphTracker(bsky *intEvents.BSky, binReaderWriter *graph.BinaryGraphReaderWriter, mentionFile, replyFile, graphFileFromEnv string) {
	fmt.Printf("\u001b[90m[%s]\u001b[32m writing mention counts to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))

	// Acquire locks on the data structures we're reading from
	bsky.SocialGraphMux.Lock()
	defer bsky.SocialGraphMux.Unlock()
	bsky.MentionCounterMapMux.Lock()
	defer bsky.MentionCounterMapMux.Unlock()
	bsky.ReplyCounterMapMux.Lock()
	defer bsky.ReplyCounterMapMux.Unlock()

	writeCountsToFile(bsky.MentionCounterMap, mentionFile, "mention")

	fmt.Printf("\u001b[90m[%s]\u001b[32m writing reply counts to file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))

	writeCountsToFile(bsky.ReplyCounterMap, replyFile, "reply")

	fmt.Printf("\u001b[90m[%s]\u001b[32m writing social graph to plaintext file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))

	timestampedGraphFilePath := getHalfHourFileName(graphFileFromEnv)

	err := bsky.SocialGraph.WriteGraph(timestampedGraphFilePath)
	if err != nil {
		log.Printf("error writing social graph to plaintext file: %s", err)
	}

	err = copyFile(timestampedGraphFilePath, graphFileFromEnv)
	if err != nil {
		log.Printf("error copying binary file: %s", err)
	}

	fmt.Printf("\u001b[90m[%s]\u001b[32m writing social graph to binary file...\u001b[0m\n", time.Now().Format("02.01.06 15:04:05"))
	// Append a timestmap to the filename with 30-minute precision

	binFileFromEnv := os.Getenv("BINARY_GRAPH_FILE")
	if binFileFromEnv == "" {
		binFileFromEnv = "social-graph.bin"
	}

	timestampedBinFilePath := getHalfHourFileName(binFileFromEnv)

	err = binReaderWriter.WriteGraph(bsky.SocialGraph, timestampedBinFilePath)
	if err != nil {
		log.Printf("error writing social graph to binary file: %s", err)
	}

	err = copyFile(timestampedBinFilePath, binFileFromEnv)
	if err != nil {
		log.Printf("error copying binary file: %s", err)
	}
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	return destinationFile.Sync()
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
