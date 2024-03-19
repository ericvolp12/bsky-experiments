package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "consumer",
		Usage:   "atproto firehose consumer",
		Version: "0.0.2",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "pds-host",
			Usage:   "host of the PDS or Relay to fetch the repo from (with protocol)",
			Value:   "https://bsky.network",
			EnvVars: []string{"PDS_URL"},
		},
		&cli.StringFlag{
			Name:    "output-dir",
			Usage:   "directory to write the repo to",
			Value:   "./<repo-did>",
			EnvVars: []string{"OUTPUT_DIR"},
		},
		&cli.BoolFlag{
			Name:  "compress",
			Usage: "compress the resulting directory into a gzip file",
		},
	}

	app.ArgsUsage = "<repo-did>"

	app.Action = Checkout

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// Checkout fetches a repo from the PDS and writes it to the output directory
func Checkout(cctx *cli.Context) error {
	ctx := cctx.Context
	rawDID := cctx.Args().First()

	did, err := syntax.ParseDID(rawDID)
	if err != nil {
		slog.Error("Error parsing DID", "error", err)
		return fmt.Errorf("Error parsing DID: %v", err)
	}

	url := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", cctx.String("pds-host"), did.String())

	outputDir := cctx.String("output-dir")

	if outputDir == "./<repo-did>" {
		outputDir = fmt.Sprintf("./%s", did.String())
		outputDir, err = filepath.Abs(outputDir)
		if err != nil {
			slog.Error("Error getting absolute path", "error", err)
			return fmt.Errorf("Error getting absolute path: %v", err)
		}

		// Create the directory if it doesn't exist
		err = os.MkdirAll(outputDir, 0755)
		if err != nil {
			slog.Error("Error creating directory", "error", err)
			return fmt.Errorf("Error creating directory: %v", err)
		}
	}

	// GET and CAR decode the body
	client := &http.Client{
		Transport: http.DefaultTransport,
		Timeout:   5 * time.Minute,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		slog.Error("Error creating request", "error", err)
		return fmt.Errorf("Error creating request: %v", err)
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "jaz-repo-checkout/0.0.2")

	slog.Info("fetching repo", "did", did.String(), "url", url)

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("Error sending request", "error", err)
		return fmt.Errorf("Error sending request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("Error response", "status", resp.StatusCode)
		return fmt.Errorf("Error response: %v", resp.StatusCode)
	}

	r, err := repo.ReadRepoFromCar(ctx, resp.Body)
	if err != nil {
		slog.Error("Error reading repo", "error", err)
		return fmt.Errorf("Error reading repo: %v", err)
	}

	err = resp.Body.Close()
	if err != nil {
		slog.Error("Error closing response body", "error", err)
		return fmt.Errorf("Error closing response body: %v", err)
	}

	numRecords := 0
	numCollections := 0
	collectionsSeen := make(map[string]struct{})

	r.ForEach(ctx, "", func(path string, nodeCid cid.Cid) error {
		recordCid, rec, err := r.GetRecordBytes(ctx, path)
		if err != nil {
			slog.Error("Error getting record", "error", err)
			return nil
		}

		// Verify that the record cid matches the cid in the event
		if recordCid != nodeCid {
			slog.Error("mismatch in record and op cid", "recordCid", recordCid, "nodeCid", nodeCid)
			return nil
		}

		parts := strings.Split(path, "/")
		if len(parts) != 2 {
			slog.Error("path does not have 2 parts", "path", path)
			return nil
		}

		collection := parts[0]
		rkey := parts[1]

		// Count the number of records and collections
		numRecords++
		if _, ok := collectionsSeen[collection]; !ok {
			numCollections++
			collectionsSeen[collection] = struct{}{}
		}

		// Create a directory for the collection if it doesn't exist
		collectionDir := filepath.Join(outputDir, collection)
		err = os.MkdirAll(collectionDir, 0755)
		if err != nil {
			slog.Error("Error creating collection directory", "error", err)
			return nil
		}

		// Write the record to a file as JSON
		recordPath := filepath.Join(collectionDir, fmt.Sprintf("%s.json", rkey))
		asCbor, err := data.UnmarshalCBOR(*rec)
		if err != nil {
			slog.Error("Error unmarshalling record", "error", err)
			return fmt.Errorf("failed to unmarshal record: %w", err)
		}

		recJSON, err := json.Marshal(asCbor)
		if err != nil {
			slog.Error("Error marshalling record to json", "error", err)
			return fmt.Errorf("failed to marshal record to json: %w", err)
		}

		err = os.WriteFile(recordPath, recJSON, 0644)
		if err != nil {
			slog.Error("Error writing record to file", "error", err)
		}

		return nil
	})

	// Compress the directory into a gzip file
	if cctx.Bool("compress") {
		err = compressDirectory(outputDir)
		if err != nil {
			slog.Error("Error compressing directory", "error", err)
			return fmt.Errorf("Error compressing directory: %v", err)
		}

		// Delete the intermediate files
		err = os.RemoveAll(outputDir)
		if err != nil {
			slog.Error("Error deleting intermediate files", "error", err)
			return fmt.Errorf("Error deleting intermediate files: %v", err)
		}
	}

	slog.Info("checkout complete", "outputDir", outputDir, "numRecords", numRecords, "numCollections", numCollections)

	return nil
}

func compressDirectory(dir string) error {
	// Create the tar.gz file
	tarGzFile := dir + ".tar.gz"
	file, err := os.Create(tarGzFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create the gzip writer
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	// Create the tar writer
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	// Walk through the directory and add files to the tar archive
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory
		if path == dir {
			return nil
		}

		// Create a tar header
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		// Update the name in the header to be relative to the directory
		header.Name = filepath.ToSlash(strings.TrimPrefix(path, dir+string(filepath.Separator)))

		// Write the header to the tar archive
		err = tarWriter.WriteHeader(header)
		if err != nil {
			return err
		}

		// If it's a regular file, copy its contents to the tar archive
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarWriter, file)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
