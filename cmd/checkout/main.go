package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
		log.Printf("Error parsing DID: %v", err)
		return fmt.Errorf("Error parsing DID: %v", err)
	}

	url := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", cctx.String("pds-host"), did.String())

	outputDir := cctx.String("output-dir")

	if outputDir == "./<repo-did>" {
		outputDir = fmt.Sprintf("./%s", did.String())
		outputDir, err = filepath.Abs(outputDir)
		if err != nil {
			log.Printf("Error getting absolute path: %v", err)
			return fmt.Errorf("Error getting absolute path: %v", err)
		}

		// Create the directory if it doesn't exist
		err = os.MkdirAll(outputDir, 0755)
		if err != nil {
			log.Printf("Error creating directory: %v", err)
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
		log.Printf("Error creating request: %v", err)
		return fmt.Errorf("Error creating request: %v", err)
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "jaz-repo-checkout/0.0.1")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending request: %v", err)
		return fmt.Errorf("Error sending request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error response: %v", resp.StatusCode)
		return fmt.Errorf("Error response: %v", resp.StatusCode)
	}

	r, err := repo.ReadRepoFromCar(ctx, resp.Body)
	if err != nil {
		log.Printf("Error reading repo: %v", err)
		return fmt.Errorf("Error reading repo: %v", err)
	}

	err = resp.Body.Close()
	if err != nil {
		log.Printf("Error closing response body: %v", err)
		return fmt.Errorf("Error closing response body: %v", err)
	}

	r.ForEach(ctx, "", func(path string, nodeCid cid.Cid) error {
		recordCid, rec, err := r.GetRecordBytes(ctx, path)
		if err != nil {
			log.Printf("Error getting record: %v", err)
			return nil
		}

		// Verify that the record cid matches the cid in the event
		if recordCid != nodeCid {
			log.Printf("mismatch in record and op cid: %s != %s", recordCid, nodeCid)
			return nil
		}

		parts := strings.Split(path, "/")
		if len(parts) != 2 {
			log.Printf("path does not have 2 parts: %s", path)
			return nil
		}

		collection := parts[0]
		rkey := parts[1]

		// Create a directory for the collection if it doesn't exist
		collectionDir := filepath.Join(outputDir, collection)
		err = os.MkdirAll(collectionDir, 0755)
		if err != nil {
			log.Printf("Error creating collection directory: %v", err)
		}

		// Write the record to a file as JSON
		recordPath := filepath.Join(collectionDir, fmt.Sprintf("%s.json", rkey))
		asCbor, err := data.UnmarshalCBOR(*rec)
		if err != nil {
			return fmt.Errorf("failed to unmarshal record: %w", err)
		}

		recJSON, err := json.Marshal(asCbor)
		if err != nil {
			return fmt.Errorf("failed to marshal record to json: %w", err)
		}

		err = os.WriteFile(recordPath, recJSON, 0644)
		if err != nil {
			log.Printf("Error writing record to file: %v", err)
		}

		return nil
	})

	// Compress the directory into a gzip file
	if cctx.Bool("compress") {
		err = compressDirectory(outputDir)
		if err != nil {
			log.Printf("Error compressing directory: %v", err)
			return fmt.Errorf("Error compressing directory: %v", err)
		}

		// Delete the intermediate files
		err = os.RemoveAll(outputDir)
		if err != nil {
			log.Printf("Error deleting intermediate files: %v", err)
			return fmt.Errorf("Error deleting intermediate files: %v", err)
		}
	}

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
