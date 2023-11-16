package endpoints

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/time/rate"
)

type CleanupOldRecordsRequest struct {
	Identifier          string   `json:"identifier"`
	AppPassword         string   `json:"app_password"`
	CleanupTypes        []string `json:"cleanup_types"`
	DeleteUntilDaysAgo  int      `json:"delete_until_days_ago"`
	ActuallyDeleteStuff bool     `json:"actually_delete_stuff"`
}

var cleanupUserAgent = "jaz-repo-cleanup-tool/0.0.1"

func (api *API) CleanupOldRecords(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "CleanupOldRecords")
	defer span.End()

	var req CleanupOldRecordsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("error parsing request: %w", err).Error()})
		return
	}

	res, err := api.cleanupRecordsForAccount(ctx, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error cleaning up records: %w", err).Error()})
		return
	}

	c.JSON(http.StatusOK, res)
	return
}

type cleanupResults struct {
	NumEnqueued int    `json:"num_enqueued"`
	NumBatches  int    `json:"num_batches"`
	NumDeleted  int    `json:"num_deleted"`
	DryRun      bool   `json:"dry_run"`
	Message     string `json:"message,omitempty"`
	Error       string `json:"error,omitempty"`
}

func (api *API) cleanupRecordsForAccount(ctx context.Context, req CleanupOldRecordsRequest) (*cleanupResults, error) {
	log := slog.With("source", "cleanup_old_records_handler")

	// Create a new client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
		Host:      "https://bsky.social",
		UserAgent: &cleanupUserAgent,
	}

	log = log.With("identifier", req.Identifier)

	// Login as the user
	out, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: req.Identifier,
		Password:   req.AppPassword,
	})
	if err != nil {
		log.Error("Error logging in", "error", err)
		return nil, fmt.Errorf("error logging in: %w", err)
	}

	// Set client auth info
	client.Auth = &xrpc.AuthInfo{
		Did:        out.Did,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	log = log.With("did", out.Did)
	log = log.With("handle", out.Handle)

	// Get the user's PDS from PLC
	did, err := syntax.ParseDID(out.Did)
	if err != nil {
		log.Error("Error parsing DID", "error", err)
		return nil, fmt.Errorf("error parsing DID: %w", err)
	}

	ident, err := api.Directory.LookupDID(ctx, did)
	if err != nil {
		log.Error("Error looking up DID", "error", err)
		return nil, fmt.Errorf("error looking up DID: %w", err)
	}

	client.Host = ident.PDSEndpoint()

	if client.Host == "" {
		log.Error("No PDS endpoint found for DID")
		return nil, fmt.Errorf("no PDS endpoint found for DID")
	}

	log = log.With("pds", client.Host)

	// Get the user's Repo from the PDS
	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, did.String(), "")
	if err != nil {
		log.Error("Error getting repo from PDS", "error", err)
		return nil, fmt.Errorf("error getting repo from PDS: %w", err)
	}

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		log.Error("Error reading repo CAR", "error", err)
		return nil, fmt.Errorf("error reading repo CAR: %w", err)
	}

	recordsToDelete := []string{}
	lk := sync.Mutex{}

	// Iterate over records in the repo to find the ones to delete
	err = rr.ForEach(ctx, "app.bsky.feed.", func(path string, nodeCid cid.Cid) error {
		log := log.With("path", path)
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Error("Error getting record", "error", err)
			return nil
		}

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			if !slices.Contains(req.CleanupTypes, "post") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedRepost:
			if !slices.Contains(req.CleanupTypes, "repost") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedLike:
			if !slices.Contains(req.CleanupTypes, "like") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		}

		return nil
	})
	if err != nil {
		log.Error("Error iterating over records", "error", err)
		return nil, fmt.Errorf("error iterating over records: %w", err)
	}

	log.Info("Found records to delete", "count", len(recordsToDelete))

	// Create delete batches of 10 records each
	deleteBatches := []*comatproto.RepoApplyWrites_Input{}
	batchNum := 0
	for i, path := range recordsToDelete {
		if i%10 == 0 {
			if i != 0 {
				batchNum++
			}
			nextBatch := comatproto.RepoApplyWrites_Input{
				Repo:   did.String(),
				Writes: []*comatproto.RepoApplyWrites_Input_Writes_Elem{},
			}
			deleteBatches = append(deleteBatches, &nextBatch)
		}

		collection := strings.Split(path, "/")[0]
		rkey := strings.Split(path, "/")[1]

		deleteObj := comatproto.RepoApplyWrites_Delete{
			Collection: collection,
			Rkey:       rkey,
		}

		deleteBatch := deleteBatches[batchNum]
		deleteBatch.Writes = append(deleteBatch.Writes, &comatproto.RepoApplyWrites_Input_Writes_Elem{
			RepoApplyWrites_Delete: &deleteObj,
		})
	}

	log.Info("Created delete batches", "count", len(deleteBatches))

	res := cleanupResults{
		NumEnqueued: len(recordsToDelete),
		NumBatches:  len(deleteBatches),
	}

	if req.ActuallyDeleteStuff {
		res.DryRun = false
		// Only delete up to 4k records in one go to avoid hard rate limits on the repo
		maxBatchesDeleted := 400
		limiter := rate.NewLimiter(rate.Limit(4), 1)
		for i, batch := range deleteBatches {
			if i >= maxBatchesDeleted {
				break
			}
			limiter.Wait(ctx)
			err := comatproto.RepoApplyWrites(ctx, &client, batch)
			if err != nil {
				log.Error("Error applying writes", "error", err)
				res.Error = fmt.Sprintf("Errored out after deleting (%d) records: %v", 10*i, err)
				return &res, nil
			}
			res.NumDeleted += len(batch.Writes)
		}
		res.Message = fmt.Sprintf("Deleted (%d) records", res.NumDeleted)
		if res.NumDeleted < res.NumEnqueued {
			res.Message += fmt.Sprintf(
				"but there are still (%d) records to delete due to rate limiting, come back in an hour and delete the next batch",
				res.NumEnqueued-res.NumDeleted,
			)
		}
	} else {
		res.DryRun = true
		res.Message = "Dry run, nothing deleted"
	}

	return &res, nil
}
