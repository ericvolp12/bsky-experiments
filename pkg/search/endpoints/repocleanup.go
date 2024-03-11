package endpoints

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/semaphore"
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

	res, err := api.enqueueCleanupJob(ctx, req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error cleaning up records: %w", err).Error()})
		return
	}

	c.JSON(http.StatusOK, res)
	return
}

func (api *API) GetCleanupStatus(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetCleanupStatus")
	defer span.End()

	jobID := c.Query("job_id")
	if jobID != "" {
		job, err := api.Store.Queries.GetRepoCleanupJob(ctx, jobID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error getting job: %w", err).Error()})
			return
		}
		job.RefreshToken = ""
		c.JSON(http.StatusOK, job)
		return
	}

	did := c.Query("did")
	if did != "" {
		jobs, err := api.Store.Queries.GetCleanupJobsByRepo(ctx, store_queries.GetCleanupJobsByRepoParams{Repo: did, Limit: 100})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"error": "no jobs found for the provided DID"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error getting jobs: %w", err).Error()})
			return
		}
		for i := range jobs {
			jobs[i].RefreshToken = ""
		}
		c.JSON(http.StatusOK, jobs)
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{"error": "must specify either job_id or did in query params"})
	return
}

func (api *API) GetCleanupStats(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetCleanupStats")
	defer span.End()

	cleanupStats, err := api.Store.Queries.GetCleanupStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error getting cleanup metadata: %w", err).Error()})
		return
	}

	c.JSON(http.StatusOK, cleanupStats)
}

func (api *API) CancelCleanupJob(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "CancelCleanupJob")
	defer span.End()

	jobID := c.Query("job_id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "must specify job_id in query params"})
		return
	}

	job, err := api.Store.Queries.GetRepoCleanupJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error getting job: %w", err).Error()})
		return
	}

	if job.JobState == "completed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job already completed"})
		return
	}

	job.JobState = "cancelled"
	job.RefreshToken = ""
	job.UpdatedAt = time.Now().UTC()

	_, err = api.Store.Queries.UpsertRepoCleanupJob(ctx, store_queries.UpsertRepoCleanupJobParams{
		JobID:           job.JobID,
		Repo:            job.Repo,
		RefreshToken:    job.RefreshToken,
		CleanupTypes:    job.CleanupTypes,
		DeleteOlderThan: job.DeleteOlderThan,
		NumDeleted:      job.NumDeleted,
		NumDeletedToday: job.NumDeletedToday,
		EstNumRemaining: job.EstNumRemaining,
		JobState:        job.JobState,
		UpdatedAt:       job.UpdatedAt,
		LastDeletedAt:   job.LastDeletedAt,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error updating job: %w", err).Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "job cancelled"})
	return
}

type cleanupInfo struct {
	NumEnqueued int    `json:"num_enqueued"`
	DryRun      bool   `json:"dry_run"`
	Message     string `json:"message,omitempty"`
	JobID       string `json:"job_id,omitempty"`
}

func (api *API) enqueueCleanupJob(ctx context.Context, req CleanupOldRecordsRequest) (*cleanupInfo, error) {
	log := slog.With("source", "cleanup_old_records_handler")

	// Create a new client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   5 * time.Minute,
		},
		Host:      "https://bsky.social",
		UserAgent: &cleanupUserAgent,
	}

	if api.MagicHeaderVal != "" {
		client.Headers = map[string]string{"x-ratelimit-bypass": api.MagicHeaderVal}
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

	// Check if we have a pending job for this DID
	existingJobs, err := api.Store.Queries.GetRunningCleanupJobsByRepo(ctx, out.Did)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Error("Error getting existing jobs", "error", err)
		}
	} else if len(existingJobs) > 0 {
		log.Info("Found existing job for DID, skipping")
		return &cleanupInfo{
			JobID:   existingJobs[0].JobID,
			Message: "Found existing active job for your account, you can only have one job running at a time.",
		}, nil
	}

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
		// Skip threadgates
		if strings.Contains(path, "threadgate") {
			return nil
		}
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Error("Error getting record", "error", err)
			return nil
		}

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}
			if createdAt.After(time.Now().AddDate(0, 0, -req.DeleteUntilDaysAgo)) {
				return nil
			}

			hasMedia := rec.Embed != nil && rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0

			if hasMedia {
				if slices.Contains(req.CleanupTypes, "post_with_media") {
					lk.Lock()
					recordsToDelete = append(recordsToDelete, path)
					lk.Unlock()
				}
				return nil
			}

			if slices.Contains(req.CleanupTypes, "post") {
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

	info := cleanupInfo{
		NumEnqueued: len(recordsToDelete),
		DryRun:      !req.ActuallyDeleteStuff,
		Message:     "Records will not be deleted",
	}
	if req.ActuallyDeleteStuff {
		// Create a new jobParams
		jobParams := store_queries.UpsertRepoCleanupJobParams{
			JobID:           uuid.New().String(),
			Repo:            did.String(),
			RefreshToken:    out.RefreshJwt,
			CleanupTypes:    req.CleanupTypes,
			DeleteOlderThan: time.Now().UTC().AddDate(0, 0, -req.DeleteUntilDaysAgo),
			NumDeleted:      0,
			NumDeletedToday: 0,
			EstNumRemaining: int32(len(recordsToDelete)),
			JobState:        "running",
			UpdatedAt:       time.Now().UTC(),
		}

		job, err := api.Store.Queries.UpsertRepoCleanupJob(ctx, jobParams)
		if err != nil {
			log.Error("Error creating job", "error", err)
			return nil, fmt.Errorf("error creating job: %w", err)
		}

		info.Message = fmt.Sprintf("%d records enqueued for deletion in job at: https://bsky-search.jazco.io/repo/cleanup?job_id=%s", len(recordsToDelete), job.JobID)
		info.JobID = job.JobID
		log.Info("Created cleanup job", "job_id", job.JobID)
	}

	return &info, nil
}

// RunCleanupDaemon runs a daemon that periodically checks for cleanup jobs to run
func (api *API) RunCleanupDaemon(ctx context.Context) {
	log := slog.With("source", "cleanup_daemon")
	log.Info("Starting cleanup daemon")

	for {
		log.Info("Getting jobs to process")
		jobs, err := api.Store.Queries.GetRunningCleanupJobs(ctx, 100)
		if err != nil {
			log.Error("Error getting running jobs", "error", err)
			time.Sleep(30 * time.Second)
			continue
		}

		// Filter for jobs that haven't had a deletion run in the last hour and haven't hit the max for the day
		jobsToRun := []*store_queries.RepoCleanupJob{}
		anHourAgo := time.Now().UTC().Add(-1 * time.Hour)
		aDayAgo := time.Now().UTC().Add(-24 * time.Hour)
		for i := range jobs {
			job := jobs[i]
			if !job.LastDeletedAt.Valid || // Add new jobs
				(job.LastDeletedAt.Time.Before(anHourAgo) && job.NumDeletedToday < int32(maxDeletesPerDay)) ||
				(job.LastDeletedAt.Time.Before(aDayAgo)) {
				jobsToRun = append(jobsToRun, &job)
			}
		}

		log.Info("Found jobs to run", "count", len(jobsToRun))

		if len(jobsToRun) == 0 {
			log.Info("No jobs to run, sleeping")
			time.Sleep(30 * time.Second)
			continue
		}

		wg := sync.WaitGroup{}
		maxConcurrent := 10
		sem := semaphore.NewWeighted(int64(maxConcurrent))
		for i := range jobsToRun {
			job := jobsToRun[i]
			wg.Add(1)
			sem.Acquire(ctx, 1)
			go func(job store_queries.RepoCleanupJob) {
				defer func() {
					sem.Release(1)
					wg.Done()
				}()
				log := log.With("job_id", job.JobID, "did", job.Repo)
				resJob, err := api.cleanupNextBatch(ctx, job)
				if err != nil {
					log.Error("Error cleaning up batch", "error", err)
					resJob = &job
					resJob.LastDeletedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
					resJob.UpdatedAt = time.Now().UTC()
					return
				}

				if resJob != nil {
					_, err = api.Store.Queries.UpsertRepoCleanupJob(ctx, store_queries.UpsertRepoCleanupJobParams{
						JobID:           resJob.JobID,
						Repo:            resJob.Repo,
						RefreshToken:    resJob.RefreshToken,
						CleanupTypes:    resJob.CleanupTypes,
						DeleteOlderThan: resJob.DeleteOlderThan,
						NumDeleted:      resJob.NumDeleted,
						NumDeletedToday: resJob.NumDeletedToday,
						EstNumRemaining: resJob.EstNumRemaining,
						JobState:        resJob.JobState,
						UpdatedAt:       resJob.UpdatedAt,
						LastDeletedAt:   resJob.LastDeletedAt,
					})
					if err != nil {
						log.Error("Error updating job", "error", err)
						return
					}
				}
			}(*job)
		}

		wg.Wait()
		log.Info("Finished running jobs, sleeping")
		time.Sleep(30 * time.Second)
	}
}

var maxDeletesPerHour = 4000
var maxDeletesPerDay = 30_000

func (api *API) cleanupNextBatch(ctx context.Context, job store_queries.RepoCleanupJob) (*store_queries.RepoCleanupJob, error) {
	log := slog.With("source", "cleanup_next_batch", "job_id", job.JobID, "did", job.Repo)
	log.Info("Cleaning up next batch")
	// If the last deletion job ran today and we're at the max for the day, return
	if job.LastDeletedAt.Time.UTC().Day() == time.Now().UTC().Day() && job.NumDeletedToday >= int32(maxDeletesPerDay) {
		log.Info("Already deleted max records today, skipping")
		return nil, nil
	} else if job.LastDeletedAt.Time.UTC().Day() != time.Now().UTC().Day() {
		// If the last deletion job ran on a different day, reset the daily counter
		job.NumDeletedToday = 0
	}

	// Create a new client
	client := xrpc.Client{
		Client: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   5 * time.Minute,
		},
		Host:      "https://bsky.social",
		UserAgent: &cleanupUserAgent,
	}

	if api.MagicHeaderVal != "" {
		client.Headers = map[string]string{"x-ratelimit-bypass": api.MagicHeaderVal}
	}

	ident, err := api.Directory.LookupDID(ctx, syntax.DID(job.Repo))
	if err != nil {
		log.Error("Error looking up DID", "error", err)
		return nil, fmt.Errorf("error looking up DID: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:       job.Repo,
		AccessJwt: job.RefreshToken,
	}

	out, err := comatproto.ServerRefreshSession(ctx, &client)
	if err != nil {
		log.Error("Error refreshing session", "error", err)
		if strings.Contains(err.Error(), "ExpiredToken") {
			job.RefreshToken = ""
			job.JobState = "errored: ExpiredToken"
			job.UpdatedAt = time.Now().UTC()
			return &job, nil
		}
		if strings.Contains(err.Error(), "Could not find user info for account") {
			job.RefreshToken = ""
			job.JobState = "errored: Could not find user info for account (account may have been deleted)"
			job.UpdatedAt = time.Now().UTC()
			return &job, nil
		}
		return nil, fmt.Errorf("error refreshing session: %w", err)
	}

	client.Auth = &xrpc.AuthInfo{
		Did:        job.Repo,
		Handle:     out.Handle,
		RefreshJwt: out.RefreshJwt,
		AccessJwt:  out.AccessJwt,
	}

	job.RefreshToken = out.RefreshJwt

	// Use the user's PDS for further requests
	client.Host = ident.PDSEndpoint()

	log = log.With("pds", client.Host)

	// Get the user's Repo from the PDS
	repoBytes, err := comatproto.SyncGetRepo(ctx, &client, job.Repo, "")
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
		// Skip threadgates
		if strings.Contains(path, "threadgate") {
			return nil
		}
		_, rec, err := rr.GetRecord(ctx, path)
		if err != nil {
			log.Error("Error getting record", "error", err)
			return nil
		}

		switch rec := rec.(type) {
		case *bsky.FeedPost:
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}
			if createdAt.After(job.DeleteOlderThan) {
				return nil
			}

			hasMedia := rec.Embed != nil && rec.Embed.EmbedImages != nil && len(rec.Embed.EmbedImages.Images) > 0

			if hasMedia {
				if slices.Contains(job.CleanupTypes, "post_with_media") {
					lk.Lock()
					recordsToDelete = append(recordsToDelete, path)
					lk.Unlock()
				}
				return nil
			}

			if slices.Contains(job.CleanupTypes, "post") {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedRepost:
			if !slices.Contains(job.CleanupTypes, "repost") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(job.DeleteOlderThan) {
				lk.Lock()
				recordsToDelete = append(recordsToDelete, path)
				lk.Unlock()
			}
		case *bsky.FeedLike:
			if !slices.Contains(job.CleanupTypes, "like") {
				return nil
			}
			createdAt, err := dateparse.ParseAny(rec.CreatedAt)
			if err != nil {
				log.Error("Error parsing date", "error", err)
				return nil
			}

			if createdAt.Before(job.DeleteOlderThan) {
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

	// Create delete batches of 10 records each for up to 4,000 records
	deleteBatches := []*comatproto.RepoApplyWrites_Input{}
	batchNum := 0
	for i, path := range recordsToDelete {
		if i > maxDeletesPerHour || job.NumDeletedToday+int32(i) > int32(maxDeletesPerDay) {
			break
		}
		if i%10 == 0 {
			if i != 0 {
				batchNum++
			}
			nextBatch := comatproto.RepoApplyWrites_Input{
				Repo:   job.Repo,
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

	numDeleted := 0

	limiter := rate.NewLimiter(rate.Limit(4), 1)
	for _, batch := range deleteBatches {
		limiter.Wait(ctx)
		err := comatproto.RepoApplyWrites(ctx, &client, batch)
		if err != nil {
			log.Error("Error applying writes", "error", err)
			err = fmt.Errorf("Errored out after deleting (%d) records: %w", numDeleted, err)
			return nil, err
		}
		numDeleted += len(batch.Writes)
	}

	estRemaining := len(recordsToDelete) - numDeleted

	log.Info("Deleted records",
		"count", numDeleted,
		"total", len(recordsToDelete),
		"est_remaining", estRemaining,
	)

	job.NumDeletedToday += int32(numDeleted)
	job.NumDeleted += int32(numDeleted)
	job.EstNumRemaining = int32(estRemaining)
	job.UpdatedAt = time.Now().UTC()
	job.LastDeletedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}

	if job.EstNumRemaining <= 0 {
		job.RefreshToken = ""
		job.JobState = "completed"
	}

	return &job, nil
}
