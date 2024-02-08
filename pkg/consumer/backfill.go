package consumer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/repo"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/ipfs/go-cid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type BackfillRepoStatus struct {
	RepoDid      string
	Seq          int64
	State        string
	DeleteBuffer []*Delete
	lk           sync.Mutex
}

func (b *BackfillRepoStatus) SetState(state string) {
	b.lk.Lock()
	b.State = state
	b.lk.Unlock()
}

func (b *BackfillRepoStatus) AddDelete(repo, path string) {
	b.lk.Lock()
	b.DeleteBuffer = append(b.DeleteBuffer, &Delete{repo: repo, path: path})
	b.lk.Unlock()
}

func (c *Consumer) FlushBackfillBuffer(ctx context.Context, bf *BackfillRepoStatus) int {
	ctx, span := tracer.Start(ctx, "FlushBackfillBuffer")
	defer span.End()
	log := c.Logger.With("source", "backfill_buffer_flush", "repo", bf.RepoDid)

	processed := 0

	bf.lk.Lock()
	for _, del := range bf.DeleteBuffer {
		err := c.HandleDeleteRecord(ctx, del.repo, del.path)
		if err != nil {
			log.Errorf("failed to handle delete record: %+v", err)
		}
		backfillDeletesBuffered.WithLabelValues(c.SocketURL).Dec()
		processed++
	}
	bf.DeleteBuffer = []*Delete{}
	bf.lk.Unlock()

	return processed
}

type RecordJob struct {
	RecordPath string
	NodeCid    cid.Cid
}

type RecordResult struct {
	RecordPath string
	Error      error
}

func (c *Consumer) BackfillProcessor(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "BackfillProcessor")
	defer span.End()

	log := c.Logger.With("source", "backfill_main")
	log.Info("starting backfill processor")

	// Create a semaphore with a capacity of 50
	sem := make(chan struct{}, 50)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping backfill processor")
			return
		default:
		}

		// Get the next backfill
		var backfill *BackfillRepoStatus
		c.BackfillStatus.Range(func(repo string, b *BackfillRepoStatus) bool {
			b.lk.Lock()
			if b.State == "enqueued" {
				backfill = b
				b.State = "in_progress"
				b.lk.Unlock()
				return false
			}
			b.lk.Unlock()
			return true
		})

		if backfill == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		sem <- struct{}{} // Block until there is a slot in the semaphore
		go func(b *BackfillRepoStatus) {
			// Process the backfill
			c.ProcessBackfill(ctx, b.RepoDid)
			backfillJobsProcessed.WithLabelValues(c.SocketURL).Inc()
			<-sem // Release a slot in the semaphore when the goroutine finishes
		}(backfill)
	}
}

type instrumentedReader struct {
	source  io.ReadCloser
	counter prometheus.Counter
}

func (r instrumentedReader) Read(b []byte) (int, error) {
	n, err := r.source.Read(b)
	r.counter.Add(float64(n))
	return n, err
}

func (r instrumentedReader) Close() error {
	var buf [32]byte
	var n int
	var err error
	for err == nil {
		n, err = r.source.Read(buf[:])
		r.counter.Add(float64(n))
	}
	closeerr := r.source.Close()
	if err != nil && err != io.EOF {
		return err
	}
	return closeerr
}

func (c *Consumer) ProcessBackfill(ctx context.Context, repoDID string) {
	ctx, span := tracer.Start(ctx, "ProcessBackfill")
	defer span.End()

	start := time.Now()

	log := c.Logger.With("source", "backfill", "repo", repoDID)
	log.Infof("processing backfill for %s", repoDID)

	var url = "https://bsky.network/xrpc/com.atproto.sync.getRepo?did=" + repoDID

	// GET and CAR decode the body
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   15 * time.Minute,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Errorf("Error creating request: %v", err)
		return
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "jaz-atproto-backfill/0.0.1")
	if c.magicHeaderKey != "" && c.magicHeaderVal != "" {
		req.Header.Set(c.magicHeaderKey, c.magicHeaderVal)
	}

	c.SyncLimiter.Wait(ctx)

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Error sending request: %v", err)
		return
	}

	bf, _ := c.BackfillStatus.Load(repoDID)

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Error response: %v", resp.StatusCode)
		reason := "unknown error"
		if resp.StatusCode == http.StatusBadRequest {
			reason = "repo not found"
		}
		state := fmt.Sprintf("failed (%s)", reason)

		// Mark the backfill as "failed"
		bf.SetState(state)
		err = c.Store.Queries.UpdateRepoBackfillRecord(ctx, store_queries.UpdateRepoBackfillRecordParams{
			Repo:         repoDID,
			LastBackfill: time.Now(),
			SeqStarted:   bf.Seq,
			State:        state,
		})
		if err != nil {
			log.Errorf("failed to update repo backfill record: %+v", err)
		}

		// Process buffered deletes
		c.FlushBackfillBuffer(ctx, bf)

		return
	}

	instrumentedReader := instrumentedReader{
		source:  resp.Body,
		counter: backfillBytesProcessed.WithLabelValues(c.SocketURL),
	}

	defer instrumentedReader.Close()

	r, err := repo.ReadRepoFromCar(ctx, instrumentedReader)
	if err != nil {
		log.Errorf("Error reading repo: %v", err)
		// Mark the backfill as "failed"
		bf, _ := c.BackfillStatus.Load(repoDID)

		state := "failed (couldn't read repo CAR from response body)"

		// Mark the backfill as "failed"
		bf.SetState(state)
		err = c.Store.Queries.UpdateRepoBackfillRecord(ctx, store_queries.UpdateRepoBackfillRecordParams{
			Repo:         repoDID,
			LastBackfill: time.Now(),
			SeqStarted:   bf.Seq,
			State:        state,
		})
		if err != nil {
			log.Errorf("failed to update repo backfill record: %+v", err)
		}

		// Process buffered deletes
		c.FlushBackfillBuffer(ctx, bf)

		return
	}

	numRecords := 0
	numRoutines := 50
	recordJobs := make(chan RecordJob, numRoutines)
	recordResults := make(chan RecordResult, numRoutines)

	wg := sync.WaitGroup{}

	// Producer routine
	go func() {
		defer close(recordJobs)
		r.ForEach(ctx, "", func(recordPath string, nodeCid cid.Cid) error {
			numRecords++
			recordJobs <- RecordJob{RecordPath: recordPath, NodeCid: nodeCid}
			return nil
		})
	}()

	// Consumer routines
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range recordJobs {
				recordCid, rec, err := r.GetRecord(ctx, job.RecordPath)
				if err != nil {
					log.Errorf("Error getting record: %v", err)
					recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
					continue
				}

				// Verify that the record cid matches the cid in the event
				if recordCid != job.NodeCid {
					log.Errorf("mismatch in record and op cid: %s != %s", recordCid, job.NodeCid)
					recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
					continue
				}

				_, err = c.HandleCreateRecord(ctx, repoDID, job.RecordPath, rec)
				if err != nil {
					log.Errorf("failed to handle create record: %+v", err)
				}

				backfillRecordsProcessed.WithLabelValues(c.SocketURL).Inc()
				recordResults <- RecordResult{RecordPath: job.RecordPath, Error: err}
			}
		}()
	}

	resultWG := sync.WaitGroup{}
	resultWG.Add(1)
	// Handle results
	go func() {
		defer resultWG.Done()
		for result := range recordResults {
			if result.Error != nil {
				log.Errorf("Error processing record %s: %v", result.RecordPath, result.Error)
			}
		}
	}()

	wg.Wait()
	close(recordResults)
	resultWG.Wait()

	state := "complete"

	// Mark the backfill as "complete"
	bf.SetState(state)

	// Process buffered deletes
	numProcessed := c.FlushBackfillBuffer(ctx, bf)

	err = c.Store.Queries.UpdateRepoBackfillRecord(ctx, store_queries.UpdateRepoBackfillRecordParams{
		Repo:         repoDID,
		LastBackfill: time.Now(),
		SeqStarted:   bf.Seq,
		State:        state,
	})
	if err != nil {
		log.Errorf("failed to update repo backfill record: %+v", err)
	}

	log.Infow("backfill complete", "buffered_deletes_processed", numProcessed, "records_backfilled", numRecords, "duration", time.Since(start))
}
