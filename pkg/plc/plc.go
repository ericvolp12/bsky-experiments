package plc

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var plcDirectoryRequestHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "plc_directory_request_duration_seconds",
	Help: "Histogram of the time (in seconds) each request to the PLC directory takes",
}, []string{"status_code"})

type Directory struct {
	Endpoint       string
	PLCRateLimiter *rate.Limiter
	PDSRateLimiter *rate.Limiter
	CheckPeriod    time.Duration
	AfterCursor    time.Time
	Logger         *zap.SugaredLogger

	ValidationTTL time.Duration

	RedisClient *redis.Client
	RedisPrefix string

	Store    *store.Store
	validate bool
}

type DirectoryEntry struct {
	Did string `json:"did"`
	AKA string `json:"handle"`
}

type RawDirectoryEntry struct {
	JSON json.RawMessage
}

type DirectoryJSONLRow struct {
	Did       string    `json:"did"`
	Operation Operation `json:"operation"`
	Cid       string    `json:"cid"`
	Nullified bool      `json:"nullified"`
	CreatedAt time.Time `json:"createdAt"`
}

type Operation struct {
	AlsoKnownAs []string `json:"alsoKnownAs"`
	Type        string   `json:"type"`
}

var tracer = otel.Tracer("plc-directory")

func NewDirectory(endpoint string, redisClient *redis.Client, store *store.Store, redisPrefix string, validate bool) (*Directory, error) {
	ctx := context.Background()
	rawLogger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %+v", err)
	}
	logger := rawLogger.Sugar().With("source", "plc_directory")

	cmd := redisClient.Get(ctx, redisPrefix+":last_cursor")
	if cmd.Err() != nil {
		logger.Info("no last cursor found, starting from beginning")
	}

	var lastCursor time.Time
	if cmd.Val() != "" {
		lastCursor, err = time.Parse(time.RFC3339Nano, cmd.Val())
		if err != nil {
			logger.Info("failed to parse last cursor, starting from beginning")
		}
	}

	return &Directory{
		Endpoint:       endpoint,
		Logger:         logger,
		PLCRateLimiter: rate.NewLimiter(rate.Limit(2), 1),
		PDSRateLimiter: rate.NewLimiter(rate.Limit(20), 1),
		CheckPeriod:    30 * time.Second,
		AfterCursor:    lastCursor,

		ValidationTTL: 12 * time.Hour,

		RedisClient: redisClient,
		RedisPrefix: redisPrefix,

		Store:    store,
		validate: validate,
	}, nil
}

func (d *Directory) Start() {
	ticker := time.NewTicker(d.CheckPeriod)
	ctx := context.Background()
	go func() {
		d.fetchDirectoryEntries(ctx)

		for range ticker.C {
			d.fetchDirectoryEntries(ctx)
		}
	}()

	if d.validate {
		go func() {
			d.ValidateHandles(ctx, 1200, 5*time.Second)
		}()
	}
}

func (d *Directory) fetchDirectoryEntries(ctx context.Context) {
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	d.Logger.Info("fetching directory entries...")

	for {
		d.Logger.Infof("querying for entries after %s", d.AfterCursor.Format(time.RFC3339Nano))
		req, err := http.NewRequestWithContext(ctx, "GET", d.Endpoint, nil)
		if err != nil {
			d.Logger.Errorf("failed to create request: %+v", err)
			break
		}
		q := req.URL.Query()
		if !d.AfterCursor.IsZero() {
			q.Add("after", d.AfterCursor.Format(time.RFC3339Nano))
		}
		req.URL.RawQuery = q.Encode()
		d.PLCRateLimiter.Wait(ctx)
		start := time.Now()
		resp, err := client.Do(req)
		plcDirectoryRequestHistogram.WithLabelValues(fmt.Sprintf("%d", resp.StatusCode)).Observe(time.Since(start).Seconds())
		if err != nil {
			d.Logger.Errorf("failed to fetch directory entries: %+v", err)
			resp.Body.Close()
			break
		}

		// Create a bufio scanner to read the response line by line
		scanner := bufio.NewScanner(resp.Body)

		var newEntries []DirectoryJSONLRow
		for scanner.Scan() {
			line := scanner.Text()
			var entry DirectoryJSONLRow

			// Try to unmarshal the line into a DirectoryJSONLRow
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				d.Logger.Errorf("failed to unmarshal directory entry: %+v", err)
				resp.Body.Close()
				return
			}

			newEntries = append(newEntries, entry)
		}

		// Check if the scan finished without errors
		if err := scanner.Err(); err != nil {
			d.Logger.Errorf("failed to read response body: %+v", err)
			resp.Body.Close()
			return
		}

		if len(newEntries) <= 1 {
			resp.Body.Close()
			break
		}

		resp.Body.Close()

		var tx *sql.Tx
		if d.Store != nil {
			tx, err = d.Store.DB.Begin()
			if err != nil {
				d.Logger.Errorf("failed to start transaction: %+v", err)
				return
			}
		}

		pipeline := d.RedisClient.Pipeline()
		for _, entry := range newEntries {
			if len(entry.Operation.AlsoKnownAs) > 0 {
				handle := strings.TrimPrefix(entry.Operation.AlsoKnownAs[0], "at://")

				// Set both forward and backward mappings in redis

				// Lookup an existing DID entry if it exists
				cmd := d.RedisClient.Get(ctx, d.RedisPrefix+":by_did:"+entry.Did)
				if cmd.Err() != nil {
					if cmd.Err() != redis.Nil {
						d.Logger.Errorf("failed to get redis key: %+v", cmd.Err())
					}
				}

				oldHandle := cmd.Val()
				if oldHandle != "" {
					pipeline.Del(ctx, d.RedisPrefix+":by_handle:"+oldHandle)
				}

				pipeline.Set(ctx, d.RedisPrefix+":by_did:"+entry.Did, handle, 0)
				pipeline.Set(ctx, d.RedisPrefix+":by_handle:"+handle, entry.Did, 0)

				// Set the DID entry in the database
				if d.Store != nil && tx != nil {
					err := d.Store.Queries.WithTx(tx).UpsertActor(ctx, store_queries.UpsertActorParams{
						Did:       entry.Did,
						Handle:    handle,
						CreatedAt: sql.NullTime{Time: entry.CreatedAt, Valid: true},
					})
					if err != nil {
						d.Logger.Errorf("failed to upsert actor: %+v", err)
					}
				}
			}
		}
		_, err = pipeline.Exec(ctx)
		if err != nil {
			d.Logger.Errorf("failed to set redis keys: %+v", err)
		}

		if d.Store != nil && tx != nil {
			err = tx.Commit()
			if err != nil {
				d.Logger.Errorf("failed to commit transaction: %+v", err)
			}
		}

		d.AfterCursor = newEntries[len(newEntries)-1].CreatedAt
		cmd := d.RedisClient.Set(ctx, d.RedisPrefix+":last_cursor", d.AfterCursor.Format(time.RFC3339Nano), 0)
		if cmd.Err() != nil {
			d.Logger.Errorf("failed to set last cursor: %+v", cmd.Err())
		}
		d.Logger.Infof("fetched %d new directory entries", len(newEntries))
	}

	d.Logger.Info("finished fetching directory entries")
}

var plcDirectoryValidationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "plc_directory_validation_duration_seconds",
	Help: "Histogram of the time (in seconds) each validation of the PLC directory takes",
}, []string{"is_valid"})

func (d *Directory) ValidateHandles(ctx context.Context, pageSize int, timeBetweenLoops time.Duration) {
	logger := d.Logger.With("source", "plc_directory_validation")
	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled, stopping validation loop")
			return
		default:
			if !d.ValidateHandlePage(ctx, pageSize) {
				time.Sleep(timeBetweenLoops)
			}
		}
	}
}

func (d *Directory) ValidateHandlePage(ctx context.Context, pageSize int) bool {
	ctx, span := tracer.Start(ctx, "ValidateHandles")
	defer span.End()

	logger := d.Logger.With("source", "plc_directory_validation_page")

	logger.Info("validating handles entries...")

	start := time.Now()

	actors, err := d.Store.Queries.GetActorsForValidation(ctx, store_queries.GetActorsForValidationParams{
		LastValidated: sql.NullTime{Time: time.Now().Add(-d.ValidationTTL), Valid: true},
		Limit:         int32(pageSize),
	})
	if err != nil {
		logger.Errorf("failed to get actors for validation: %+v", err)
		return false
	}

	queryDone := time.Now()

	validDids := []string{}
	validLk := &sync.Mutex{}
	invalidDids := []string{}
	invalidLk := &sync.Mutex{}

	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	// Validate in 20 goroutines
	sem := semaphore.NewWeighted(20)
	wg := &sync.WaitGroup{}
	for _, actor := range actors {
		wg.Add(1)
		go func(actor store_queries.Actor) {
			defer wg.Done()
			defer sem.Release(1)
			validStart := time.Now()
			valid, errs := d.ValidateHandle(ctx, client, actor.Did, actor.Handle)
			plcDirectoryValidationHistogram.WithLabelValues(fmt.Sprintf("%t", valid)).Observe(time.Since(validStart).Seconds())
			if valid {
				validLk.Lock()
				validDids = append(validDids, actor.Did)
				validLk.Unlock()
			} else {
				invalidLk.Lock()
				invalidDids = append(invalidDids, actor.Did)
				invalidLk.Unlock()
				logger.Errorw("failed to validate handle",
					"did", actor.Did,
					"handle", actor.Handle,
					"errors", errs,
				)

			}
		}(actor)
		sem.Acquire(ctx, 1)
	}

	wg.Wait()

	validationDone := time.Now()

	// Update the actors in the database
	if len(validDids) > 0 {
		err = d.Store.Queries.UpdateActorsValidation(ctx, store_queries.UpdateActorsValidationParams{
			Dids:          validDids,
			HandleValid:   true,
			LastValidated: sql.NullTime{Time: time.Now(), Valid: true},
		})
		if err != nil {
			logger.Errorf("failed to update valid actors: %+v", err)
		}
	}
	if len(invalidDids) > 0 {
		err = d.Store.Queries.UpdateActorsValidation(ctx, store_queries.UpdateActorsValidationParams{
			Dids:          invalidDids,
			HandleValid:   false,
			LastValidated: sql.NullTime{Time: time.Now(), Valid: true},
		})
		if err != nil {
			logger.Errorf("failed to update invalid actors: %+v", err)
		}
	}

	updateDone := time.Now()

	logger.Infow("finished validating directory entries",
		"valid", len(validDids),
		"invalid", len(invalidDids),
		"query_time", queryDone.Sub(start).Seconds(),
		"validation_time", validationDone.Sub(queryDone).Seconds(),
		"update_time", updateDone.Sub(validationDone).Seconds(),
		"total_time", updateDone.Sub(start).Seconds(),
	)

	if len(actors) >= pageSize {
		return true
	}

	return false
}

func (d *Directory) ValidateHandle(ctx context.Context, client *http.Client, did string, handle string) (bool, []error) {
	ctx, span := tracer.Start(ctx, "ValidateHandle")
	defer span.End()

	var errs []error
	expectedTxtValue := fmt.Sprintf("did=%s", did)

	// Start by looking up TXT records for the Handle
	txtrecords, err := net.DefaultResolver.LookupTXT(ctx, fmt.Sprintf("_atproto.%s", handle))
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to lookup TXT records for handle: %+v", err))
	} else {
		for _, txt := range txtrecords {
			if txt == expectedTxtValue {
				span.SetAttributes(attribute.Bool("txt_validated", true))
				return true, nil
			}
		}
	}

	// If no TXT records were found, check /.well-known/atproto-did
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://%s/.well-known/atproto-did", handle), nil)
	if err != nil {
		return false, append(errs, fmt.Errorf("failed to create request for HTTPS validation: %+v", err))
	}

	// If the handle ends in `.bsky.social`, use the PDS rate limiter
	if strings.HasSuffix(handle, ".bsky.social") {
		d.PDSRateLimiter.Wait(ctx)
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, append(errs, fmt.Errorf("failed to fetch /.well-known/atproto-did: %+v", err))
	}

	if resp.StatusCode != http.StatusOK {
		span.SetAttributes(attribute.Bool("both_invalid", true))
		span.SetAttributes(attribute.Int("https_status_code", resp.StatusCode))
		return false, append(errs, fmt.Errorf("failed to fetch /.well-known/atproto-did: %s", resp.Status))
	}

	// There should only be one line in the response with the contenr of the DID
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == did {
			span.SetAttributes(attribute.Bool("https_validated", true))
			return true, nil
		}
	}

	span.SetAttributes(attribute.Bool("both_invalid", true))
	return false, append(errs, fmt.Errorf("failed to find DID in /.well-known/atproto-did"))
}

func (d *Directory) GetEntryForDID(ctx context.Context, did string) (DirectoryEntry, error) {
	cmd := d.RedisClient.Get(ctx, d.RedisPrefix+":by_did:"+did)
	if cmd.Err() != nil {
		return DirectoryEntry{}, cmd.Err()
	}

	return DirectoryEntry{
		Did: did,
		AKA: cmd.Val(),
	}, nil
}

func (d *Directory) GetBatchEntriesForDID(ctx context.Context, dids []string) ([]DirectoryEntry, error) {
	cmd := d.RedisClient.MGet(ctx, func() []string {
		var keys []string
		for _, did := range dids {
			keys = append(keys, d.RedisPrefix+":by_did:"+did)
		}
		return keys
	}()...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	var entries []DirectoryEntry
	for i, val := range cmd.Val() {
		if val != nil {
			entries = append(entries, DirectoryEntry{
				Did: dids[i],
				AKA: val.(string),
			})
		}
	}

	return entries, nil
}

func (d *Directory) GetEntryForHandle(ctx context.Context, handle string) (DirectoryEntry, error) {
	cmd := d.RedisClient.Get(ctx, d.RedisPrefix+":by_handle:"+handle)
	if cmd.Err() != nil {
		return DirectoryEntry{}, cmd.Err()
	}

	return DirectoryEntry{
		Did: cmd.Val(),
		AKA: handle,
	}, nil
}

func (d *Directory) GetBatchEntriesForHandle(ctx context.Context, handles []string) ([]DirectoryEntry, error) {
	cmd := d.RedisClient.MGet(ctx, func() []string {
		var keys []string
		for _, handle := range handles {
			keys = append(keys, d.RedisPrefix+":by_handle:"+handle)
		}
		return keys
	}()...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	var entries []DirectoryEntry
	for i, val := range cmd.Val() {
		if val != nil {
			entries = append(entries, DirectoryEntry{
				Did: val.(string),
				AKA: handles[i],
			})
		}
	}

	return entries, nil
}
