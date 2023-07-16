package plc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var plcDirectoryRequestHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "plc_directory_request_duration_seconds",
	Help: "Histogram of the time (in seconds) each request to the PLC directory takes",
}, []string{"status_code"})

type Directory struct {
	Endpoint    string
	RateLimiter *rate.Limiter
	CheckPeriod time.Duration
	AfterCursor time.Time
	Logger      *zap.SugaredLogger

	RedisClient *redis.Client
	RedisPrefix string
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

func NewDirectory(endpoint string, redisClient *redis.Client, redisPrefix string) (*Directory, error) {
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
		Endpoint:    endpoint,
		Logger:      logger,
		RateLimiter: rate.NewLimiter(rate.Limit(2), 1),
		CheckPeriod: 30 * time.Second,
		AfterCursor: lastCursor,

		RedisClient: redisClient,
		RedisPrefix: redisPrefix,
	}, nil
}

func (d *Directory) Start() {
	ticker := time.NewTicker(d.CheckPeriod)
	ctx := context.Background()
	go func() {
		for range ticker.C {
			d.fetchDirectoryEntries(ctx)
		}
	}()

	d.fetchDirectoryEntries(ctx)
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
		d.RateLimiter.Wait(ctx)
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
			}
		}
		_, err = pipeline.Exec(ctx)
		if err != nil {
			d.Logger.Errorf("failed to set redis keys: %+v", err)
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
