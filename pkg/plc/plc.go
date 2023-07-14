package plc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	Entries     []DirectoryEntry
	Lock        sync.RWMutex
	AfterCursor time.Time
}

type DirectoryEntry struct {
	Did       string    `json:"did"`
	Operation Operation `json:"operation"`
	Cid       string    `json:"cid"`
	Nullified bool      `json:"nullified"`
	CreatedAt time.Time `json:"createdAt"`
}

type Operation struct {
	Sig         string `json:"sig"`
	Prev        string `json:"prev"`
	Type        string `json:"type"`
	Handle      string `json:"handle"`
	Service     string `json:"service"`
	SigningKey  string `json:"signingKey"`
	RecoveryKey string `json:"recoveryKey"`
}

func NewDirectory(endpoint string) *Directory {
	return &Directory{
		Endpoint:    endpoint,
		RateLimiter: rate.NewLimiter(rate.Limit(2), 1),
		CheckPeriod: 30 * time.Second,
		Entries:     make([]DirectoryEntry, 0),
		Lock:        sync.RWMutex{},
		AfterCursor: time.Now(),
	}
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

	for {
		d.Lock.Lock()
		req, _ := http.NewRequestWithContext(ctx, "GET", d.Endpoint, nil)
		q := req.URL.Query()
		q.Add("after", d.AfterCursor.Format(time.RFC3339))
		req.URL.RawQuery = q.Encode()
		d.RateLimiter.Wait(ctx)
		start := time.Now()
		resp, err := client.Do(req)
		plcDirectoryRequestHistogram.WithLabelValues(fmt.Sprintf("%d", resp.StatusCode)).Observe(time.Since(start).Seconds())
		if err != nil {
			d.Lock.Unlock()
			resp.Body.Close()
			continue
		}

		body, _ := ioutil.ReadAll(resp.Body)

		var newEntries []DirectoryEntry
		json.Unmarshal(body, &newEntries)

		if len(newEntries) == 0 {
			d.Lock.Unlock()
			resp.Body.Close()
			break
		}

		d.Entries = append(d.Entries, newEntries...)
		d.AfterCursor = d.Entries[len(d.Entries)-1].CreatedAt
		d.Lock.Unlock()
		resp.Body.Close()
	}
}

func (d *Directory) GetDirectoryEntries() []DirectoryEntry {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	return d.Entries
}
