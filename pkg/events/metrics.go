package events

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Initialize Prometheus Metrics for cache hits and misses
var cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_hits_total",
	Help: "The total number of cache hits",
}, []string{"cache_type"})

var cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsky_cache_misses_total",
	Help: "The total number of cache misses",
}, []string{"cache_type"})

var cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsky_cache_size_bytes",
	Help: "The size of the cache in bytes",
}, []string{"cache_type"})

// Initialize Prometheus Metrics for mentions and replies
var mentionCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_mentions_total",
	Help: "The total number of mentions",
})

var replyCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_replies_total",
	Help: "The total number of replies",
})

var quoteCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_quotes_total",
	Help: "The total number of quotes",
})

// Initialize Prometheus Metrics for total number of posts processed
var postsProcessedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_posts_processed_total",
	Help: "The total number of posts processed",
})

var rebaseEventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_rebase_events_processed_total",
	Help: "The total number of rebase events processed",
})

var deleteRecordsProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_delete_records_processed_total",
	Help: "The total number of delete records processed",
})

// Initialize Prometheus metrics for duration of processing posts
var postProcessingDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsky_post_processing_duration_seconds",
	Help:    "The duration of processing posts",
	Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
})

var apiCallDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsky_api_call_duration_seconds",
	Help:    "The duration of API calls",
	Buckets: prometheus.DefBuckets,
}, []string{"api_call"})

var likesProcessedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_likes_processed_total",
	Help: "The total number of likes processed",
})

var indexingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsky_indexing_latency_seconds",
	Help:    "The duration of adding a post to the index",
	Buckets: prometheus.ExponentialBuckets(0.001, 30, 15),
})

var lastSeq = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_last_seq",
	Help: "The last sequence number processed",
})

var lastSeqProcessedAt = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_last_seq_processed_at",
	Help: "The timestamp of the last sequence number processed",
})

var lastSeqCreatedAt = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_last_seq_created_at",
	Help: "The timestamp of the last sequence number created",
})
