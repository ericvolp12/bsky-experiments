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

// Initialize Prometheus Metrics for total number of posts processed
var postsProcessedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_posts_processed_total",
	Help: "The total number of posts processed",
})

// Initialize Prometheus metrics for duration of processing posts
var postProcessingDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsky_post_processing_duration_seconds",
	Help:    "The duration of processing posts",
	Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
})
