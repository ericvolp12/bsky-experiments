package events

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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

var likesProcessedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsky_likes_processed_total",
	Help: "The total number of likes processed",
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
