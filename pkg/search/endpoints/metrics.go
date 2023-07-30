package endpoints

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

var totalUsers = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_total_users",
	Help: "The total number of users",
})

var totalAuthors = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_total_authors",
	Help: "The total number of authors",
})

var meanPostCount = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_mean_post_count",
	Help: "The mean number of posts per author",
})

var totalPostCount = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsky_total_post_count",
	Help: "The total number of posts",
})
