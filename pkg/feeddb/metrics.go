package feeddb

import "github.com/prometheus/client_golang/prometheus"

var postsAdded = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "feeddb_posts_added",
	Help: "Number of posts added to the feeddb",
})

var postsDeleted = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "feeddb_posts_deleted",
	Help: "Number of posts deleted from the feeddb",
})

var queriesPerformed = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "feeddb_queries_performed",
	Help: "Number of queries performed against the feeddb",
})

var queryLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "feeddb_query_latency",
	Help:    "Latency of queries performed against the feeddb",
	Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
}, []string{"query"})
