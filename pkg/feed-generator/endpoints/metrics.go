package endpoints

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var feedRequestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "feed_request_count",
	Help: "The total number of feed requests",
}, []string{"feed_name"})

var feedUserCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "feed_user_count",
	Help: "The total number of feed users",
}, []string{"feed_name"})

var uniqueFeedUserCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "unique_feed_user_count",
	Help: "The total number of unique feed users",
})

var feedRequestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "feed_request_latency",
	Help:    "The latency of feed requests",
	Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
}, []string{"feed_name"})
