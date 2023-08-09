package jazbot

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Initialize Prometheus Metrics for total number of posts processed
var eventsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_events_processed_total",
	Help: "The total number of firehose events processed by Jazbot",
}, []string{"event_type", "socket_url"})

var recordsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_records_processed_total",
	Help: "The total number of records processed by Jazbot",
}, []string{"record_type", "socket_url"})

// Initialize Prometheus metrics for duration of processing events
var eventProcessingDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "jazbot_event_processing_duration_seconds",
	Help:    "The amount of time it takes to process a firehose event",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
}, []string{"socket_url"})

var lastSeqGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_seq",
	Help: "The sequence number of the last event processed",
}, []string{"socket_url"})

var lastEvtCreatedRecordCreatedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_evt_created_record_created_gap",
	Help: "The gap between the last event's event timestamp and it's record timestamp",
}, []string{"socket_url"})

var lastEvtCreatedEvtProcessedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_evt_created_evt_processed_gap",
	Help: "The gap between the last event's event timestamp and when it was processed by jazbot",
}, []string{"socket_url"})

var lastRecordCreatedEvtProcessedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_record_created_evt_processed_gap",
	Help: "The gap between the last record's record timestamp and when it was processed by jazbot",
}, []string{"socket_url"})
