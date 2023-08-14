package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Initialize Prometheus Metrics for total number of posts processed
var EventsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_events_processed_total",
	Help: "The total number of firehose events processed by Jazbot",
}, []string{"event_type", "socket_url"})

var RecordsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_records_processed_total",
	Help: "The total number of records processed by Jazbot",
}, []string{"record_type", "socket_url"})

// Initialize Prometheus metrics for duration of processing events
var EventProcessingDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "jazbot_event_processing_duration_seconds",
	Help:    "The amount of time it takes to process a firehose event",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
}, []string{"socket_url"})

var LastSeqGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_seq",
	Help: "The sequence number of the last event processed",
}, []string{"socket_url"})

var LastEvtCreatedRecordCreatedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_evt_created_record_created_gap",
	Help: "The gap between the last event's event timestamp and it's record timestamp",
}, []string{"socket_url"})

var LastEvtCreatedEvtProcessedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_evt_created_evt_processed_gap",
	Help: "The gap between the last event's event timestamp and when it was processed by jazbot",
}, []string{"socket_url"})

var LastRecordCreatedEvtProcessedGapGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jazbot_last_record_created_evt_processed_gap",
	Help: "The gap between the last record's record timestamp and when it was processed by jazbot",
}, []string{"socket_url"})

var CommandsReceivedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_commands_received_total",
	Help: "The total number of commands received by Jazbot",
}, []string{})

var ValidCommandsReceivedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_valid_commands_received_total",
	Help: "The total number of valid commands received by Jazbot",
}, []string{"command"})

var PostsSentCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_posts_sent_total",
	Help: "The total number of posts sent by Jazbot",
}, []string{})

var PostsFailedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_posts_failed_total",
	Help: "The total number of posts failed to send by Jazbot",
}, []string{"failure_type"})

var FailedCommandsReceivedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jazbot_failed_commands_received_total",
	Help: "The total number of failed commands received by Jazbot",
}, []string{"failure_type"})
