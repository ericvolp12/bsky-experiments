package sharddb

import (
	"context"
	"log/slog"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
)

type ShardDB struct {
	scyllaNodes []string
	logger      *slog.Logger
	session     *gocql.Session
}

var tracer = otel.Tracer("sharddb")

var QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "sharddb_query_duration_seconds",
	Help: "Duration of sharddb queries",
}, []string{"query_name", "status"})

func NewShardDB(ctx context.Context, scyllaNodes []string, logger *slog.Logger) (*ShardDB, error) {
	cluster := gocql.NewCluster(scyllaNodes...)
	cluster.Keyspace = "bsky"
	cluster.Consistency = gocql.One // One-node cluster
	cluster.Port = 9042
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &ShardDB{
		scyllaNodes: scyllaNodes,
		logger:      logger,
		session:     session,
	}, nil
}
