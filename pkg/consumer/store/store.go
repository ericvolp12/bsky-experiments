// Package store represents a store of ScyllaDB tables
package store

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gocql/gocql/otelgocql"
	"go.opentelemetry.io/otel"
)

// Store represents a store
type Store struct {
	ScyllaSession *gocql.Session
	Keyspace      string
}

// NewStore creates a new store
func NewStore(ctx context.Context, connectionString string, keyspace string) (*Store, error) {
	cluster := gocql.NewCluster(connectionString)
	cluster.Keyspace = keyspace

	session, err := otelgocql.NewSessionWithTracing(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to create scylla session: %w", err)
	}

	return &Store{
		ScyllaSession: session,
		Keyspace:      keyspace,
	}, nil
}

// TeardownStore tears down a store
func TeardownStore(ctx context.Context, store *Store) {
	store.ScyllaSession.Close()
}

var tracer = otel.Tracer("store")
