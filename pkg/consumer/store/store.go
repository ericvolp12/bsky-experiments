package store

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
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

	session, err := cluster.CreateSession()
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
