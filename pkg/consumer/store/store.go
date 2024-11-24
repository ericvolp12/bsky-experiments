package store

import (
	"database/sql"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store/store_queries"
	_ "github.com/lib/pq" // postgres driver
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
)

var tracer = otel.Tracer("store")

// Store is a wrapper around the database connection.
type Store struct {
	DB      *sql.DB
	Queries *store_queries.Queries
}

// NewStore creates a new store.
func NewStore(connectionString string) (*Store, error) {
	var db *sql.DB
	var err error

	for i := 0; i < 5; i++ {
		db, err = otelsql.Open(
			"postgres",
			connectionString,
			otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		)
		if err != nil {
			return nil, err
		}

		db.SetMaxOpenConns(200)
		db.SetMaxIdleConns(100)
		db.SetConnMaxLifetime(1 * time.Hour)

		err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
			semconv.DBSystemPostgreSQL,
		))
		if err != nil {
			return nil, err
		}

		err = db.Ping()
		if err == nil {
			break
		}

		db.Close() // Close the connection if it failed.
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return nil, err
	}

	queries := store_queries.New(db)

	registry := &Store{
		DB:      db,
		Queries: queries,
	}

	return registry, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.DB.Close()
}
