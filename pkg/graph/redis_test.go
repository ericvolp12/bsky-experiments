package graph_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkRedis is a benchmark for the Redis storage backend
// We generate a bunch of expiring keys and set them in Redis, then we read them back
func BenchmarkRedis(b *testing.B) {
	ctx := context.Background()

	n := b.N

	// Initialize Redis
	conn := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// Generate a bunch of keys
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + fmt.Sprint(i)
	}

	b.ResetTimer()

	pipeline := conn.Pipeline()

	// Set them in Redis
	for i := 0; i < n; i++ {
		pipeline.Set(ctx, keys[i], "value", time.Minute*2)
	}

	// Execute the pipeline
	_, err := pipeline.Exec(ctx)
	if err != nil {
		b.Fatal(err)
	}

	// Read back the keys 100 times in a row

	for i := 0; i < 100; i++ {
		pipeline = conn.Pipeline()

		for i := 0; i < n; i++ {
			pipeline.Get(ctx, keys[i])
		}

		// Execute the pipeline
		cmder, err := pipeline.Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}

		// Check that we got the right values
		for i := 0; i < n; i++ {
			cmd := cmder[i]
			if cmd.Err() != nil {
				b.Fatal(cmd.Err())
			}
			if cmd.(*redis.StringCmd).Val() != "value" {
				b.Fatal("wrong value")
			}
		}
	}
}

// TestRedis is a test for the Redis storage backend
// We generate a bunch of expiring keys and set them in Redis, then we read them back
func TestRedis(t *testing.T) {
	ctx := context.Background()

	// Number of keys to generate
	n := 50000

	// Initialize Redis
	conn := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// Generate a bunch of keys
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + fmt.Sprint(i)
	}

	pipeline := conn.Pipeline()

	// Set them in Redis
	for i := 0; i < n; i++ {
		pipeline.Set(ctx, keys[i], "value", time.Minute*2)
	}

	// Execute the pipeline
	_, err := pipeline.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Read back the keys in 50 goroutines (2.5m reads total)
	numRoutines := 100
	errChan := make(chan []error, numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func() {
			pipeline := conn.Pipeline()

			errs := []error{}

			for i := 0; i < n; i++ {
				pipeline.Get(ctx, keys[i])
			}

			// Execute the pipeline
			cmder, err := pipeline.Exec(ctx)
			if err != nil {
				errs = append(errs, err)
			}

			// Check that we got the right values
			for i := 0; i < n; i++ {
				cmd := cmder[i]
				if cmd.Err() != nil {
					errs = append(errs, cmd.Err())
				}
				if cmd.(*redis.StringCmd).Val() != "value" {
					errs = append(errs, fmt.Errorf("wrong value"))
				}
			}

			errChan <- errs
		}()
	}

	// Wait for all goroutines to finish
	errs := []error{}
	for i := 0; i < numRoutines; i++ {
		errs = append(errs, <-errChan...)
	}

	// Check that there were no errors
	if len(errs) > 0 {
		t.Fatal(errs)
	}
}
