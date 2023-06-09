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

// TestRedisWithPipelines is a test for the Redis storage backend
// We generate a bunch of expiring keys and set them in Redis, then we read them back
func TestRedisWithPipelines(t *testing.T) {
	ctx := context.Background()

	// Number of keys to generate
	n := 500000
	maxPipelineSize := 10000

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

	// Generate a 1KB value
	value := ""
	for k := 0; k < 100; k++ {
		value += "0123456789"
	}

	// Add keys in batches of 10,000
	for i := 0; i < n; i += maxPipelineSize {
		pipeline := conn.Pipeline()

		for j := i; j < i+maxPipelineSize; j++ {
			pipeline.Set(ctx, keys[j], value, time.Minute*2)
		}

		// Execute the pipeline
		_, err := pipeline.Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read back the keys in 50 goroutines (25m reads total)
	numRoutines := 50
	errChan := make(chan []error, numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func() {
			errs := []error{}

			// Read in batches of 10,000
			for i := 0; i < n; i += maxPipelineSize {
				pipeline := conn.Pipeline()

				for j := i; j < i+maxPipelineSize; j++ {
					pipeline.Get(ctx, keys[j])
				}

				// Execute the pipeline
				cmder, err := pipeline.Exec(ctx)
				if err != nil {
					errs = append(errs, err)
				}

				// Check that we got the right values
				for j := i; j < i+10000; j++ {
					cmd := cmder[j-i]
					if cmd.Err() != nil {
						errs = append(errs, cmd.Err())
					}
					if cmd.(*redis.StringCmd).Val() != value {
						errs = append(errs, fmt.Errorf("wrong value"))
					}
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

// TestRedisWithoutPipelines is a test for the Redis storage backend
// We generate a bunch of expiring keys and set them in Redis, then we read them back
// This test does not use pipelines
func TestRedisWithoutPipelines(t *testing.T) {
	ctx := context.Background()

	// Number of keys to generate
	n := 100000

	// Initialize Redis
	conn := redis.NewClient(&redis.Options{
		Addr: "localhost:6385",
		DB:   0,
	})

	// Generate a bunch of keys
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "key" + fmt.Sprint(i)
	}

	// Generate a 1KB value
	value := ""
	for k := 0; k < 100; k++ {
		value += "0123456789"
	}

	// Add keys
	for i := 0; i < n; i++ {
		_, err := conn.Set(ctx, keys[i], value, time.Minute*2).Result()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read back the keys in 50 goroutines (5m reads total)
	numRoutines := 50
	errChan := make(chan []error, numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func() {
			errs := []error{}

			for i := 0; i < n; i++ {
				val, err := conn.Get(ctx, keys[i]).Result()
				if err != nil {
					errs = append(errs, err)
				}
				if val != value {
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
