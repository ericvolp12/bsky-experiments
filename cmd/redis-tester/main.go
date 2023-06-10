package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/redis/go-redis/v9"
)

type TestParam struct {
	Inserts      int
	ValueSize    int
	ReadAmp      int
	PipelineSize int
	TestName     string
	RedisBackend string
	Repetitions  int
}

var backendImageMap = map[string]string{
	"redis-stack": "redis/redis-stack-server:latest",
	"dragonfly":   "docker.dragonflydb.io/dragonflydb/dragonfly:latest",
}

func main() {
	params := []TestParam{
		{Inserts: 100_000, ValueSize: 5, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},

		{Inserts: 100_000, ValueSize: 5, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},

		{Inserts: 500_000, ValueSize: 5, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},

		{Inserts: 500_000, ValueSize: 5, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
	}

	runtimes := make([][]time.Duration, len(params))

	for idx, param := range params {
		runtimes = append(runtimes, runTest(param, idx))
	}

	// Average the runtimes for each test and print them out.
	for idx, param := range params {
		var total time.Duration
		for _, runtime := range runtimes[idx] {
			total += runtime
		}
		avg := total / time.Duration(param.Repetitions)
		fmt.Printf("%s\t%s\t%d\t%d\t%d\t%d\t%.3f\t%d\n", param.TestName, param.RedisBackend, param.Inserts, param.ValueSize, param.ReadAmp, param.PipelineSize, avg.Seconds(), param.Repetitions)
	}
}

func runTest(param TestParam, idx int) []time.Duration {
	// Create a new Redis container for each test.
	ctx := context.Background()
	log.SetPrefix(fmt.Sprintf("[%s:%d] ", param.TestName, idx))

	durations := make([]time.Duration, param.Repetitions)

	for i := 0; i < param.Repetitions; i++ {
		log.Printf("Starting test %d/%d", i+1, param.Repetitions)

		cli, err := client.NewClientWithOpts(client.FromEnv)
		if err != nil {
			log.Fatal(err)
		}

		image := backendImageMap[param.RedisBackend]
		containerName := namesgenerator.GetRandomName(0)

		log.Printf("Starting container %s with image %s", containerName, image)

		resp, err := cli.ContainerCreate(ctx, &container.Config{
			Image: image,
		}, &container.HostConfig{
			NetworkMode: "host",
		}, nil, nil, containerName)

		if err != nil {
			log.Fatal(err)
		}

		if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
			log.Fatal(err)
		}

		log.Printf("Checking container %s", containerName)

		// Wait for Redis to start up.
		time.Sleep(5 * time.Second)

		// Test the connection to Redis.
		conn := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   0,
		})

		// Try to ping the Redis server 5 times before giving up.
		redisReady := false
		for i := 0; i < 5; i++ {
			_, err := conn.Ping(ctx).Result()
			if err == nil {
				redisReady = true
				break
			}
			time.Sleep(1 * time.Second)
		}

		if !redisReady {
			log.Fatal("Failed to ping Redis server after 5 attempts")
		}

		log.Printf("Running test %s", param.TestName)

		start := time.Now()

		// Run the test
		if param.TestName == "No-Pipeline" {
			err = TestRedisWithoutPipelines(param)
		} else {
			err = TestRedisWithPipelines(param)
		}

		durations = append(durations, time.Since(start))

		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Test %s finished", param.TestName)

		// Cleanup: Stop and remove the Redis container.
		if err := cli.ContainerStop(ctx, resp.ID, container.StopOptions{}); err != nil {
			log.Fatal(err)
		}
		if err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{}); err != nil {
			log.Fatal(err)
		}

		log.Printf("Container %s stopped and removed", containerName)
	}

	return durations
}

func TestRedisWithPipelines(param TestParam) error {
	ctx := context.Background()

	n := param.Inserts
	maxPipelineSize := param.PipelineSize

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

	value := ""
	for k := 0; k < param.ValueSize; k++ {
		value += fmt.Sprint(rand.Intn(10))
	}

	// Add keys in batches
	for i := 0; i < n; i += maxPipelineSize {
		pipeline := conn.Pipeline()

		for j := i; j < i+maxPipelineSize; j++ {
			pipeline.Set(ctx, keys[j], value, time.Minute*2)
		}

		// Execute the pipeline
		_, err := pipeline.Exec(ctx)
		if err != nil {
			return fmt.Errorf("pipeline.Exec: %v", err)
		}
	}

	// Read back the keys in goroutines
	numRoutines := param.ReadAmp
	errChan := make(chan []error, numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func() {
			errs := []error{}

			// Read in batches
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
				for j := i; j < i+maxPipelineSize; j++ {
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
		return fmt.Errorf("errs: %v", errs)
	}

	return nil
}

func TestRedisWithoutPipelines(param TestParam) error {
	ctx := context.Background()

	n := param.Inserts

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

	value := ""
	for k := 0; k < param.ValueSize; k++ {
		value += fmt.Sprint(rand.Intn(10))
	}

	// Add keys
	for i := 0; i < n; i++ {
		_, err := conn.Set(ctx, keys[i], value, time.Minute*2).Result()
		if err != nil {
			return fmt.Errorf("conn.Set: %v", err)
		}
	}

	// Read back the keys in goroutines
	numRoutines := param.ReadAmp
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
		return fmt.Errorf("errs: %v", errs)
	}

	return nil
}
