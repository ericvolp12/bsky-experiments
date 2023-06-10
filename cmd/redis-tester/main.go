package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/redis/go-redis/v9"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type TestParam struct {
	TestID       int
	Inserts      int
	ValueSize    int
	ReadAmp      int
	PipelineSize int
	TestName     string
	RedisBackend string
	Repetitions  int
}

type BackendResult struct {
	BackendName         string
	AverageWriteRuntime time.Duration
	AverageReadRuntime  time.Duration
}

type TestResultRow struct {
	TestName       string
	Inserts        int
	ValueSize      int
	Reads          int
	PipelineSize   int
	Repetitions    int
	BackendResults map[string]BackendResult
}

var backendImageMap = map[string]string{
	"redis-stack": "redis/redis-stack-server:latest",
	"dragonfly":   "docker.dragonflydb.io/dragonflydb/dragonfly:latest",
}

var backends = []string{"redis-stack", "dragonfly"}

func main() {
	params := []TestParam{
		{TestID: 0, Inserts: 100_000, ValueSize: 5, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{TestID: 1, Inserts: 100_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{TestID: 2, Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "redis-stack", Repetitions: 3},

		{TestID: 0, Inserts: 100_000, ValueSize: 5, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{TestID: 1, Inserts: 100_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{TestID: 2, Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: -1, TestName: "No-Pipeline", RedisBackend: "dragonfly", Repetitions: 3},

		{TestID: 3, Inserts: 500_000, ValueSize: 5, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{TestID: 4, Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{TestID: 5, Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},
		{TestID: 6, Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "redis-stack", Repetitions: 3},

		{TestID: 3, Inserts: 500_000, ValueSize: 5, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{TestID: 4, Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 10_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{TestID: 5, Inserts: 500_000, ValueSize: 1_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
		{TestID: 6, Inserts: 100_000, ValueSize: 10_000, ReadAmp: 50, PipelineSize: 1_000, TestName: "Pipeline", RedisBackend: "dragonfly", Repetitions: 3},
	}

	writeRuntimes := [][]time.Duration{}
	readRuntimes := [][]time.Duration{}

	for idx, param := range params {
		writeRuns, readRuns := runTest(param, idx)
		writeRuntimes = append(writeRuntimes, writeRuns)
		readRuntimes = append(readRuntimes, readRuns)
	}

	// Create a file to write the results to.
	f, err := os.Create("results.txt")
	if err != nil {
		log.Printf("Error creating results file: %v", err)
	}

	if f != nil {
		defer f.Close()
		fmt.Fprintf(f, "|Test Name|Inserts|Value Size|Reads|Pipeline Size|Repetitions|")
		// Add a column for each Backend read and write runtime.
		for _, backend := range backends {
			fmt.Fprintf(f, "%s (write)|", backend)
			fmt.Fprintf(f, "%s (read)|", backend)
		}
		fmt.Fprintf(f, "\n")

		fmt.Fprintf(f, "|---|---|---|---|---|---|")
		for range backends {
			fmt.Fprintf(f, "---|")
			fmt.Fprintf(f, "---|")
		}
		fmt.Fprintf(f, "\n")
	}

	testResults := map[int]TestResultRow{}
	for _, param := range params {
		testResults[param.TestID] = TestResultRow{
			TestName:       param.TestName,
			Inserts:        param.Inserts,
			ValueSize:      param.ValueSize,
			Reads:          param.ReadAmp * param.Inserts,
			PipelineSize:   param.PipelineSize,
			Repetitions:    param.Repetitions,
			BackendResults: map[string]BackendResult{},
		}
	}

	for idx, param := range params {
		avgWriteRuntime := time.Duration(0)
		avgReadRuntime := time.Duration(0)
		for j, runtime := range writeRuntimes[idx] {
			avgWriteRuntime += runtime
			avgReadRuntime += readRuntimes[idx][j]
		}

		avgWriteRuntime /= time.Duration(len(writeRuntimes[idx]))
		avgReadRuntime /= time.Duration(len(readRuntimes[idx]))

		testResults[param.TestID].BackendResults[param.RedisBackend] = BackendResult{
			AverageWriteRuntime: avgWriteRuntime,
			AverageReadRuntime:  avgReadRuntime,
		}
	}

	for _, result := range testResults {
		fmt.Fprintf(f, "|%s|%d|%d|%d|%d|%d|", result.TestName, result.Inserts, result.ValueSize, result.Reads, result.PipelineSize, result.Repetitions)
		for _, backend := range backends {
			if backendResult, ok := result.BackendResults[backend]; ok {
				fmt.Fprintf(f, "%s|", backendResult.AverageWriteRuntime)
				fmt.Fprintf(f, "%s|", backendResult.AverageReadRuntime)
			} else {
				fmt.Fprintf(f, "N/A|")
			}
		}
		fmt.Fprintf(f, "\n")
	}
}

func runTest(param TestParam, idx int) ([]time.Duration, []time.Duration) {
	// Create a new Redis container for each test.
	ctx := context.Background()
	log.SetPrefix(fmt.Sprintf("[%s:%d] ", param.TestName, idx))

	writeRuntimes := []time.Duration{}
	readRuntimes := []time.Duration{}

	p := message.NewPrinter(language.English)

	testMsg := p.Sprintf("Starting test %s\tBackend: %s\tInserts: %d\tValue Size: %d bytes\tRead Amplification: %d\tPipeline Size: %d", param.TestName, param.RedisBackend, param.Inserts, param.ValueSize, param.ReadAmp, param.PipelineSize)

	log.Printf(testMsg)

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

		log.Printf("Running test ...")

		testStart := time.Now()

		writeRuntime, readRuntime := time.Duration(0), time.Duration(0)

		// Run the test
		if param.TestName == "No-Pipeline" {
			writeRuntime, readRuntime, err = TestRedisWithoutPipelines(param)
		} else {
			writeRuntime, readRuntime, err = TestRedisWithPipelines(param)
		}

		testRuntime := time.Since(testStart)

		if err != nil {
			log.Fatal(err)
		}

		writeRuntimes = append(writeRuntimes, writeRuntime)
		readRuntimes = append(readRuntimes, readRuntime)

		log.Printf("Test %s finished in %s (%s:%s)", param.TestName, testRuntime.String(), writeRuntime.String(), readRuntime.String())

		// Cleanup: Stop and remove the Redis container.
		if err := cli.ContainerStop(ctx, resp.ID, container.StopOptions{}); err != nil {
			log.Fatal(err)
		}
		if err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{}); err != nil {
			log.Fatal(err)
		}

		log.Printf("Container %s stopped and removed", containerName)
	}

	return writeRuntimes, readRuntimes
}

func TestRedisWithPipelines(param TestParam) (writeRuntime time.Duration, readRuntime time.Duration, err error) {
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

	writeStart := time.Now()

	// Add keys in batches
	for i := 0; i < n; i += maxPipelineSize {
		pipeline := conn.Pipeline()

		for j := i; j < i+maxPipelineSize; j++ {
			pipeline.Set(ctx, keys[j], value, time.Minute*30)
		}

		// Execute the pipeline
		_, err = pipeline.Exec(ctx)
		if err != nil {
			return writeRuntime, readRuntime, fmt.Errorf("pipeline.Exec: %v", err)
		}
	}

	writeRuntime = time.Since(writeStart)

	// Read back the keys in goroutines
	numRoutines := param.ReadAmp
	errChan := make(chan []error, numRoutines)

	readStart := time.Now()

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

	readRuntime = time.Since(readStart)

	// Check that there were no errors
	if len(errs) > 0 {
		return writeRuntime, readRuntime, fmt.Errorf("errs: %v", errs)
	}

	return writeRuntime, readRuntime, nil
}

func TestRedisWithoutPipelines(param TestParam) (writeRuntime time.Duration, readRuntime time.Duration, err error) {
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

	writeStart := time.Now()

	// Add keys
	for i := 0; i < n; i++ {
		_, err = conn.Set(ctx, keys[i], value, time.Minute*30).Result()
		if err != nil {
			return writeRuntime, readRuntime, fmt.Errorf("conn.Set: %v", err)
		}
	}

	writeRuntime = time.Since(writeStart)

	// Read back the keys in goroutines
	numRoutines := param.ReadAmp
	errChan := make(chan []error, numRoutines)

	readStart := time.Now()

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

	readRuntime = time.Since(readStart)

	// Check that there were no errors
	if len(errs) > 0 {
		return writeRuntime, readRuntime, fmt.Errorf("errs: %v", errs)
	}

	return writeRuntime, readRuntime, nil
}
