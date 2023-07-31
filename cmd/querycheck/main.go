package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ericvolp12/bsky-experiments/pkg/querycheck"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "querycheck",
		Usage:   "a postgresql query plan checker",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
		&cli.IntFlag{
			Name:    "port",
			Usage:   "port to serve metrics on",
			Value:   8080,
			EnvVars: []string{"PORT"},
		},
	}

	app.Action = Querycheck

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer trace.Tracer

type Querychecker struct {
	connectionURL string
}

func NewQuerychecker(ctx context.Context, connectionURL string) (*Querychecker, error) {
	return &Querychecker{
		connectionURL: connectionURL,
	}, nil
}

func (q *Querychecker) CheckQueryPlan(ctx context.Context, query string) error {
	conn, err := pgx.Connect(ctx, q.connectionURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) "+query)
	if err != nil {
		return err
	}

	for rows.Next() {
		var plans querycheck.QueryPlans
		err := rows.Scan(&plans)
		if err != nil {
			return err
		}
		for _, plan := range plans {
			plan.Print()
		}
	}

	return nil
}

// Querycheck is the main function for querycheck
func Querycheck(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	rawlog, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %+v\n", err)
	}
	defer func() {
		log.Printf("main function teardown\n")
		err := rawlog.Sync()
		if err != nil {
			log.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := rawlog.Sugar().With("source", "querycheck_main")

	log.Info("starting querycheck")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "Querycheck", 0.01)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	wg := sync.WaitGroup{}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	metricServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: mux,
	}

	// Startup metrics server
	go func() {
		wg.Add(1)
		defer wg.Done()
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "metrics_server")

		log.Infof("metrics server listening on port %d", cctx.Int("port"))

		if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("failed to start metrics server: %+v\n", err)
		}
		log.Info("metrics server shut down successfully")
	}()

	// Start the query checker
	querychecker, err := NewQuerychecker(ctx, cctx.String("postgres-url"))
	if err != nil {
		log.Fatalf("failed to create querychecker: %+v\n", err)
	}

	go func() {
		wg.Add(1)
		defer wg.Done()
		rawlog, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to create logger: %+v\n", err)
		}
		log := rawlog.Sugar().With("source", "query_checker")

		log.Infof("query checker started")

		query := `SELECT *
		FROM likes
		WHERE subject_actor_did = 'did:plc:q6gjnaw2blty4crticxkmujt'
			AND subject_namespace = 'app.bsky.feed.post'
			AND subject_rkey = '3k3jf5lgbsw24'
		ORDER BY created_at DESC
		LIMIT 50;`

		// Check the query plan every 30 seconds
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		err = querychecker.CheckQueryPlan(ctx, query)
		if err != nil {
			log.Errorf("failed to check query plan: %+v\n", err)
		}

		for {
			select {
			case <-ticker.C:
				err := querychecker.CheckQueryPlan(ctx, query)
				if err != nil {
					log.Errorf("failed to check query plan: %+v\n", err)
				}
			case <-ctx.Done():
				log.Info("shutting down query checker")
				return
			}
		}
	}()

	select {
	case <-signals:
		cancel()
		fmt.Println("shutting down on signal")
	case <-ctx.Done():
		fmt.Println("shutting down on context done")
	}

	log.Info("shutting down, waiting for workers to clean up...")

	if err := metricServer.Shutdown(ctx); err != nil {
		log.Errorf("failed to shut down metrics server: %+v\n", err)
		wg.Done()
	}

	wg.Wait()
	log.Info("shut down successfully")

	return nil
}
