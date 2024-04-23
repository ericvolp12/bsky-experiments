package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"github.com/ericvolp12/bsky-experiments/pkg/graphd"
	"github.com/ericvolp12/bsky-experiments/pkg/graphd/handlers"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "graphd",
		Usage:   "relational graph daemon",
		Version: "0.1.1",
	}

	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug logging",
		},
		&cli.StringFlag{
			Name:  "listen-addr",
			Usage: "listen address for http server",
			Value: ":1323",
		},
		&cli.IntFlag{
			Name:    "expected-node-count",
			Usage:   "expected number of nodes in the graph",
			EnvVars: []string{"GRAPHD_EXPECTED_NODE_COUNT"},
		},
		&cli.StringFlag{
			Name:    "follows-csv",
			Usage:   "path to graph csv file",
			EnvVars: []string{"GRAPHD_FOLLOWS_CSV"},
		},
		&cli.StringFlag{
			Name:    "sqlite-path",
			Usage:   "path to directory to store graphd sqlite dbs",
			EnvVars: []string{"GRAPHD_SQLITE_PATH"},
			Value:   "data/graphd/",
		},
		&cli.DurationFlag{
			Name:    "sync-interval",
			Usage:   "interval to flush updates to disk",
			EnvVars: []string{"GRAPHD_SYNC_INTERVAL"},
			Value:   5 * time.Second,
		},
	}

	app.Action = GraphD

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}

func GraphD(cctx *cli.Context) error {
	ctx := cctx.Context
	logLevel := slog.LevelInfo
	if cctx.Bool("debug") {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})))

	logger := slog.Default()

	absPath, err := filepath.Abs(cctx.String("sqlite-path"))
	if err != nil {
		slog.Error("failed to get absolute path", "error", err, "path", cctx.String("sqlite-path"))
		return err
	}
	dbDir := filepath.Clean(absPath)

	bulkMode := cctx.IsSet("follows-csv")
	cfg := graphd.DefaultGraphConfig()
	cfg.DBPath = dbDir
	if bulkMode {
		cfg.CacheSize = 10_000_000
	}

	start := time.Now()

	logger.Info("initializing graph", "db_path", dbDir)

	graph, err := graphd.NewGraph(ctx, logger, cfg)
	if err != nil {
		slog.Error("failed to create graph", "error", err)
		return err
	}

	go func() {
		if bulkMode {
			slog.Info("loading graph from csv", "path", cctx.String("follows-csv"))
			err = graph.LoadFromCSV(cctx.String("follows-csv"))
			if err != nil {
				slog.Error("failed to load graph from file", "error", err)
			}
		}
	}()

	logger.Info("graphd initialized", "duration", time.Since(start))

	e := echo.New()

	h := handlers.NewHandlers(graph)

	e.GET("/_health", h.Health)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/debug/*", echo.WrapHandler(http.DefaultServeMux))

	echoProm := echoprometheus.NewMiddlewareWithConfig(echoprometheus.MiddlewareConfig{
		Namespace: "graphd",
		HistogramOptsFunc: func(opts prometheus.HistogramOpts) prometheus.HistogramOpts {
			opts.Buckets = prometheus.ExponentialBuckets(0.00001, 2, 20)
			return opts
		},
	})

	e.Use(echoProm)

	e.GET("/followers", h.GetFollowers)
	e.GET("/following", h.GetFollowing)
	e.GET("/moots", h.GetMoots)
	e.GET("/followers_not_following", h.GetFollowersNotFollowing)

	e.GET("/does_follow", h.GetDoesFollow)
	e.GET("/are_moots", h.GetAreMoots)
	e.GET("/intersect_followers", h.GetIntersectFollowers)
	e.GET("/intersect_following", h.GetIntersectFollowing)
	e.GET("/follows_following", h.GetFollowsFollowing)

	e.POST("/follow", h.PostFollow)
	e.POST("/follows", h.PostFollows)

	e.POST("/unfollow", h.PostUnfollow)
	e.POST("/unfollows", h.PostUnfollows)

	s := &http.Server{
		Addr:    cctx.String("listen-addr"),
		Handler: e,
	}

	shutdownEcho := make(chan struct{})
	echoShutdown := make(chan struct{})
	go func() {
		log := slog.With("component", "graphd_echo")

		log.Info("graphd listening", "addr", cctx.String("listen-addr"))

		go func() {
			if err := s.ListenAndServe(); err != http.ErrServerClosed {
				log.Error("failed to start echo", "error", err)
			}
		}()
		<-shutdownEcho
		if err := s.Shutdown(context.Background()); err != nil {
			log.Error("failed to shutdown echo", "error", err)
		}
		log.Info("echo shut down")
		close(echoShutdown)
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		slog.Info("shutting down on signal")
	}

	slog.Info("shutting down, waiting for workers to clean up...")
	close(shutdownEcho)
	<-echoShutdown

	graph.Shutdown()

	slog.Info("shut down successfully")

	return nil
}
