package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
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
		Version: "0.1.0",
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
			Value:   "data/follows.csv",
		},
		&cli.StringFlag{
			Name:    "sqlite-path",
			Usage:   "path to graph db sqlite file",
			EnvVars: []string{"GRAPHD_SQLITE_PATH"},
			Value:   "data/graphd.db",
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
	logLevel := slog.LevelInfo
	if cctx.Bool("debug") {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})))

	logger := slog.Default()

	dbExists := true
	_, err := os.Stat(cctx.String("sqlite-path"))
	if err != nil {
		if os.IsNotExist(err) {
			dbExists = false
		} else {
			slog.Error("failed to stat graph db", "error", err)
			return err
		}
	}

	graph, err := graphd.NewGraph(
		cctx.String("sqlite-path"),
		cctx.Duration("sync-interval"),
		cctx.Int("expected-node-count"),
		logger,
	)
	if err != nil {
		slog.Error("failed to create graph", "error", err)
		return err
	}

	go func() {
		if !dbExists {
			err = graph.LoadFromCSV(cctx.String("follows-csv"))
			if err != nil {
				slog.Error("failed to load graph from file", "error", err)
			}
		} else {
			err = graph.LoadFromSQLite(context.Background())
			if err != nil {
				slog.Error("failed to load graph from db", "error", err)
			}
		}
	}()

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

	e.GET("/doesFollow", h.GetDoesFollow)
	e.GET("/areMoots", h.GetAreMoots)
	e.GET("/intersectFollowers", h.GetIntersectFollowers)
	e.GET("/intersectFollowing", h.GetIntersectFollowing)
	e.GET("/follows_following", h.GetFollowsFollowing)

	e.POST("/follow", h.PostFollow)
	e.POST("/follows", h.PostFollows)

	e.POST("/unfollow", h.PostUnfollow)
	e.POST("/unfollows", h.PostUnfollows)

	e.GET("/flush_updated", h.GetFlushUpdates)

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
