package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

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
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug logging",
		},
		&cli.IntFlag{
			Name:  "port",
			Usage: "listen port for http server",
			Value: 1323,
		},
		&cli.StringFlag{
			Name:    "graph-csv",
			Usage:   "path to graph csv file",
			EnvVars: []string{"GRAPH_CSV"},
			Value:   "data/follows.csv",
		},
		&cli.StringFlag{
			Name:    "graph-binary",
			Usage:   "path to graph binary file",
			EnvVars: []string{"GRAPH_BINARY"},
			Value:   "data/graphd_graph.bin",
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
		Level: logLevel,
	})))

	graph := graphd.NewGraph(cctx.String("graph-csv"), cctx.String("graph-binary"))

	err := graph.LoadFromFile()
	if err != nil {
		slog.Error("failed to load graph from file", "error", err)
		return err
	}

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
	e.GET("/followersNotFollowing", h.GetFollowersNotFollowing)

	e.GET("/doesFollow", h.GetDoesFollow)
	e.GET("/areMoots", h.GetAreMoots)
	e.GET("/intersectFollowers", h.GetIntersectFollowers)
	e.GET("/intersectFollowing", h.GetIntersectFollowing)

	e.POST("/follow", h.PostFollow)
	e.POST("/follows", h.PostFollows)

	e.POST("/unfollow", h.PostUnfollow)
	e.POST("/unfollows", h.PostUnfollows)

	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: e,
	}

	shutdownEcho := make(chan struct{})
	echoShutdown := make(chan struct{})
	go func() {
		log := slog.With("source", "echo")

		log.Info("echo listening", "port", cctx.Int("port"))

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
	slog.Info("shut down successfully")

	return nil
}
