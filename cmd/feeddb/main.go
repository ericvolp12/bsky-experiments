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
	"time"

	_ "github.com/joho/godotenv/autoload"

	"github.com/ericvolp12/bsky-experiments/pkg/consumer/store"
	"github.com/ericvolp12/bsky-experiments/pkg/feeddb"
	"github.com/ericvolp12/bsky-experiments/pkg/feeddb/handlers"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "feeddb",
		Usage:   "feed database daemon",
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
			Value: 1325,
		},
		&cli.StringFlag{
			Name:    "postgres-url",
			Usage:   "postgres url for storing events",
			Value:   "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			EnvVars: []string{"POSTGRES_URL"},
		},
	}

	app.Action = FeedDB

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}

func FeedDB(cctx *cli.Context) error {
	logLevel := slog.LevelInfo
	if cctx.Bool("debug") {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})))

	ctx := cctx.Context

	cstore, err := store.NewStore(cctx.String("postgres-url"))
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	db := feeddb.NewDB(cstore)

	err = db.LoadAllPosts(ctx)
	if err != nil {
		return fmt.Errorf("failed to load posts: %w", err)
	}

	e := echo.New()

	h := handlers.NewHandlers(db)

	e.GET("/_health", h.Health)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/debug/*", echo.WrapHandler(http.DefaultServeMux))

	e.GET("/posts/by_regex", h.GetPostsPageByRegex)
	e.GET("/posts/by_authors", h.GetPostsPageByAuthors)

	echoProm := echoprometheus.NewMiddlewareWithConfig(echoprometheus.MiddlewareConfig{
		Namespace: "feeddb",
		HistogramOptsFunc: func(opts prometheus.HistogramOpts) prometheus.HistogramOpts {
			opts.Buckets = prometheus.ExponentialBuckets(0.0001, 2, 20)
			return opts
		},
	})

	e.Use(echoProm)

	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: e,
	}

	shutdownPostLoader := make(chan struct{})
	postLoaderShutdown := make(chan struct{})
	go func() {
		log := slog.With("source", "post_loader")

		log.Info("starting post loader")

		t := time.NewTicker(10 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				ctx := context.Background()
				err := db.LoadNewPosts(ctx)
				if err != nil {
					log.Error("failed to load new posts", "error", err)
				}
			case <-shutdownPostLoader:
				log.Info("shutting down post loader")
				close(postLoaderShutdown)
				return
			}
		}
	}()

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
	close(shutdownPostLoader)

	<-echoShutdown
	<-postLoaderShutdown

	slog.Info("shut down successfully")

	return nil
}
