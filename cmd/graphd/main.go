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

	_ "github.com/joho/godotenv/autoload"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/ericvolp12/bsky-experiments/pkg/graphd"
	graphdconnect "github.com/ericvolp12/bsky-experiments/pkg/graphd/gen/graphdconnect"
	"github.com/ericvolp12/bsky-experiments/pkg/graphd/server"
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
		&cli.StringFlag{
			Name:  "addr",
			Usage: "listen addr for http server",
			Value: ":1323",
		},
		&cli.StringFlag{
			Name:    "graph-csv",
			Usage:   "path to graph csv file",
			EnvVars: []string{"GRAPH_CSV"},
			Value:   "data/follows.csv",
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

	graph := graphd.NewGraph()

	err := graph.LoadFromCSV(cctx.String("graph-csv"))
	if err != nil {
		slog.Error("failed to load graph from file", "error", err)
		return err
	}

	graphDServer := server.NewGraphDServer(graph)

	mux := http.DefaultServeMux
	mux.Handle("/metrics", promhttp.Handler())

	path, handler := graphdconnect.NewGraphDServiceHandler(graphDServer)
	mux.Handle(path, handler)

	s := http.Server{
		Addr:     cctx.String("addr"),
		Handler:  h2c.NewHandler(mux, &http2.Server{}),
		ErrorLog: slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}

	shutdownServer := make(chan struct{})
	serverShutdown := make(chan struct{})
	go func() {
		log := slog.With("source", "server")

		log.Info("server listening", "addr", cctx.String("addr"))

		go func() {
			if err := s.ListenAndServe(); err != http.ErrServerClosed {
				log.Error("failed to start server", "error", err)
			}
		}()
		<-shutdownServer
		if err := s.Shutdown(context.Background()); err != nil {
			log.Error("failed to shutdown server", "error", err)
		}
		log.Info("server shut down")
		close(serverShutdown)
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		slog.Info("shutting down on signal")
	}

	slog.Info("shutting down, waiting for workers to clean up...")
	close(shutdownServer)

	<-serverShutdown
	slog.Info("shut down successfully")

	return nil
}
