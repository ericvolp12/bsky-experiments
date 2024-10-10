package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/otel/trace"

	"github.com/ericvolp12/bsky-experiments/pkg/rss"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "skypub",
		Usage:   "bluesky rss/atom feed mirror",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "metrics-addr",
			Usage: "The address the metric endpoint binds to.",
			Value: ":10232",
		},
		&cli.StringFlag{
			Name:  "db-path",
			Usage: "path to sqlite db",
			Value: "data/skypub/skypub.db",
		},
		&cli.StringFlag{
			Name:    "handle",
			Usage:   "handle to use for the user",
			EnvVars: []string{"SKYPUB_HANDLE"},
		},
		&cli.StringFlag{
			Name:    "app-password",
			Usage:   "app password for the user",
			EnvVars: []string{"SKYPUB_APP_PASSWORD"},
		},
	}

	app.Action = Skypub

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer trace.Tracer

// Skypub is the main function for the skypub rss/atom feed mirror
func Skypub(cctx *cli.Context) error {
	ctx := cctx.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logLevel := slog.LevelInfo
	if cctx.Bool("debug") {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})))

	logger := slog.Default()

	logger.Info("skypub starting")

	feedConsumer, err := rss.NewFeedConsumer(logger, cctx.String("db-path"))
	if err != nil {
		return err
	}

	// feedConsumer.AddFeed("https://jazco.dev/feed", "did:plc:q6gjnaw2blty4crticxkmujt")
	_, err = feedConsumer.AddUser(ctx, cctx.String("handle"), cctx.String("app-password"))
	if err != nil {
		return fmt.Errorf("failed to add user: %w", err)
	}

	go feedConsumer.Start()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		logger.Info("shutting down on signal")
	}

	logger.Info("shutting down, waiting for workers to clean up...")
	feedConsumer.Shutdown()
	logger.Info("shut down successfully")

	return nil
}
