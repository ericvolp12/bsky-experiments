package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	objectdetection "github.com/ericvolp12/bsky-experiments/pkg/object-detection"
	"github.com/ericvolp12/bsky-experiments/pkg/search"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	var logger *zap.Logger

	if os.Getenv("DEBUG") == "true" {
		logger, _ = zap.NewDevelopment()
		logger.Info("Starting logger in DEBUG mode...")
	} else {
		logger, _ = zap.NewProduction()
		logger.Info("Starting logger in PRODUCTION mode...")
	}

	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("failed to sync logger on teardown: %+v", err.Error())
		}
	}()

	log := logger.Sugar()

	log.Info("Reading config from environment...")

	dbConnectionString := os.Getenv("REGISTRY_DB_CONNECTION_STRING")
	if dbConnectionString == "" {
		log.Fatal("REGISTRY_DB_CONNECTION_STRING environment variable is required")
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := installExportPipeline(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	postRegistry, err := search.NewPostRegistry(dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to create PostRegistry: %v", err)
	}
	defer postRegistry.Close()

	objectDetectionServiceHost := os.Getenv("OBJECT_DETECTION_SERVICE_HOST")
	if objectDetectionServiceHost == "" {
		log.Fatal("OBJECT_DETECTION_SERVICE_HOST environment variable is required")
	}

	detection := objectdetection.NewObjectDetection(objectDetectionServiceHost)

	// Start up a Metrics and Profiling goroutine
	go func() {
		log = log.With("source", "pprof_server")
		log.Info("starting pprof and prometheus server...")
		http.Handle("/metrics", promhttp.Handler())
		log.Info(http.ListenAndServe("0.0.0.0:8094", nil))
	}()

	for {
		log.Info("Processing images...")
		start := time.Now()
		unprocessedImages, err := postRegistry.GetUnprocessedImages(ctx, 50)
		if err != nil {
			if errors.As(err, &search.NotFoundError{}) {
				log.Info("No unprocessed images found, skipping process cycle...")
				continue
			}
			log.Errorf("Failed to get unprocessed images, skipping process cycle: %v", err)
			continue
		}

		imageMetas := make([]*objectdetection.ImageMeta, len(unprocessedImages))
		for i, image := range unprocessedImages {
			imageMetas[i] = &objectdetection.ImageMeta{
				CID:       image.CID,
				URL:       image.FullsizeURL,
				MimeType:  image.MimeType,
				CreatedAt: image.CreatedAt,
			}
		}

		results, err := detection.ProcessImages(ctx, imageMetas)
		if err != nil {
			log.Errorf("Failed to process images: %v", err)
			continue
		}

		executionTime := time.Now()

		successCount := 0

		for idx, result := range results {
			if len(result.Results) > 0 {
				successCount++
			}

			cvClasses, err := json.Marshal(result.Results)
			if err != nil {
				log.Errorf("Failed to marshal classes: %v", err)
				continue
			}

			err = postRegistry.AddCVDataToImage(
				ctx,
				result.Meta.CID,
				unprocessedImages[idx].PostID,
				executionTime,
				cvClasses,
			)
			if err != nil {
				log.Errorf("Failed to update image: %v", err)
				continue
			}
		}

		log.Infow("Finished processing images...",
			"batch_size", len(unprocessedImages),
			"successfully_processed_image_count", successCount,
			"processing_time", time.Since(start),
		)
		select {
		case <-ctx.Done():
			log.Info("Context cancelled, exiting...")
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func installExportPipeline(ctx context.Context) (func(context.Context) error, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	tracerProvider := newTraceProvider(exporter)
	otel.SetTracerProvider(tracerProvider)

	return tracerProvider.Shutdown, nil
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("BSkySearchAPI"),
		),
	)

	if err != nil {
		panic(err)
	}

	// initialize the traceIDRatioBasedSampler
	traceIDRatioBasedSampler := sdktrace.TraceIDRatioBased(1)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(traceIDRatioBasedSampler),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
