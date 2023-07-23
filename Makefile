# Makefile for Go project

# Variables
GRAPH_IMAGE_NAME = bsky-graph-builder
SEARCH_IMAGE_NAME = bsky-search-api
LOCAL_DATA_DIR = data/
DOCKER_MOUNT_PATH = /app/data/
ENV_FILE = .env
GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build the Graph Builder Go binary
.PHONY: build-graph-builder
build-graph-builder:
	@echo "Building Graph Builder Go binary..."
	$(GO_CMD_W_CGO) build -o graph-builder cmd/graph-builder/*.go

# Build the Graph Builder Docker image
.PHONY: docker-build-graph-builder
docker-build-graph-builder:
	@echo "Building Graph Builder Docker image..."
	docker build -t $(GRAPH_IMAGE_NAME) -f build/graph-builder/Dockerfile .

# Start up the Graph Builder
.PHONY: graph-builder-up
graph-builder-up:
	@echo "Starting Graph Builder..."
	docker compose -f build/graph-builder/docker-compose.yml up --build -d

# Restart the Graph Builder
.PHONY: graph-builder-restart
graph-builder-restart:
	@echo "Restarting Graph Builder..."
	docker compose -f build/graph-builder/docker-compose.yml restart -t 5

# Build the Search API Go binary
.PHONY: build-search
build-search:
	@echo "Building Search Go binary..."
	$(GO_CMD) build -o search cmd/search/*.go

# Build the Search API Docker image
.PHONY: docker-build-search
docker-build-search:
	@echo "Building Search Builder Docker image..."
	docker build -t $(SEARCH_IMAGE_NAME) -f build/search/Dockerfile .

.PHONY: search-up
search-up:
	@echo "Starting Search API..."
	docker compose -f build/search/docker-compose.yml up --build -d

.PHONY: search-restart
search-restart:
	@echo "Restarting Graph Builder..."
	docker compose -f build/search/docker-compose.yml restart -t 5

# Build the Layout Rust Service
.PHONY: layout-up
layout-up:
	@echo "Starting Rust Layout API..."
	docker compose -f build/layout/docker-compose.yml up --build -d

# Build the Layout TypeScript Service
.PHONY: ts-layout-up
ts-layout-up:
	@echo "Starting TypeScript Layout API..."
	docker compose -f build/ts-layout/docker-compose.yml up --build -d

# Build the Sentiment Analysis Python Service
.PHONY: sentiment-up
sentiment-up:
	@echo "Starting Sentiment Analysis API..."
	docker compose -f build/sentiment/docker-compose.yml up --build -d

# Build the Sentiment Analysis Python Service with GPU Acceleration
.PHONY: sentiment-gpu-up
sentiment-gpu-up:
	@echo "Starting Sentiment Analysis API with GPU Acceleration..."
	docker compose -f build/sentiment/gpu.docker-compose.yml up --build -d

# Build the Object Detection Python Service
.PHONY: object-detection-up
object-detection-up:
	@echo "Starting Object Detection API..."
	docker compose -f build/object-detection/docker-compose.yml up --build -d

# Build the Object Detection Python Service with GPU Acceleration
.PHONY: object-detection-gpu-up
object-detection-gpu-up:
	@echo "Starting Object Detection API with GPU Acceleration..."
	docker compose -f build/object-detection/gpu.docker-compose.yml up --build -d

# Build the Indexer Go binary
.PHONY: build-indexer
build-indexer:
	@echo "Building Indexer Go binary..."
	$(GO_CMD_W_CGO) build -o indexer cmd/indexer/*.go

.PHONY: indexer-up
indexer-up:
	@echo "Starting Indexer..."
	docker compose -f build/indexer/docker-compose.yml up --build -d

# Build the Feedgen Go binary
.PHONY: build-feedgen-go
build-feedgen-go:
	@echo "Building Feed Generator Go binary..."
	$(GO_CMD_W_CGO) build -o feedgen cmd/feed-generator/*.go

.PHONY: feedgen-go-up
feedgen-go-up:
	@echo "Starting Go Feed Generator..."
	docker compose -f build/feedgen-go/docker-compose.yml up --build -d

# Build the PLC Go binary
.PHONY: build-plc
build-plc:
	@echo "Building PLC binary..."
	$(GO_CMD_W_CGO) build -o plc cmd/plc/*.go

.PHONY: plc-up
plc-up:
	@echo "Starting PLC..."
	docker compose -f build/plc/docker-compose.yml up --build -d

# Start up the Pyroscope Continuous Profiler Backend
.PHONY: pyroscope-up
pyroscope-up:
	@echo "Starting Pyroscope..."
	docker compose -f build/pyroscope/docker-compose.yml up --build -d

# Start up the DragonflyDB Database
.PHONY: dragonfly-up
dragonfly-up:
	@echo "Starting DragonflyDB..."
	docker compose -f build/dragonfly/docker-compose.yml up -d

# Start up ScyllaDB
.PHONY: scylla-up
scylla-up:
	@echo "Starting ScyllaDB..."
	docker compose -f build/scylla/docker-compose.yml up -d

# Build the Consumer
.PHONY: build-consumer
build-consumer:
	@echo "Building Consumer Go binary..."
	$(GO_CMD_W_CGO) build -o consumer cmd/consumer/*.go

.PHONY: consumer-up
consumer-up:
	@echo "Starting Consumer..."
	docker compose -f build/consumer/docker-compose.yml up --build -d

# Generate SQLC Code
.PHONY: sqlc
sqlc:
	@echo "Generating SQLC code..."
	sqlc generate -f pkg/search/sqlc.yaml
