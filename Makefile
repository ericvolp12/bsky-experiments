# Makefile for Go project

# Variables
GRAPH_IMAGE_NAME = bsky-graph-builder
SEARCH_IMAGE_NAME = bsky-search-api
LOCAL_DATA_DIR = data/
DOCKER_MOUNT_PATH = /workspaces/bluesky/data/
ENV_FILE = .env
GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build the Graph Builder Go binary
build-graph-builder:
	@echo "Building Graph Builder Go binary..."
	$(GO_CMD_W_CGO) build -o graph-builder $(PWD)/cmd/graph-builder/*.go

# Build the Graph Builder Docker image
docker-build-graph-builder:
	@echo "Building Graph Builder Docker image..."
	docker build -t $(GRAPH_IMAGE_NAME) -f $(PWD)/build/graph-builder/Dockerfile .

graph-builder-up:
	@echo "Starting Graph Builder..."
	@echo $(PWD)
	ls -a
	ls -a build
	ls -a build/graph-builder
	docker compose -f $(PWD)/build/graph-builder/docker-compose.yml up --build -d

graph-builder-restart:
	@echo "Restarting Graph Builder..."
	docker compose -f $(PWD)/build/graph-builder/docker-compose.yml restart -t 5

# Build the Search API Go binary
build-search:
	@echo "Building Search Go binary..."
	$(GO_CMD) build -o search $(PWD)/cmd/search/*.go

# Build the Search API Docker image
docker-build-search:
	@echo "Building Search Builder Docker image..."
	docker build -t $(SEARCH_IMAGE_NAME) -f $(PWD)/build/search/Dockerfile .

search-up:
	@echo "Starting Search API..."
	docker compose -f $(PWD)/build/search/docker-compose.yml up --build -d

search-restart:
	@echo "Restarting Graph Builder..."
	docker compose -f $(PWD)/build/search/docker-compose.yml restart -t 5

# Build the Layout Rust Service
layout-up:
	@echo "Starting Rust Layout API..."
	docker compose -f $(PWD)/build/layout/docker-compose.yml up --build -d

# Build the Layout TypeScript Service
ts-layout-up:
	@echo "Starting TypeScript Layout API..."
	docker compose -f $(PWD)/build/ts-layout/docker-compose.yml up --build -d

# Build the Sentiment Analysis Python Service
sentiment-up:
	@echo "Starting Sentiment Analysis API..."
	docker compose -f $(PWD)/build/sentiment/docker-compose.yml up --build -d

# Build the Sentiment Analysis Python Service with GPU Acceleration
sentiment-gpu-up:
	@echo "Starting Sentiment Analysis API with GPU Acceleration..."
	docker compose -f $(PWD)/build/sentiment/gpu.docker-compose.yml up --build -d

# Build the Object Detection Python Service
object-detection-up:
	@echo "Starting Object Detection API..."
	docker compose -f $(PWD)/build/object-detection/docker-compose.yml up --build -d

# Build the Object Detection Python Service with GPU Acceleration
object-detection-gpu-up:
	@echo "Starting Object Detection API with GPU Acceleration..."
	docker compose -f $(PWD)/build/object-detection/gpu.docker-compose.yml up --build -d

# Build the Image Processor Go binary
build-image-processor:
	@echo "Building Image Processor Go binary..."
	$(GO_CMD_W_CGO) build -o image-processor $(PWD)/cmd/image-processor/*.go

image-processor-up:
	@echo "Starting Image Processor..."
	docker compose -f $(PWD)/build/image-processor/docker-compose.yml up --build -d

# Build the Feedgen Go binary
build-feedgen-go:
	@echo "Building Feed Generator Go binary..."
	$(GO_CMD) build -o feedgen $(PWD)/cmd/feed-generator/*.go

feedgen-go-up:
	@echo "Starting Go Feed Generator..."
	docker compose -f $(PWD)/build/feedgen-go/docker-compose.yml up --build -d

# Start up the Pyroscope Continuous Profiler Backend
pyroscope-up:
	@echo "Starting Pyroscope..."
	docker compose -f $(PWD)/build/pyroscope/docker-compose.yml up --build -d

# Start up the DragonflyDB Database
dragonfly-up:
	@echo "Starting DragonflyDB..."
	docker compose -f $(PWD)/build/dragonfly/docker-compose.yml up -d

# Generate SQLC Code
sqlc:
	@echo "Generating SQLC code..."
	sqlc generate -f $(PWD)/pkg/search/sqlc.yaml

.PHONY: build-graph-builder docker-build-graph-builder build-search docker-build-search sqlc graph-builder-up search-up graph-builder-restart search-restart layout-up ts-layout-up sentiment-up sentiment-gpu-up feedgen-up object-detection-up object-detection-gpu-up build-image-processor image-processor-up build-feedgen-go feedgen-go-up
