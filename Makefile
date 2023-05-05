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
build-graph-builder:
	@echo "Building Graph Builder Go binary..."
	$(GO_CMD_W_CGO) build -race -o graph-builder cmd/graph-builder/*.go

# Build the Graph Builder Docker image
docker-build-graph-builder:
	@echo "Building Graph Builder Docker image..."
	docker build -t $(GRAPH_IMAGE_NAME) -f build/graph-builder/Dockerfile .

graph-builder-up:
	@echo "Starting Graph Builder..."
	docker-compose -f build/graph-builder/docker-compose.yml up --build -d

graph-builder-restart:
	@echo "Restarting Graph Builder..."
	docker-compose -f build/graph-builder/docker-compose.yml restart -t 5

# Build the Search API Go binary
build-search:
	@echo "Building Search Go binary..."
	$(GO_CMD) build -o search cmd/search/*.go

# Build the Search API Docker image
docker-build-search:
	@echo "Building Search Builder Docker image..."
	docker build -t $(SEARCH_IMAGE_NAME) -f build/search/Dockerfile .

search-up:
	@echo "Starting Search API..."
	docker-compose -f build/search/docker-compose.yml up --build -d

search-restart:
	@echo "Restarting Graph Builder..."
	docker-compose -f build/search/docker-compose.yml restart -t 5

# Generate SQLC Code
sqlc:
	@echo "Generating SQLC code..."
	sqlc generate -f pkg/search/sqlc.yaml

.PHONY: build-graph-builder docker-build-graph-builder build-search docker-build-search sqlc graph-builder-up search-up graph-builder-restart search-restart
