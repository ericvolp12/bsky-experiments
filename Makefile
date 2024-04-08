# Makefile for Go project

# Variables
GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Shared preparation steps
.PHONY: prepare
prepare:
	mkdir -p output

# Build the Graph Builder Go binary
.PHONY: build-graph-builder
build-graph-builder: prepare
	@echo "Building Graph Builder Go binary..."
	$(GO_CMD_W_CGO) build -o output/graph-builder cmd/graph-builder/*.go

# Start up the Graph Builder
.PHONY: graph-builder-up
graph-builder-up:
	@echo "Starting Graph Builder..."
	docker compose -f build/graph-builder/docker-compose.yml up --build -d

# Build the Search API Go binary
.PHONY: build-search
build-search: prepare
	@echo "Building Search Go binary..."
	$(GO_CMD) build -o output/search cmd/search/*.go

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
build-indexer: prepare
	@echo "Building Indexer Go binary..."
	$(GO_CMD_W_CGO) build -o output/indexer cmd/indexer/*.go

.PHONY: indexer-up
indexer-up:
	@echo "Starting Indexer..."
	docker compose -f build/indexer/docker-compose.yml up --build -d

# Build the Feedgen Go binary
.PHONY: build-feedgen-go
build-feedgen-go: prepare
	@echo "Building Feed Generator Go binary..."
	$(GO_CMD_W_CGO) build -o output/feedgen cmd/feed-generator/*.go

.PHONY: feedgen-go-up
feedgen-go-up:
	@echo "Starting Go Feed Generator..."
	docker compose -f build/feedgen-go/docker-compose.yml up --build -d

# Build the PLC Go binary
.PHONY: build-plc
build-plc: prepare
	@echo "Building PLC binary..."
	$(GO_CMD_W_CGO) build -o output/plc cmd/plc/*.go

.PHONY: plc-up
plc-up:
	@echo "Starting PLC..."
	docker compose -f build/plc/docker-compose.yml up --build -d

# Start up the Redis Store
.PHONY: redis-up
redis-up:
	@echo "Starting Redis..."
	docker compose -f build/redis/docker-compose.yml up -d

# Stop the Redis Store
.PHONY: redis-down
redis-down:
	@echo "Stopping Redis..."
	docker compose -f build/redis/docker-compose.yml down

# Build the Consumer
.PHONY: build-consumer
build-consumer: prepare
	@echo "Building Consumer Go binary..."
	$(GO_CMD_W_CGO) build -o output/consumer cmd/consumer/*.go

.PHONY: consumer-up
consumer-up:
	@echo "Starting Consumer..."
	docker compose -f build/consumer/docker-compose.yml up --build -d

# Build Jazbot
.PHONY: build-jazbot
build-jazbot: prepare
	@echo "Building Jazbot Go binary..."
	$(GO_CMD_W_CGO) build -o output/jazbot cmd/jazbot/*.go

.PHONY: jazbot-up
jazbot-up:
	@echo "Starting Jazbot..."
	docker compose -f build/jazbot/docker-compose.yml up --build -d

# Generate SQLC Code
.PHONY: sqlc
sqlc:
	@echo "Generating SQLC code for search..."
	sqlc generate -f pkg/search/sqlc.yaml
	@echo "Generating SQLC code for store..."
	sqlc generate -f pkg/consumer/store/sqlc.yaml

.PHONY: empty-plc
empty-plc:
	@echo "Emptying PLC Mirror in Redis..."
	redis-cli --scan --pattern "plc_directory:*" | xargs -L 100 redis-cli DEL

.PHONY: empty-fanout
empty-fanout:
	@echo "Emptying Fanout keys in Redis..."
	redis-cli --scan --pattern "cg:*" | xargs -L 1000 redis-cli DEL

.PHONY: build-graphd
build-graphd: prepare
	@echo "Building GraphD Go binary..."
	$(GO_CMD_W_CGO) build -o output/graphd cmd/graphd/*.go
	
.PHONY: graphd-up
graphd-up: # Runs graphd docker container
	@echo "Starting Graphd..."
	docker compose -f build/graphd/docker-compose.yml up --build -d

.PHONY: graphd-down
graphd-down: # Stops graphd docker container
	@echo "Stopping Graphd..."
	docker compose -f build/graphd/docker-compose.yml down

.PHONY: run-dev-graphd
run-dev-graphd: .env ## Runs graphd for local dev
	@echo "Running Graphd..."
	go run ./cmd/graphd

.PHONY: rgraphd-up
rgraphd-up: # Runs rgraphd docker container
	@echo "Starting rGraphD..."
	docker compose -f build/rgraphd/docker-compose.yml up --build -d

.PHONY: rgraphd-down
rgraphd-down: # Stops rgraphd docker container
	@echo "Stopping rGraphD..."
	docker compose -f build/rgraphd/docker-compose.yml down

# Build Pagerank
.PHONY: pagerank-up
pagerank-up:
	@echo "Starting Pagerank..."
	docker compose -f build/pagerank/docker-compose.yml up --build -d
	
.PHONY: pagerank-down
pagerank-down:
	@echo "Stopping Pagerank..."
	docker compose -f build/pagerank/docker-compose.yml down
