# Makefile for Go project

# Variables
GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Build the Search API Go binary
.PHONY: build-search
build-search:
	@echo "Building Search Go binary..."
	$(GO_CMD) build -o search cmd/search/*.go

.PHONY: search-up
search-up:
	@echo "Starting Search API..."
	docker compose -f build/search/docker-compose.yml up --build -d

.PHONY: search-restart
search-restart:
	@echo "Restarting Search API..."
	docker compose -f build/search/docker-compose.yml restart -t 5

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

# Build the Translation Python Service
.PHONY: translation-up
translation-up:
	@echo "Starting Translation API..."
	docker compose -f build/translation/docker-compose.yml up --build -d

# Build the Translation Analysis Python Service with GPU Acceleration
.PHONY: translation-gpu-up
translation-gpu-up:
	@echo "Starting Translation API with GPU Acceleration..."
	docker compose -f build/translation/gpu.docker-compose.yml up --build -d

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
build-consumer:
	@echo "Building Consumer Go binary..."
	$(GO_CMD_W_CGO) build -o consumer cmd/consumer/*.go

.PHONY: consumer-up
consumer-up:
	@echo "Starting Consumer..."
	docker compose -f build/consumer/docker-compose.yml up --build -d

# Build Jazbot
.PHONY: build-jazbot
build-jazbot:
	@echo "Building Jazbot Go binary..."
	$(GO_CMD_W_CGO) build -o jazbot cmd/jazbot/*.go

.PHONY: jazbot-up
jazbot-up:
	@echo "Starting Jazbot..."
	docker compose -f build/jazbot/docker-compose.yml up --build -d

# Generate SQLC Code
.PHONY: sqlc
sqlc:
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
build-graphd:
	@echo "Building GraphD..."
	$(GO_CMD_W_CGO) build -v -o graphd cmd/graphd/*.go

.PHONY: graphd-up
graphd-up: # Runs graphd docker container
	@echo "Starting GraphD..."
	docker compose -f build/graphd/docker-compose.yml up --build -d

.PHONY: graphd-down
graphd-down: # Stops graphd docker container
	@echo "Stopping GraphD..."
	docker compose -f build/graphd/docker-compose.yml down

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

.PHONY: skypub
skypub: .env
	go run cmd/skypub/main.go
