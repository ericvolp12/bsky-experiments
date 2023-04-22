# Makefile for Go project

# Variables
DOCKER_IMAGE_NAME = bsky-mention-counts
DOCKER_CONTAINER_NAME = bsky-mention-counts
DOCKERFILE = Dockerfile
LOCAL_DATA_DIR = data/
DOCKER_MOUNT_PATH = /app/data/
ENV_FILE = .env
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build the Go binary
build:
	@echo "Building Go binary..."
	$(GO_CMD) build -o mention-counter cmd/mention-counter/*.go

clean:
	@echo "Cleaning Go binary..."
	rm -f bsky-experiments

# Build the Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE_NAME) -f $(DOCKERFILE) .

# Run the Docker container
docker-run: docker-build
	@echo "Running Docker container..."
	docker run --name $(DOCKER_CONTAINER_NAME) --rm --env-file $(ENV_FILE) -v $(shell pwd)/$(LOCAL_DATA_DIR):$(DOCKER_MOUNT_PATH) $(DOCKER_IMAGE_NAME)

# Remove the Docker container
docker-clean:
	@echo "Removing Docker container..."
	docker rm $(DOCKER_CONTAINER_NAME)

.PHONY: build clean docker-build docker-run docker-clean
