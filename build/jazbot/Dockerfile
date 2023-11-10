# Stage 1: Build the Go binary
FROM golang:1.21.1 AS builder

# Create a directory for the application
WORKDIR /app

# Fetch dependencies
COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY cmd/jazbot ./cmd/jazbot

COPY Makefile ./

# Build the application
RUN make build-jazbot

# Stage 2: Import SSL certificates
FROM alpine:latest as certs

RUN apk --update add ca-certificates

# Stage 3: Build a minimal Docker image
FROM debian:stable-slim

# Import the SSL certificates from the first stage.
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy the binary from the first stage.
COPY --from=builder /app/jazbot .

# Set the startup command to run the binary
CMD ["./jazbot"]
