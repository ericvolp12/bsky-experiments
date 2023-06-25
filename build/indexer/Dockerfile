FROM golang:1.20 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY cmd/indexer ./cmd/indexer

COPY Makefile ./

RUN make build-indexer

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/indexer .

RUN mkdir -p /app/data

CMD ["./indexer"]
