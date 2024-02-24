FROM golang:1.21.1 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY cmd/graph-builder ./cmd/graph-builder

COPY Makefile ./

RUN make build-graph-builder

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/output/graph-builder .

RUN mkdir -p /app/data

CMD ["./graph-builder"]
