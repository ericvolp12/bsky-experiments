FROM golang:1.20 as builder

WORKDIR /app

RUN go mod tidy

COPY go.mod go.sum ./

RUN go mod download

RUN go mod tidy

COPY pkg ./pkg

COPY cmd/graph-builder ./cmd/graph-builder

COPY Makefile ./

RUN make build-graph-builder

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/graph-builder .

RUN mkdir -p /app/data

CMD ["./graph-builder"]
