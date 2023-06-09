FROM golang:1.20 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY cmd/image-processor ./cmd/image-processor

COPY Makefile ./

RUN make build-image-processor

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/image-processor .

RUN mkdir -p /app/data

CMD ["./image-processor"]
