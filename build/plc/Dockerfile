FROM golang:1.22 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY cmd/plc ./cmd/plc

COPY Makefile ./

RUN make build-plc

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/plc .

RUN mkdir -p /app/data

CMD ["./plc"]
