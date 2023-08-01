FROM golang:1.20 as bin-builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ pkg/

COPY cmd/ cmd/

COPY Makefile Makefile

RUN make build-querycheck

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=bin-builder /app/querycheck .

CMD ["./querycheck"]
