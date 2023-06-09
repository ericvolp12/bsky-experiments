FROM golang:1.20 as builder

WORKDIR /workspaces/bluesky

#RUN go clean --modcache && go mod tidy -v -e

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

COPY --from=builder /workspaces/bluesky/graph-builder .

RUN mkdir -p /workspaces/bluesky/data

CMD ["/workspaces/bluesky/graph-builder"]
