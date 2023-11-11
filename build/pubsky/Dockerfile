FROM golang:1.21.1 as bin-builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ pkg/

COPY cmd/ cmd/

COPY Makefile Makefile

RUN make build-pubsky

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM node:18-alpine as web-builder

WORKDIR /app

COPY ts/pubsky /app/

RUN yarn install --frozen-lockfile

RUN yarn build

FROM debian:stable-slim

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=bin-builder /app/pubsky .

COPY --from=web-builder /app/dist/ public/

CMD ["./pubsky"]
