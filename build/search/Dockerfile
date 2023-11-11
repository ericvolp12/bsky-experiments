FROM golang:1.21.1 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ pkg/

COPY cmd/ cmd/

COPY Makefile Makefile

RUN make build-search

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM alpine:latest

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/search .

COPY public/ public/

COPY specs/search.openapi.yaml public/openapi3-spec.yaml

CMD ["./search"]
