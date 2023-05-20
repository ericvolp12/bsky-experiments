FROM golang:1.20 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ pkg/

COPY cmd/ cmd/

COPY Makefile Makefile

RUN make build-feedgen-go

FROM alpine:latest as certs

RUN apk --update add ca-certificates

FROM alpine:latest

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/feedgen .

# COPY public/ public/
# COPY specs/feedgen-go.openapi.yaml public/openapi3-spec.yaml
# COPY graph.bin /app/graph.bin

CMD ["./feedgen"]
