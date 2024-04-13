FROM rust:1.76-slim-bullseye as builder

WORKDIR /usr/src/pagerank

COPY ./rust/pagerank .

RUN cargo install --path .

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/pagerank /usr/local/bin/pagerank

CMD ["pagerank"]
