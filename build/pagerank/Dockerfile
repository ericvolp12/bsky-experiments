FROM rust:1.76 as builder

WORKDIR /usr/src/pagerank

COPY ./rust/pagerank .

RUN cargo install --path .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/pagerank /usr/local/bin/pagerank

CMD ["pagerank"]
