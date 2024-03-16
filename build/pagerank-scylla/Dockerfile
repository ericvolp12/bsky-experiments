FROM rust:1.76 as builder

WORKDIR /usr/src/pagerank-scylla
COPY ./rust/pagerank-scylla .

RUN cargo install --path .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/pagerank-scylla /usr/local/bin/pagerank-scylla

CMD ["pagerank-scylla"]
