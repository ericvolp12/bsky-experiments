# Use the rust image as the base
FROM rust:1.76-slim-bookworm as builder

RUN apt-get update && apt-get install -y libssl-dev pkg-config make

# Set the working directory
WORKDIR /usr/src/graphd

# Copy over the Cargo.toml and download dependencies
COPY rust/graphd/Cargo.toml rust/graphd/Cargo.lock ./

# Copy source code
COPY rust/graphd/. .

# Build release binary 
RUN cargo build --release 

# Start a new stage to have a lean production image
FROM debian:bookworm-slim

# Copy the release binary from the builder stage
COPY --from=builder /usr/src/graphd/target/release/graphd /usr/local/bin/graphd

# Set the CSV data location
ENV CSV_PATH=/data/follows.csv

# Create data directory 
RUN mkdir -p /data

# Run the app
CMD ["graphd"]
