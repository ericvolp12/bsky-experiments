# Use the rust image as the base
FROM rust:slim-buster as builder

# Set the working directory
WORKDIR /usr/src/graphd

# Copy over the Cargo.toml and download dependencies
COPY rust/graphd/Cargo.toml rust/graphd/Cargo.lock ./

# Copy source code
COPY rust/graphd/. .

# Build release binary 
RUN cargo build --release 

# Start a new stage to have a lean production image
FROM debian:buster-slim

# Copy the release binary from the builder stage
COPY --from=builder /usr/src/graphd/target/release/graphd /usr/local/bin/graphd

# Set the CSV data location
ENV CSV_PATH=/data/follows.csv

# Create data directory 
RUN mkdir -p /data

# Run the app
CMD ["graphd"]
