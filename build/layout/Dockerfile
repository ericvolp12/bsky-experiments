# Use the official Rust image as the base image
FROM debian:stable-slim as build

# install CURL
RUN apt-get update \
    && apt-get install -y curl gcc \
    && rm -rf /var/lib/apt/lists/*

# install rust toolchain
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly

ENV PATH=/root/.cargo/bin:$PATH

# Set the working directory
WORKDIR /usr/src/app

# Copy the source files to the container
COPY rust/layout .

# Install the required dependencies and build the application
RUN RUSTFLAGS='-C target-feature=+avx2' cargo build -r

# Use the official Debian image for the runtime environment
FROM debian:stable-slim

# Install the required packages
RUN apt-get update \
    && apt-get install -y libssl1.1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the build stage to the current stage
COPY --from=build /usr/src/app/target/release/layout /usr/local/bin/layout

# Expose the port for the FastAPI application
EXPOSE 8086

# Start the application
CMD ["layout"]
