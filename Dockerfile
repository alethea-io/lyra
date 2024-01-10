# Builder stage
FROM --platform=linux/amd64 rust:1-buster as builder

ENV RUST_TARGET=x86_64-unknown-linux-gnu

# Install necessary build dependencies
RUN apt-get update && apt-get install -y cmake protobuf-compiler

WORKDIR /usr/src/lyra
COPY . .
RUN cargo build --release --target ${RUST_TARGET}

# Final stage
FROM debian:buster-slim

ENV RUST_TARGET=x86_64-unknown-linux-gnu

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/lyra/target/${RUST_TARGET}/release/lyra /usr/local/bin/lyra

ENTRYPOINT ["lyra", "daemon"]
