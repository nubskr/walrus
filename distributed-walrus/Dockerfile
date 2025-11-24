FROM rust:1.84-slim-bookworm as builder
WORKDIR /app

# Bring in the full workspace so path dependencies resolve.
RUN apt-get update && apt-get install -y build-essential pkg-config cmake clang && rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install nightly && rustup default nightly
COPY . .

# Build the distributed-walrus binary.
WORKDIR /app/distributed-walrus
RUN cargo +nightly -Zunstable-options build --release

# Runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl iputils-ping && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/distributed-walrus/target/release/distributed-walrus /usr/local/bin/app-node
RUN mkdir -p /data

ENTRYPOINT ["/usr/local/bin/app-node"]