# Build stage
FROM rust:1.84-slim-bookworm as builder
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    cmake \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Install nightly Rust toolchain
RUN rustup toolchain install nightly && rustup default nightly

# Copy workspace files for dependency resolution (layer caching)
COPY Cargo.toml Cargo.lock ./
COPY distributed-walrus/Cargo.toml ./distributed-walrus/
COPY octopii/Cargo.toml ./octopii/

# Copy source code
COPY . .

# Build the distributed-walrus binary with optimizations
WORKDIR /app/distributed-walrus
RUN cargo +nightly -Zunstable-options build --release

# Runtime image - optimized for performance
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -r -u 1000 -m -d /app walrus && \
    mkdir -p /data && \
    chown -R walrus:walrus /app /data

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/distributed-walrus/target/release/distributed-walrus /usr/local/bin/app-node
RUN chmod +x /usr/local/bin/app-node

# Switch to non-root user
USER walrus

# Set environment variables for optimal performance
ENV RUST_LOG=info
ENV WALRUS_DATA_DIR=/data
# io_uring is enabled by default on Linux kernel >= 5.1
# Set WALRUS_DISABLE_IO_URING=1 to disable if needed

# Expose default port (adjust if your app uses different port)
EXPOSE 9091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9091/health || exit 1

ENTRYPOINT ["/usr/local/bin/app-node"]