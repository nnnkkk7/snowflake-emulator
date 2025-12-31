# Dockerfile
# Multi-architecture build supporting AMD64 and ARM64
# Uses QEMU emulation for cross-platform builds with CGO

# Stage 1: Build
# Note: Do NOT use --platform=$BUILDPLATFORM here
# CGO requires native compilation, QEMU will emulate the target platform
FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application with CGO for DuckDB
# Native build on each platform (emulated via QEMU for cross-platform)
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o snowflake-emulator ./cmd/server

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies and health check tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -r -u 1001 -g root snowflake

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/snowflake-emulator .

# Create directories for persistent data and stages
RUN mkdir -p /data/stages && chown -R snowflake:root /app /data

USER snowflake

# Environment variables with defaults
ENV PORT=8080
ENV DB_PATH=":memory:"
ENV STAGE_DIR="/data/stages"

# Expose default port
EXPOSE 8080

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Run the application
ENTRYPOINT ["./snowflake-emulator"]
