# syntax=docker/dockerfile:1.20
#
# Dockerfile
# Multi-architecture build supporting AMD64 and ARM64
# Uses QEMU emulation for cross-platform builds with CGO

# Stage 1: Build
# Note: Do NOT use --platform=$BUILDPLATFORM here
# CGO requires native compilation, QEMU will emulate the target platform
FROM golang:1.25-bookworm AS builder

WORKDIR /src

RUN --mount=type=bind,source=go.mod,target=go.mod,ro \
    --mount=type=bind,source=go.sum,target=go.sum,ro \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build the application with CGO for DuckDB
# Native build on each platform (emulated via QEMU for cross-platform)
RUN --mount=type=bind,source=.,target=/src,ro \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build \
      -trimpath \
      -buildvcs=false \
      -ldflags="-s -w" \
      -o /snowflake-emulator \
      ./cmd/server

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies and health check tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -u 10001 -g root -d /nonexistent -s /usr/sbin/nologin --no-create-home snowflake

WORKDIR /app

COPY --from=builder /snowflake-emulator .

RUN mkdir -p /data/stages \
    && chown -R snowflake:root /app /data

USER snowflake

ENV PORT=8080 \
    DB_PATH=":memory:" \
    STAGE_DIR="/data/stages"

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

ENTRYPOINT ["./snowflake-emulator"]
