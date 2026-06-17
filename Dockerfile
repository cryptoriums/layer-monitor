# syntax=docker/dockerfile:1
# Dockerfile for layer-monitor standalone binary.

### Build stage
FROM golang:1.23-bookworm AS builder

WORKDIR /src/layer-monitor

COPY go.mod go.sum ./
# vendor-api is a local replace target (bridge-remote-signer/api) and must be
# present before `go mod download` so the relative replace resolves.
COPY vendor-api ./vendor-api

ENV GOTOOLCHAIN=auto

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /tmp/monitord ./cmd

### Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /tmp/monitord /usr/local/bin/monitord

EXPOSE 8888

ENTRYPOINT ["/usr/local/bin/monitord"]
