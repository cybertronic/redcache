# Multi-stage build for minimal image size
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build arguments
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# Build binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${GIT_COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -a -installsuffix cgo \
    -o cache-agent \
    ./cmd/cache-agent

# Final stage - minimal runtime image
FROM scratch

# Copy CA certificates for HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy timezone data
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy binary
COPY --from=builder /build/cache-agent /cache-agent

# Create cache directory
VOLUME ["/cache"]

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/cache-agent", "health"]

# Run as non-root user
USER 65534:65534

# Set entrypoint
ENTRYPOINT ["/cache-agent"]
CMD ["--config", "/etc/cache-agent/config.yaml"]

# Labels
LABEL org.opencontainers.image.title="GPU Cache Agent"
LABEL org.opencontainers.image.description="Ultra-lightweight S3 caching proxy for GPU training workloads"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.source="https://github.com/your-org/gpu-cache"
LABEL org.opencontainers.image.licenses="Apache-2.0"