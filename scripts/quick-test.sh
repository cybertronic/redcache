#!/bin/bash

# Quick Test Script for RedCache
# One-command setup and test execution
#
# Usage: ./scripts/quick-test.sh

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Banner
echo "=========================================="
echo "  RedCache Quick Test Suite"
echo "=========================================="
echo ""

# Check prerequisites
log_info "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    log_error "Docker Compose is not installed"
    exit 1
fi

if ! command -v make &> /dev/null; then
    log_error "Make is not installed"
    exit 1
fi

log_success "Prerequisites met"

# Build binary
log_info "Building RedCache binary..."
make build-linux
if [ ! -f "./bin/redcache-linux-amd64" ]; then
    log_error "Failed to build binary"
    exit 1
fi
log_success "Binary built successfully"

# Build Docker image
log_info "Building Docker image..."
if ! docker images | grep -q "redcache.*latest"; then
    docker build -t redcache:latest .
    log_success "Docker image built"
else
    log_info "Docker image already exists, skipping build"
fi

# Create necessary directories
log_info "Creating directories..."
mkdir -p ./cache-data/node-1
mkdir -p ./cache-data/node-2
mkdir -p ./cache-data/node-3
mkdir -p ./localstack-data
mkdir -p ./prometheus-data
mkdir -p ./grafana-data
log_success "Directories created"

# Stop any running containers
log_info "Stopping any existing containers..."
docker-compose down 2>/dev/null || true

# Start services
log_info "Starting Docker Compose services..."
docker-compose up -d

# Wait for services to be ready
log_info "Waiting for services to be ready..."
sleep 5

# Check LocalStack
log_info "Checking LocalStack health..."
for i in {1..30}; do
    if curl -s http://localhost:4566/health > /dev/null 2>&1; then
        log_success "LocalStack is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        log_error "LocalStack failed to start"
        docker-compose logs localstack
        exit 1
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

# Check cache nodes
log_info "Checking cache nodes..."
for node in 1 2 3; do
    port=$((8080 + node))
    for i in {1..30}; do
        if curl -s http://localhost:$port/health > /dev/null 2>&1; then
            log_success "Node $node (port $port) is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            log_warning "Node $node (port $port) health check failed"
            docker-compose logs redcache-node-$node | tail -20
        fi
        sleep 2
    done
done

# Generate test data
log_info "Generating test data..."
./generate-test-data.sh
log_success "Test data generated"

# Run quick load test
log_info "Running quick load test (60 seconds)..."
RESULTS_DIR="./test-results/quick-test-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Simple load test
log_info "Executing 60-second load test..."
for i in {1..60}; do
    for node in 1 2 3; do
        port=$((8080 + node))
        requests=0
        for j in {1..10}; do
            if curl -s -o /dev/null http://localhost:$port/test-data/random/file-$((RANDOM % 100 + 1)).bin; then
                requests=$((requests + 1))
            fi
        done
        echo "[$i/60] Node $node: $requests requests" | tee -a "$RESULTS_DIR/load-test.log"
    done
    sleep 1
done

# Collect metrics
log_info "Collecting metrics..."
for node in 1 2 3; do
    metrics_port=$((9090 + node))
    curl -s http://localhost:$metrics_port/metrics > "$RESULTS_DIR/metrics-node-$node.txt"
done

# Generate summary
log_info "Generating summary..."
cat > "$RESULTS_DIR/summary.md" <<EOF
# RedCache Quick Test Summary

## Test Duration
60 seconds

## Nodes Tested
- Node 1 (port 8081, metrics: 9091) - 1GB cache
- Node 2 (port 8082, metrics: 9092) - 512MB cache
- Node 3 (port 8083, metrics: 9093) - 2GB cache

## Test Data
- Total S3 objects: ~200 files
- File types: Models, datasets, checkpoints, weights, artifacts
- Size range: 1MB - 100MB

## Results
Check the log files for detailed results.

## Files Generated
- \`load-test.log\` - Request statistics during test
- \`metrics-node-*.txt\` - Prometheus metrics from each node

## Access Points
- Cache nodes: http://localhost:8081, 8082, 8083
- Metrics endpoints: http://localhost:9091, 9092, 9093/metrics
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9091
- LocalStack: http://localhost:8080
EOF

# Final summary
echo ""
echo "=========================================="
log_success "Quick test completed!"
echo "=========================================="
echo ""
echo "📊 Results saved to: $RESULTS_DIR"
echo ""
echo "🔍 Access Points:"
echo "   Cache Nodes:"
echo "     - Node 1: http://localhost:8081"
echo "     - Node 2: http://localhost:8082"
echo "     - Node 3: http://localhost:8083"
echo ""
echo "   Monitoring:"
echo "     - Grafana:    http://localhost:3000 (admin/admin)"
echo "     - Prometheus: http://localhost:9091"
echo "     - LocalStack: http://localhost:8080"
echo ""
echo "📝 View results:"
echo "   cat $RESULTS_DIR/summary.md"
echo "   cat $RESULTS_DIR/load-test.log"
echo ""
echo "🧹 Cleanup:"
echo "   docker-compose down"
echo "   docker-compose down -v  # Remove volumes"
echo ""
echo "🚀 For advanced testing:"
echo "   ./run-distributed-test.sh --help"
echo ""