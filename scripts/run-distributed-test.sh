#!/bin/bash
set -e

echo "🚀 Starting RedCache Distributed Cluster Test"

# Build if necessary
go build -o ../redcache ../cmd/cache-agent/main.go

# Launch 3-node cluster using docker-compose
cd "$(dirname "$0")"
docker-compose up -d

echo "⏳ Waiting for VShard bootstrapping (15s)..."
sleep 15

# Verify VShard leadership and routing
echo "🔍 Verifying Shard Topology..."
curl -s http://localhost:9090/metrics | grep redcache_shards_active

# Run performance benchmark against the cluster
echo "⚡ Running Sharded Write/Read Test..."
go test -v ../tests/performance_test.go -run TestPerformanceBenchmark

echo "✅ Distributed Test Complete"
