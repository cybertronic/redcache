#!/bin/bash

# Script to setup LocalStack for S3 testing
# Usage: ./scripts/setup-localstack.sh

set -e

CONTAINER_NAME="redcache-localstack"
IMAGE_NAME="localstack/localstack:latest"

echo "🔧 Setting up LocalStack for S3 testing..."

# Check if container exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "✅ Container ${CONTAINER_NAME} exists"
    
    # Start it if not running
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "🚀 Starting existing container..."
        docker start ${CONTAINER_NAME}
    else
        echo "✅ Container already running"
    fi
else
    echo "📦 Creating new LocalStack container..."
    docker run -d \
        --name ${CONTAINER_NAME} \
        -p 4566:4566 \
        -p 4571:4571 \
        -e SERVICES=s3 \
        -e DEBUG=1 \
        -e DATA_DIR=/tmp/localstack/data \
        -e PORT_WEB_UI=8080 \
        ${IMAGE_NAME}
fi

echo "⏳ Waiting for LocalStack to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:4566/health > /dev/null 2>&1; then
        echo "✅ LocalStack is ready!"
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

# Create test bucket
echo "🪣 Creating test bucket..."
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 mb s3://test-bucket --region us-east-1 || true

echo "✅ LocalStack setup complete!"
echo "   S3 Endpoint: http://localhost:4566"
echo "   Web UI: http://localhost:8080"
echo ""
echo "📝 Environment variables to use:"
echo "   export S3_ENDPOINT=http://localhost:4566"
echo "   export S3_REGION=us-east-1"
echo "   export S3_ACCESS_KEY_ID=test"
echo "   export S3_SECRET_ACCESS_KEY=test"
echo ""
echo "🔍 Test S3 connectivity:"
echo "   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_ENDPOINT_URL=http://localhost:4566 awslocal s3 ls s3://test-bucket --region us-east-1"