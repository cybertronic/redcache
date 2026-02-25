#!/bin/bash

# Script to generate test data for S3
# Creates realistic cache workload patterns

set -e

# Configuration
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:4566}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-test}"
S3_SECRET_KEY="${S3_SECRET_KEY:-test}"
S3_BUCKET="${S3_BUCKET:-test-bucket}"
NUM_FILES="${NUM_FILES:-100}"
MAX_FILE_SIZE="${MAX_FILE_SIZE:-100}"  # MB

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Setup AWS CLI for LocalStack
export AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY
export AWS_ENDPOINT_URL=$S3_ENDPOINT
alias awslocal='aws --endpoint-url '$S3_ENDPOINT

log_info "Generating test data for S3 cache testing"
log_info "Endpoint: $S3_ENDPOINT"
log_info "Bucket: $S3_BUCKET"
log_info "Files: $NUM_FILES"
log_info "Max file size: ${MAX_FILE_SIZE}MB"
echo ""

# Create bucket if it doesn't exist
log_info "Creating S3 bucket..."
awslocal s3 mb s3://$S3_BUCKET --region $S3_REGION 2>/dev/null || true
log_success "Bucket ready"

# Create directory structure
log_info "Creating directory structure..."
awslocal s3api put-object --bucket $S3_BUCKET --key "models/" --region $S3_REGION 2>/dev/null || true
awslocal s3api put-object --bucket $S3_BUCKET --key "datasets/" --region $S3_REGION 2>/dev/null || true
awslocal s3api put-object --bucket $S3_BUCKET --key "checkpoints/" --region $S3_REGION 2>/dev/null || true
awslocal s3api put-object --bucket $S3_BUCKET --key "artifacts/" --region $S3_REGION 2>/dev/null || true
awslocal s3api put-object --bucket $S3_BUCKET --key "weights/" --region $S3_REGION 2>/dev/null || true
log_success "Directory structure created"

# Generate model files (larger files, representing ML models)
log_info "Generating model files..."
for i in $(seq 1 20); do
    size=$((RANDOM % 50 + 50))  # 50-100 MB
    key="models/model-v1.0-${i}.pth"
    
    log_info "  Creating $key (${size}MB)..."
    dd if=/dev/urandom of="/tmp/model-${i}.bin" bs=1M count=$size 2>/dev/null
    awslocal s3 cp "/tmp/model-${i}.bin" "s3://$S3_BUCKET/$key" --region $S3_REGION
    rm -f "/tmp/model-${i}.bin"
done
log_success "Model files generated"

# Generate dataset files (medium-sized files)
log_info "Generating dataset files..."
for i in $(seq 1 30); do
    size=$((RANDOM % 20 + 10))  # 10-30 MB
    key="datasets/dataset-train-${i}.tfrecord"
    
    dd if=/dev/urandom of="/tmp/dataset-${i}.bin" bs=1M count=$size 2>/dev/null
    awslocal s3 cp "/tmp/dataset-${i}.bin" "s3://$S3_BUCKET/$key" --region $S3_REGION
    rm -f "/tmp/dataset-${i}.bin"
done
log_success "Dataset files generated"

# Generate checkpoint files (smaller, frequent access)
log_info "Generating checkpoint files..."
for i in $(seq 1 50); do
    size=$((RANDOM % 5 + 1))  # 1-5 MB
    key="checkpoints/checkpoint-epoch-$((i % 10))-step-$i.ckpt"
    
    dd if=/dev/urandom of="/tmp/checkpoint-${i}.bin" bs=1M count=$size 2>/dev/null
    awslocal s3 cp "/tmp/checkpoint-${i}.bin" "s3://$S3_BUCKET/$key" --region $S3_REGION
    rm -f "/tmp/checkpoint-${i}.bin"
done
log_success "Checkpoint files generated"

# Generate weight files (various sizes)
log_info "Generating weight files..."
for i in $(seq 1 40); do
    size=$((RANDOM % 10 + 2))  # 2-12 MB
    key="weights/layer-$((i % 5))-weights-${i}.h5"
    
    dd if=/dev/urandom of="/tmp/weights-${i}.bin" bs=1M count=$size 2>/dev/null
    awslocal s3 cp "/tmp/weights-${i}.bin" "s3://$S3_BUCKET/$key" --region $S3_REGION
    rm -f "/tmp/weights-${i}.bin"
done
log_success "Weight files generated"

# Generate artifact files (small files)
log_info "Generating artifact files..."
for i in $(seq 1 60); do
    size=$((RANDOM % 2 + 1))  # 1-2 MB
    key="artifacts/artifact-$((i % 3))-${i}.tar.gz"
    
    dd if=/dev/urandom of="/tmp/artifact-${i}.bin" bs=1M count=$size 2>/dev/null
    awslocal s3 cp "/tmp/artifact-${i}.bin" "s3://$S3_BUCKET/$key" --region $S3_REGION
    rm -f "/tmp/artifact-${i}.bin"
done
log_success "Artifact files generated"

# Calculate and display statistics
log_info "Calculating statistics..."
TOTAL_SIZE=$(awslocal s3 ls s3://$S3_BUCKET --recursive --region $S3_REGION --summarize | tail -1 | awk '{print $3}')
TOTAL_FILES=$(awslocal s3 ls s3://$S3_BUCKET --recursive --region $S3_REGION --summarize | grep "Total Objects" | awk '{print $3}')

echo ""
log_success "Test data generation complete!"
echo ""
echo "📊 Statistics:"
echo "   Total files: $TOTAL_FILES"
echo "   Total size: $TOTAL_SIZE"
echo ""
echo "📁 File distribution:"
echo "   Models:       20 files (50-100 MB each)"
echo "   Datasets:     30 files (10-30 MB each)"
echo "   Checkpoints:  50 files (1-5 MB each)"
echo "   Weights:      40 files (2-12 MB each)"
echo "   Artifacts:    60 files (1-2 MB each)"
echo ""
echo "🔍 View files in S3:"
echo "   AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY AWS_ENDPOINT_URL=$S3_ENDPOINT awslocal s3 ls s3://$S3_BUCKET --recursive --region $S3_REGION"