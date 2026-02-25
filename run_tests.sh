#!/bin/bash

# GPU Cache System - Test Runner Script
# This script runs all tests and benchmarks with proper configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Go version
    if ! command -v go &> /dev/null; then
        print_error "Go is not installed"
        exit 1
    fi
    
    GO_VERSION=$(go version | awk '{print $3}')
    print_success "Go version: $GO_VERSION"
    
    # Check disk space
    AVAILABLE_SPACE=$(df /tmp | tail -1 | awk '{print $4}')
    if [ "$AVAILABLE_SPACE" -lt 20971520 ]; then  # 20GB in KB
        print_warning "Less than 20GB free space in /tmp"
    else
        print_success "Sufficient disk space available"
    fi
    
    # Check write permissions
    if [ ! -w /tmp ]; then
        print_error "No write permission to /tmp"
        exit 1
    fi
    print_success "Write permissions OK"
    
    echo ""
}

# Clean up old test directories
cleanup() {
    print_header "Cleaning Up Old Test Data"
    
    rm -rf /tmp/cache-bench* /tmp/cache-latency-test 2>/dev/null || true
    print_success "Cleaned up old test directories"
    
    echo ""
}

# Download dependencies
download_deps() {
    print_header "Downloading Dependencies"
    
    go mod download
    print_success "Dependencies downloaded"
    
    echo ""
}

# Run unit tests
run_unit_tests() {
    print_header "Running Unit Tests"
    
    if go test -v -race -timeout 10m ./pkg/...; then
        print_success "Unit tests passed"
    else
        print_error "Unit tests failed"
        return 1
    fi
    
    echo ""
}

# Run integration tests
run_integration_tests() {
    print_header "Running Integration Tests"
    
    if go test -v -timeout 15m ./tests/...; then
        print_success "Integration tests passed"
    else
        print_error "Integration tests failed"
        return 1
    fi
    
    echo ""
}

# Run performance benchmarks
run_benchmarks() {
    print_header "Running Performance Benchmarks"
    
    echo "Running cache hit benchmark..."
    go test -bench=BenchmarkCacheHit$ -benchmem -benchtime=5s ./test/performance/
    
    echo ""
    echo "Running parallel cache hit benchmark..."
    go test -bench=BenchmarkCacheHitParallel -benchmem -benchtime=5s ./test/performance/
    
    echo ""
    echo "Running cache put benchmark..."
    go test -bench=BenchmarkCachePut -benchmem -benchtime=3s ./test/performance/
    
    echo ""
    echo "Running different sizes benchmark..."
    go test -bench=BenchmarkDifferentSizes -benchmem ./test/performance/
    
    print_success "Benchmarks completed"
    echo ""
}

# Run latency tests
run_latency_tests() {
    print_header "Running Latency Distribution Tests"
    
    if go test -v -timeout 5m ./test/performance/ -run TestCacheLatency; then
        print_success "Latency tests passed"
    else
        print_error "Latency tests failed"
        return 1
    fi
    
    echo ""
}

# Generate coverage report
generate_coverage() {
    print_header "Generating Coverage Report"
    
    go test -coverprofile=coverage.out ./...
    go tool cover -func=coverage.out | tail -1
    
    if command -v go tool cover &> /dev/null; then
        go tool cover -html=coverage.out -o coverage.html
        print_success "Coverage report generated: coverage.html"
    fi
    
    echo ""
}

# Main execution
main() {
    echo ""
    print_header "GPU Cache System - Test Suite"
    echo ""
    
    # Parse command line arguments
    RUN_ALL=true
    RUN_UNIT=false
    RUN_INTEGRATION=false
    RUN_BENCH=false
    RUN_LATENCY=false
    RUN_COVERAGE=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --unit)
                RUN_ALL=false
                RUN_UNIT=true
                shift
                ;;
            --integration)
                RUN_ALL=false
                RUN_INTEGRATION=true
                shift
                ;;
            --bench)
                RUN_ALL=false
                RUN_BENCH=true
                shift
                ;;
            --latency)
                RUN_ALL=false
                RUN_LATENCY=true
                shift
                ;;
            --coverage)
                RUN_COVERAGE=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --unit         Run unit tests only"
                echo "  --integration  Run integration tests only"
                echo "  --bench        Run benchmarks only"
                echo "  --latency      Run latency tests only"
                echo "  --coverage     Generate coverage report"
                echo "  --help         Show this help message"
                echo ""
                echo "If no options are specified, all tests will run."
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Run checks
    check_prerequisites
    cleanup
    download_deps
    
    # Run tests based on flags
    FAILED=0
    
    if [ "$RUN_ALL" = true ] || [ "$RUN_UNIT" = true ]; then
        run_unit_tests || FAILED=1
    fi
    
    if [ "$RUN_ALL" = true ] || [ "$RUN_INTEGRATION" = true ]; then
        run_integration_tests || FAILED=1
    fi
    
    if [ "$RUN_ALL" = true ] || [ "$RUN_BENCH" = true ]; then
        run_benchmarks || FAILED=1
    fi
    
    if [ "$RUN_ALL" = true ] || [ "$RUN_LATENCY" = true ]; then
        run_latency_tests || FAILED=1
    fi
    
    if [ "$RUN_COVERAGE" = true ]; then
        generate_coverage
    fi
    
    # Final cleanup
    cleanup
    
    # Print summary
    echo ""
    print_header "Test Summary"
    
    if [ $FAILED -eq 0 ]; then
        print_success "All tests passed!"
        echo ""
        exit 0
    else
        print_error "Some tests failed"
        echo ""
        exit 1
    fi
}

# Run main function
main "$@"