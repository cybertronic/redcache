.PHONY: help build test docker-build docker-push deploy clean

# Variables
VERSION ?= v1.2.0-stable
REGISTRY ?= ghcr.io/cybertronic
IMAGE_NAME = redcache
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(VERSION)

# Go parameters
GOCMD = go
GOBUILD = $(GOCMD) build
GOTEST = $(GOCMD) test
GOMOD = $(GOCMD) mod
GOFMT = $(GOCMD) fmt
GOVET = $(GOCMD) vet

# Build parameters
BUILD_DIR = bin
MAIN_PATH = cmd/cache-agent
BINARY_NAME = redcache

# Git parameters
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME = $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Linker flags
LDFLAGS = -ldflags "-X main.version=$(VERSION) -X main.commit=$(GIT_COMMIT) -X main.buildTime=$(BUILD_TIME) -s -w"

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

fmt: ## Format Go code
	$(GOFMT) ./...

vet: ## Run go vet
	$(GOVET) ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

test: ## Run unit tests
	$(GOTEST) -v -race -coverprofile=coverage.out ./pkg/...

test-all: ## Run all tests including cluster integration
	$(GOTEST) -v -race ./pkg/... ./tests/...

test-coverage: test ## Run tests with coverage report
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench: ## Run benchmarks
	$(GOTEST) -bench=. -benchmem ./...

##@ Build

deps: ## Download dependencies
	$(GOMOD) download
	$(GOMOD) tidy

build: deps ## Build binary for current platform
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(MAIN_PATH)

build-linux: deps ## Build binary for Linux (static build)
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(MAIN_PATH)

##@ Docker

docker-build: ## Build Docker image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(FULL_IMAGE) \
		-f Dockerfile .

docker-push: ## Push Docker image to registry
	docker push $(FULL_IMAGE)

##@ Deployment

deploy: ## Deploy to Kubernetes (DaemonSet)
	kubectl apply -f deploy/kubernetes/cache-agent-daemonset.yaml

undeploy: ## Remove from Kubernetes
	kubectl delete -f deploy/kubernetes/cache-agent-daemonset.yaml

logs: ## View redcache logs in K8s
	kubectl logs -n redcache -l app=redcache -f

status: ## Check K8s deployment status
	kubectl get pods -n redcache -l app=redcache
	kubectl get daemonset -n redcache redcache-agent

cluster-test: ## Run sharded cluster test via Docker Compose
	./scripts/run-distributed-test.sh

##@ Cleanup

clean: ## Clean build artifacts
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html coverage.xml

clean-all: clean ## Clean everything

##@ Release

release: test-all build-linux docker-build ## Build and tag release
	@echo "Released version $(VERSION)"
	git tag -d $(VERSION) 2>/dev/null || true
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION) --force

.DEFAULT_GOAL := help
