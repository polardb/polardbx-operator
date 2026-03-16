# Default target executed when no arguments are given to make.
all: build

# --- Variables ---
# Binary name
BINARY_NAME=polardbx-dashboard-backend
# Feature Flags (override via `make run MONITORING_V2_ENABLED=false`)
MONITORING_V2_ENABLED ?= true
# Docker image settings
IMG ?= polardbx/polardbx-dashboard-backend:latest
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_VERSION ?= $(shell go env GOVERSION 2>/dev/null || go version | awk '{print $$3}')
LDFLAGS := -w -s \
	-X 'polardbx-dashboard-backend/pkg/api/router.Version=$(VERSION)' \
	-X 'polardbx-dashboard-backend/pkg/api/router.Commit=$(COMMIT)' \
	-X 'polardbx-dashboard-backend/pkg/api/router.BuildDate=$(BUILD_DATE)' \
	-X 'polardbx-dashboard-backend/pkg/api/router.GoVersion=$(GO_VERSION)'

# --- Build Targets ---

# Build the Go application for the current OS/architecture
build:
	@echo "Building binary..."
	go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) ./main.go

# Run the Go application locally
run:
	@echo "Starting application..."
	MONITORING_V2_ENABLED=$(MONITORING_V2_ENABLED) go run -ldflags "$(LDFLAGS)" ./main.go

# Build the Docker image
docker-build:
	@echo "Building Docker image: $(IMG)..."
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg GO_VERSION=$(GO_VERSION) \
		-t $(IMG) \
		-f Dockerfile \
		../../..

# --- Cleanup ---

# Clean up build artifacts
clean:
	@echo "Cleaning up..."
	rm -f $(BINARY_NAME)

# --- Testing ---

# Run unit tests
test:
	@echo "Running unit tests..."
	go test ./... -v

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test ./... -coverprofile=coverage.out -v
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	go test ./... -tags=integration -v

# Run benchmark tests
test-benchmark:
	@echo "Running benchmark tests..."
	go test ./... -bench=. -benchmem

# Run race condition tests
test-race:
	@echo "Running race condition tests..."
	go test ./... -race -v

# Run all tests (unit, integration, benchmark)
test-all: test test-integration test-benchmark

# --- Tooling ---

# Tidy up Go module files
tidy:
	@echo "Tidying go module files..."
	go mod tidy

# Check legacy error-handling patterns for the unified error model (advisory)
check-error-model:
	@echo "Checking for legacy error-handling patterns (error model)..."
	@# 1) Service methods still directly bound to *gin.Context (excluding tests)
	@rg "func \\(s \\*.*Service\\) .*\\(c \\*gin.Context" pkg/api/domain --glob '!*_test.go' || true
	@# 2) Direct util.HandleK8sError usage
	@rg "util\\.HandleK8sError\\(" pkg || true
	@# 3) Direct AbortInternal / AbortNotFound / AbortValidation (definitions excluded)
	@rg "AbortInternal\\(" pkg --glob '!errors/handler.go' || true
	@rg "AbortNotFound\\(" pkg --glob '!errors/handler.go' || true
	@rg "AbortValidation\\(" pkg --glob '!errors/handler.go' || true
	@# 4) Remaining TODO(error-model) anchors
	@rg "TODO\\(error-model\\)" . || true
	@echo "NOTE: check-error-model is advisory for now. CI can later enforce zero matches based on ERROR_MODEL_GUIDE.md."

# Strict check for unified error model (intended for CI once migration reaches zero-legacy)
check-error-model-strict:
	@echo "Running STRICT error-model checks (CI mode)..."
	@echo "1) util.HandleK8sError must not be used..."
	@rg "util\\.HandleK8sError\\(" pkg && echo "ERROR: util.HandleK8sError found" && exit 1 || true
	@echo "2) Service methods must not bind *gin.Context..."
	@rg "func \\(s \\*.*Service\\) .*\\(c \\*gin.Context" pkg/api/domain --glob '!*_test.go' && echo "ERROR: Service methods still bind gin.Context" && exit 1 || true
	@echo "3) AbortInternal/AbortNotFound usage limited to allowed locations (domain layers only)..."
	@rg "AbortInternal\(" pkg/api/domain/polardbxclusters pkg/api/domain/xstores --glob '!errors/handler.go' && echo "ERROR: AbortInternal found in domain layer" && exit 1 || true
	@rg "AbortNotFound\(" pkg/api/domain/polardbxclusters pkg/api/domain/xstores --glob '!errors/handler.go' && echo "ERROR: AbortNotFound found in domain layer" && exit 1 || true
	@echo "4) TODO(error-model) anchors should be cleared for zero-legacy..."
	@rg "TODO\\(error-model\\)" pkg && echo "ERROR: TODO(error-model) anchors remaining" && exit 1 || true
	@echo "STRICT error-model checks passed."

# Format code
format:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run ./...

# Vet code for suspicious constructs
vet:
	@echo "Vetting code..."
	go vet ./...

# Check for security vulnerabilities
security:
	@echo "Checking for security vulnerabilities..."
	gosec ./...

# Run all code quality checks
quality: format vet lint security

# --- Code Generation ---

SWAG ?= $(shell go env GOPATH)/bin/swag

generate-monitoring-types:
	@echo "Generating monitoring v2 API models..."
	cd pkg/api/domain/monitoring/spec && go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen --config server.cfg.yaml installation-openapi.yaml

# Generate Swagger documentation
generate-swagger:
	@echo "Generating Swagger documentation..."
	@test -x "$(SWAG)" || (echo "swag not found, installing to $(shell dirname $(SWAG))..." && GOBIN="$(shell dirname $(SWAG))" go install github.com/swaggo/swag/cmd/swag@latest)
	"$(SWAG)" init -g main.go -o ./docs

.PHONY: all build run docker-build clean tidy test test-coverage test-integration test-benchmark test-race test-all format lint vet security quality generate-monitoring-types generate-swagger check-error-model check-error-model-strict 
