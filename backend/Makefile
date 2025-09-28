# Default target executed when no arguments are given to make.
all: build

# --- Variables ---
# Binary name
BINARY_NAME=polardbx-ui-backend
# Docker image settings
IMG ?= polardbx/polardbx-ui-backend:latest

# --- Build Targets ---

# Build the Go application for the current OS/architecture
build:
	@echo "Building binary..."
	go build -o $(BINARY_NAME) ./main.go

# Run the Go application locally
run:
	@echo "Starting application..."
	go run ./main.go

# Build the Docker image
docker-build:
	@echo "Building Docker image: $(IMG)..."
	docker build -t $(IMG) .

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

.PHONY: all build run docker-build clean tidy test test-coverage test-integration test-benchmark test-race test-all format lint vet security quality 