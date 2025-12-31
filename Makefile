.PHONY: all build test test-unit test-integration test-e2e test-all test-coverage lint fmt ci clean run docker-build docker-up docker-down docker-test docker-logs

# Default target
all: build

# Build all packages
build:
	go build ./...

# Run unit tests (pkg/ only) with race detection
test:
	go test -v -race ./pkg/...

# Alias for test
test-unit: test

# Run integration tests
test-integration:
	go test -v -race ./tests/integration/...

# Run e2e tests
test-e2e:
	go test -v -race ./tests/e2e/...

# Run all tests (unit + integration + e2e)
test-all:
	go test -v -race ./...

# Run tests with coverage (80%+ threshold enforced in CI)
test-coverage:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

# Run linter
lint:
	golangci-lint run --timeout=5m

# Format code
fmt:
	gofmt -w .

# CI target: lint + all tests (used by GitHub Actions)
ci: lint test-all

# Clean build artifacts
clean:
	rm -f coverage.out
	go clean ./...

# Run the server (default port 8080, in-memory DB)
run:
	go run cmd/server/main.go

# Run with persistent DB (usage: make run-persistent DB_PATH=/path/to/file.db)
run-persistent:
	DB_PATH=$(DB_PATH) go run cmd/server/main.go

# Docker targets
docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# Run Docker integration test (builds, starts, tests, stops)
docker-test: docker-build
	docker compose up -d
	@echo "Waiting for emulator to be ready..."
	@for i in $$(seq 1 30); do \
		if curl -s http://localhost:8080/health > /dev/null 2>&1; then \
			echo "Emulator is ready"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 1; \
	done
	go run ./example/docker
	docker compose down
