# Metrics Pipeline Makefile

.PHONY: help build test test-unit test-integration test-bench run clean docker-up docker-down

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build
build: ## Build the metrics pipeline
	go build -o metrics-pipeline ./cmd/main.go

build-generator: ## Build the OTLP test generator
	go build -o otlp-generator ./test/otlp_generator.go

build-validator: ## Build the percentile validator
	go build -o percentile-validator ./test/validate_percentiles.go

# Testing
test: test-unit test-integration ## Run all tests

test-unit: ## Run unit tests
	@echo "Running unit tests..."
	go test ./test/histogram_test.go -v
	go test ./test/temporality_test.go -v

test-integration: ## Run integration tests (requires ClickHouse)
	@echo "Running integration tests..."
	go test ./test/integration_test.go -v

test-bench: ## Run benchmark tests
	@echo "Running benchmark tests..."
	go test ./test/benchmark_test.go -bench=. -benchmem

test-full: ## Run comprehensive test suite
	@echo "Running full test suite..."
	cd scripts && chmod +x run_tests.sh && ./run_tests.sh

# Development
run: build ## Build and run the pipeline
	./metrics-pipeline -config config/config.yaml

run-dev: ## Run pipeline in development mode with debug logging
	CONFIG_LOG_LEVEL=debug ./metrics-pipeline -config config/config.yaml

generate-metrics: build-generator ## Generate test metrics
	./otlp-generator -endpoint="localhost:4317" -duration="5m" -service="test-service"

validate-percentiles: build-validator ## Validate histogram percentile calculations
	./percentile-validator -ch-addr="localhost:9000" -database="metrics" -workspace="default"

# Docker
docker-up: ## Start ClickHouse and supporting services
	cd scripts && docker-compose up -d

docker-down: ## Stop Docker services
	cd scripts && docker-compose down

docker-logs: ## Show Docker service logs
	cd scripts && docker-compose logs -f

# Database
init-db: ## Initialize ClickHouse database schema
	cd scripts && chmod +x init-clickhouse.sh && ./init-clickhouse.sh

clean-db: ## Clean test data from ClickHouse
	clickhouse-client --query="DROP DATABASE IF EXISTS metrics_test"

# Utilities
clean: ## Clean build artifacts
	rm -f metrics-pipeline otlp-generator percentile-validator

fmt: ## Format Go code
	go fmt ./...

vet: ## Run Go vet
	go vet ./...

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	golangci-lint run

deps: ## Download Go dependencies
	go mod download
	go mod tidy

deps-update: ## Update Go dependencies
	go get -u ./...
	go mod tidy

# Monitoring
monitor-clickhouse: ## Monitor ClickHouse metrics table
	@echo "Monitoring ClickHouse metrics (Ctrl+C to stop)..."
	@while true; do \
		clickhouse-client --query="SELECT COUNT(*) as total, MAX(timestamp) as latest FROM metrics.metrics_raw" --format=Pretty; \
		sleep 5; \
		clear; \
	done

show-metrics: ## Show recent metrics in ClickHouse
	clickhouse-client --query="SELECT metric, COUNT(*) as count, MIN(timestamp), MAX(timestamp) FROM metrics.metrics_raw WHERE timestamp >= now() - interval 1 hour GROUP BY metric ORDER BY count DESC" --format=Pretty

show-histogram-percentiles: ## Show histogram percentiles from ClickHouse
	clickhouse-client --query="SELECT metric, quantile(0.5)(value) as p50, quantile(0.95)(value) as p95, quantile(0.99)(value) as p99 FROM (SELECT metric, arrayJoin(buckets.count) as value FROM metrics.metrics_raw WHERE metric_type = 'histogram') GROUP BY metric" --format=Pretty

# Performance
profile-cpu: ## Run CPU profiling
	go test ./test/benchmark_test.go -bench=. -cpuprofile=cpu.prof

profile-mem: ## Run memory profiling
	go test ./test/benchmark_test.go -bench=. -memprofile=mem.prof

# Documentation
docs: ## Generate documentation
	@echo "API documentation available at: http://localhost:6060/pkg/github.com/kloudmate/metrics-pipeline/"
	godoc -http=:6060

# All-in-one targets
setup: deps build init-db ## Setup development environment

test-e2e: docker-up init-db test-full ## Run end-to-end tests with Docker

demo: docker-up init-db build run-demo ## Run full demo
run-demo:
	@echo "Starting demo..."
	@echo "1. Starting metrics pipeline..."
	./metrics-pipeline -config config/config.yaml &
	@echo "2. Generating test data..."
	sleep 5
	./otlp-generator -endpoint="localhost:4317" -duration="30s" -service="demo-service" &
	@echo "3. Demo running... Check http://localhost:3000 (Grafana) and http://localhost:9090 (Prometheus)"
	@echo "Press Ctrl+C to stop"
	@wait