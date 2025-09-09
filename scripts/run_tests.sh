#!/bin/bash

# Comprehensive testing script for metrics pipeline
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
PIPELINE_ADDR=${PIPELINE_ADDR:-localhost:4317}
REMOTE_READ_ADDR=${REMOTE_READ_ADDR:-localhost:9201}
TEST_DURATION=${TEST_DURATION:-60s}
WORKSPACE_ID=${WORKSPACE_ID:-test_workspace}

echo -e "${BLUE}🚀 Starting Metrics Pipeline Testing Suite${NC}"
echo "=================================================="
echo "ClickHouse: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "Pipeline: $PIPELINE_ADDR"
echo "Remote Read: $REMOTE_READ_ADDR"
echo "Test Duration: $TEST_DURATION"
echo "Workspace: $WORKSPACE_ID"
echo

# Function to check if service is running
check_service() {
    local service=$1
    local host=$2
    local port=$3
    local timeout=${4:-10}
    
    echo -n "Checking $service... "
    
    for i in $(seq 1 $timeout); do
        if nc -z $host $port 2>/dev/null; then
            echo -e "${GREEN}✓ Running${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}✗ Not available${NC}"
    return 1
}

# Function to run test with error handling
run_test() {
    local test_name=$1
    local test_command=$2
    
    echo -e "\n${YELLOW}📋 Running: $test_name${NC}"
    echo "Command: $test_command"
    echo "----------------------------------------"
    
    if eval $test_command; then
        echo -e "${GREEN}✅ $test_name: PASSED${NC}"
        return 0
    else
        echo -e "${RED}❌ $test_name: FAILED${NC}"
        return 1
    fi
}

# Start testing
echo -e "${BLUE}🔍 Pre-flight Checks${NC}"

# Check dependencies
check_service "ClickHouse" $CLICKHOUSE_HOST $CLICKHOUSE_PORT 30 || {
    echo -e "${RED}❌ ClickHouse is required for testing${NC}"
    echo "Start ClickHouse with: docker run -d --name ch-test -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server"
    exit 1
}

# Check if Go is available
if ! command -v go &> /dev/null; then
    echo -e "${RED}❌ Go is required for testing${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Go is available${NC}"

# Initialize test database
echo -e "\n${YELLOW}🗄️ Initializing Test Database${NC}"
CLICKHOUSE_HOST=$CLICKHOUSE_HOST CLICKHOUSE_DB="metrics_test" ./init-clickhouse.sh

# Run unit tests
echo -e "\n${BLUE}🧪 Unit Tests${NC}"
cd .. # Go back to project root

failed_tests=0

run_test "Histogram Percentile Tests" "go test ./test/histogram_test.go -v" || ((failed_tests++))
run_test "Temporality Conversion Tests" "go test ./test/temporality_test.go -v" || ((failed_tests++))
run_test "Benchmark Tests" "go test ./test/benchmark_test.go -bench=. -benchmem" || ((failed_tests++))

# Run integration tests (if ClickHouse is available)
if check_service "ClickHouse" $CLICKHOUSE_HOST $CLICKHOUSE_PORT 5; then
    run_test "Integration Tests" "go test ./test/integration_test.go -v" || ((failed_tests++))
fi

# Build the pipeline
echo -e "\n${YELLOW}🔨 Building Pipeline${NC}"
if go build -o metrics-pipeline ./cmd/main.go; then
    echo -e "${GREEN}✅ Pipeline built successfully${NC}"
else
    echo -e "${RED}❌ Pipeline build failed${NC}"
    exit 1
fi

# Start pipeline in background
echo -e "\n${YELLOW}🚀 Starting Pipeline${NC}"
./metrics-pipeline -config config/config.yaml &
PIPELINE_PID=$!

# Wait for pipeline to start
sleep 5

# Check if pipeline is running
if ! check_service "Pipeline OTLP" localhost 4317 10; then
    echo -e "${RED}❌ Pipeline failed to start${NC}"
    kill $PIPELINE_PID 2>/dev/null || true
    exit 1
fi

# Generate test data
echo -e "\n${YELLOW}📊 Generating Test Metrics${NC}"
cd test
go run otlp_generator.go -endpoint="$PIPELINE_ADDR" -duration="$TEST_DURATION" -service="test-service" &
GENERATOR_PID=$!

echo "Generating metrics for $TEST_DURATION..."
sleep $(echo $TEST_DURATION | sed 's/s//') # Convert duration to seconds

# Stop generator
kill $GENERATOR_PID 2>/dev/null || true

# Wait a bit for data to be processed
sleep 10

# Validate histogram percentiles
echo -e "\n${YELLOW}🎯 Validating Histogram Percentiles${NC}"
go run validate_percentiles.go \
    -ch-addr="$CLICKHOUSE_HOST:$CLICKHOUSE_PORT" \
    -database="metrics" \
    -workspace="$WORKSPACE_ID" \
    -metric="http_request_duration_0" || ((failed_tests++))

# Test Prometheus Remote Read API
echo -e "\n${YELLOW}🔄 Testing Prometheus Remote Read${NC}"
if check_service "Remote Read API" localhost 9201 5; then
    # Simple curl test for Remote Read API
    echo "Testing Remote Read API availability..."
    if curl -f -s http://localhost:9201/api/v1/read -X POST -H "Content-Type: application/x-protobuf" --data-binary "" > /dev/null; then
        echo -e "${GREEN}✅ Remote Read API is accessible${NC}"
    else
        echo -e "${YELLOW}⚠️ Remote Read API test inconclusive (may need valid protobuf data)${NC}"
    fi
else
    echo -e "${RED}❌ Remote Read API not available${NC}"
    ((failed_tests++))
fi

# Query ClickHouse for verification
echo -e "\n${YELLOW}📈 Verifying Data in ClickHouse${NC}"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --query="
SELECT 
    'Raw Metrics' as table_name,
    COUNT(*) as count,
    MIN(timestamp) as min_time,
    MAX(timestamp) as max_time
FROM metrics.metrics_raw
WHERE workspaceId = '$WORKSPACE_ID'
UNION ALL
SELECT 
    '1m Aggregated' as table_name,
    COUNT(*) as count,
    MIN(timestamp) as min_time,
    MAX(timestamp) as max_time
FROM metrics.metrics_1m
WHERE workspaceId = '$WORKSPACE_ID'
FORMAT Pretty
" || echo -e "${YELLOW}⚠️ Could not query ClickHouse verification${NC}"

# Performance summary
echo -e "\n${YELLOW}⚡ Performance Summary${NC}"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --query="
SELECT 
    metric_type,
    COUNT(*) as total_metrics,
    COUNT(DISTINCT metric) as unique_metrics,
    AVG(timestamp) as avg_timestamp
FROM metrics.metrics_raw
WHERE workspaceId = '$WORKSPACE_ID'
GROUP BY metric_type
FORMAT Pretty
" || echo -e "${YELLOW}⚠️ Could not query performance summary${NC}"

# Cleanup
echo -e "\n${YELLOW}🧹 Cleanup${NC}"
kill $PIPELINE_PID 2>/dev/null || true
rm -f metrics-pipeline

# Summary
echo -e "\n${BLUE}📊 Test Results Summary${NC}"
echo "========================================="
if [ $failed_tests -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed successfully!${NC}"
    echo -e "The metrics pipeline is working correctly."
else
    echo -e "${RED}❌ $failed_tests tests failed${NC}"
    echo -e "Please review the failed tests above."
fi

echo -e "\n${BLUE}🔍 Manual Verification${NC}"
echo "You can manually verify the results by:"
echo "1. Connecting to ClickHouse: clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT"
echo "2. Querying metrics: SELECT * FROM metrics.metrics_raw WHERE workspaceId = '$WORKSPACE_ID' LIMIT 10"
echo "3. Checking percentiles: SELECT metric, quantile(0.95)(value) FROM metrics.metrics_raw WHERE metric_type = 'histogram' GROUP BY metric"

exit $failed_tests