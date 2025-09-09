#!/bin/bash

# ClickHouse initialization script for testing
set -e

CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DB=${CLICKHOUSE_DB:-metrics}

echo "Initializing ClickHouse for metrics pipeline testing..."
echo "Host: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "Database: $CLICKHOUSE_DB"

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
for i in {1..30}; do
    if clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --query="SELECT 1" >/dev/null 2>&1; then
        echo "ClickHouse is ready!"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Create database if not exists
echo "Creating database..."
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --query="
CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB;
"

# Apply schema
echo "Applying schema..."
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB < ../internal/clickhouse/schema.sql

# Verify tables
echo "Verifying tables..."
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB --query="
SHOW TABLES;
"

# Insert sample data for testing
echo "Inserting sample test data..."
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB --query="
INSERT INTO metrics_raw (
    workspaceId,
    series_hash,
    metric,
    serviceName,
    timestamp,
    metric_type,
    temporality,
    is_monotonic,
    value,
    attributes
) VALUES
    ('test', 1001, 'test_gauge', 'test-service', now() - interval 1 minute, 'gauge', 'unspecified', 0, 42.5, {'env': 'test', 'host': 'localhost'}),
    ('test', 1002, 'test_counter', 'test-service', now() - interval 1 minute, 'sum', 'cumulative', 1, 100, {'env': 'test', 'host': 'localhost'}),
    ('test', 1002, 'test_counter', 'test-service', now(), 'sum', 'cumulative', 1, 150, {'env': 'test', 'host': 'localhost'});
"

# Create test histogram data
echo "Creating histogram test data..."
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB --query="
INSERT INTO metrics_raw (
    workspaceId,
    series_hash,
    metric,
    serviceName,
    timestamp,
    metric_type,
    temporality,
    is_monotonic,
    count,
    sum,
    buckets.le,
    buckets.count,
    attributes
) VALUES
    ('test', 2001, 'http_duration', 'api-service', now() - interval 5 minute, 'histogram', 'delta', 0, 1000, 5000.0,
     [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, inf],
     [100, 200, 300, 200, 150, 100, 80, 60, 40, 20, 10, 5],
     {'endpoint': '/api/users', 'method': 'GET', 'status': '200'});
"

echo "Test data insertion complete!"

# Run validation queries
echo "Running validation queries..."

echo "1. Checking raw metrics count:"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB --query="
SELECT 
    metric_type,
    COUNT(*) as count,
    MIN(timestamp) as min_time,
    MAX(timestamp) as max_time
FROM metrics_raw
GROUP BY metric_type
FORMAT Pretty;
"

echo "2. Checking histogram buckets:"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD --database=$CLICKHOUSE_DB --query="
SELECT 
    metric,
    arrayZip(buckets.le, buckets.count) as bucket_data
FROM metrics_raw
WHERE metric_type = 'histogram'
LIMIT 1
FORMAT Pretty;
"

echo "ClickHouse initialization complete!"