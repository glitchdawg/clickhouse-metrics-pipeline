# Metrics Pipeline for KloudMate

High-performance metrics pipeline built with Go, supporting OpenTelemetry metrics ingestion, efficient ClickHouse storage, and Prometheus Remote Read API compatibility.

## Features

### Core Capabilities
- **OTLP Metrics Ingestion**: Native OpenTelemetry Protocol support via gRPC
- **Multiple Metric Types**: Gauge, Sum, Histogram, Exponential Histogram, Summary
- **Temporality Management**: Automatic conversion between cumulative and delta temporality
- **Reset Detection**: Intelligent detection and handling of counter resets
- **Histogram Percentile Calculations**: Accurate P50, P95, P99 calculations with delta support
- **Prometheus Compatibility**: Full Remote Read API implementation

### Storage & Performance
- **ClickHouse Integration**: Optimized schema with compression (ZSTD, Gorilla, T64, Delta)
- **Multi-Resolution Storage**:
  - Raw data (<60s): 3 hours retention
  - 1-minute: 15 days retention
  - 5-minute: 63 days retention
  - 1-hour: 455 days retention
- **Efficient Batching**: Configurable batch sizes and flush intervals
- **Materialized Views**: Automatic down-sampling for different time resolutions

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   OTLP       │────▶│  Processor   │────▶│  ClickHouse  │
│  Receiver    │     │  & Converter │     │   Writer     │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Temporality  │
                    │  Converter   │
                    └──────────────┘
                    
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Prometheus  │◀────│ Remote Read  │◀────│  ClickHouse  │
│              │     │   Handler    │     │   Reader     │
└──────────────┘     └──────────────┘     └──────────────┘
```

## Quick Start

### Prerequisites
- Go 1.21+
- Docker & Docker Compose
- ClickHouse server

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd metrics-pipeline
```

2. Install dependencies:
```bash
go mod download
```

3. Run tests:
```bash
go test ./test/...
```

### Docker Deployment

1. Start all services:
```bash
cd scripts
docker-compose up -d
```

2. Services will be available at:
- OTLP Receiver: `localhost:4317`
- Prometheus Remote Read: `localhost:9201`
- ClickHouse: `localhost:9000` (native), `localhost:8123` (HTTP)
- Prometheus: `localhost:9090`
- Grafana: `localhost:3000` (admin/admin)

### Manual Deployment

1. Set up ClickHouse schema:
```bash
clickhouse-client < internal/clickhouse/schema.sql
```

2. Configure the pipeline:
```yaml
# config.yaml
receiver:
  otlp:
    address: ":4317"

clickhouse:
  addresses:
    - "localhost:9000"
  database: "default"
  username: "default"
  password: ""
  batch_size: 1000
  flush_interval: 10s

processor:
  workspace_id: "default"
  convert_to_delta: true
  batch_size: 1000
  flush_interval: 10s

remote_read:
  enabled: true
  address: ":9201"
```

3. Run the pipeline:
```bash
go run cmd/main.go -config config.yaml
```

## Configuration

### Environment Variables
- `CLICKHOUSE_HOST`: ClickHouse server address
- `WORKSPACE_ID`: Workspace identifier for multi-tenancy
- `LOG_LEVEL`: Logging level (debug, info, warn, error)

### Key Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `convert_to_delta` | Convert cumulative metrics to delta | `true` |
| `batch_size` | Metrics batch size | `1000` |
| `flush_interval` | Batch flush interval | `10s` |
| `enable_exemplars` | Store exemplar data | `true` |
| `max_exemplars_per_metric` | Max exemplars per metric | `10` |

## Testing

### Unit Tests
```bash
go test ./test/... -v
```

### Integration Tests with Sample Data
```bash
# Send test metrics via OTLP
go run test/send_metrics.go

# Query via Prometheus Remote Read
curl -X POST http://localhost:9201/api/v1/read \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  --data-binary @test/read_request.pb
```

### Verify Histogram Percentiles
```sql
-- In ClickHouse
SELECT 
    metric,
    quantile(0.5)(value) as p50,
    quantile(0.95)(value) as p95,
    quantile(0.99)(value) as p99
FROM metrics_raw
WHERE metric = 'http_request_duration'
GROUP BY metric;
```

## Performance Optimization

### ClickHouse Tuning
- Adjust `index_granularity` based on query patterns
- Configure appropriate TTL for each resolution table
- Use projection queries for common aggregations

### Pipeline Tuning
- Increase batch size for higher throughput
- Adjust flush interval based on latency requirements
- Configure connection pool sizes

### Memory Management
- Use appropriate buffer sizes
- Enable compression for network transfer
- Implement backpressure handling

## Monitoring

The pipeline exposes internal metrics on port 8080:
- `metrics_processed_total`: Total processed metrics
- `metrics_dropped_total`: Dropped metrics count
- `batch_flush_duration_seconds`: Batch flush latency
- `clickhouse_write_errors_total`: Write error count
<!--
## Troubleshooting

### Common Issues

1. **High Memory Usage**
   - Reduce batch size
   - Decrease flush interval
   - Check for memory leaks with pprof

2. **ClickHouse Write Failures**
   - Verify table schema compatibility
   - Check disk space
   - Review ClickHouse logs

3. **Temporality Conversion Issues**
   - Ensure consistent series hash calculation
   - Verify reset detection logic
   - Check state store persistence
-->

<!--
## Development

### Adding New Metric Types
1. Update `models/metric.go` with new type
2. Implement conversion logic in `converter/temporality.go`
3. Add handler in `receiver/otlp.go`
4. Update ClickHouse schema if needed
-->
### Testing PromQL Compatibility
```yaml
# prometheus.yml
remote_read:
  - url: http://localhost:9201/api/v1/read
    read_recent: true
```
