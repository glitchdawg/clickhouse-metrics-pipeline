module github.com/kloudmate/metrics-pipeline

go 1.21

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.15.0
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/prometheus/client_golang v1.18.0
	github.com/prometheus/common v0.45.0
	github.com/prometheus/prometheus v0.48.1
	go.opentelemetry.io/collector/pdata v1.0.1
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.44.0
	go.opentelemetry.io/otel/metric v1.21.0
	go.opentelemetry.io/otel/sdk/metric v1.21.0
	go.uber.org/zap v1.26.0
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0
	gopkg.in/yaml.v3 v3.0.1
)