package test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap/zaptest"

	"github.com/kloudmate/metrics-pipeline/internal/clickhouse"
	"github.com/kloudmate/metrics-pipeline/internal/converter"
	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/internal/processor"
	"github.com/kloudmate/metrics-pipeline/pkg/histogram"
	"github.com/kloudmate/metrics-pipeline/pkg/promread"
)

func TestEndToEndPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Set up ClickHouse connection
	chConfig := &clickhouse.Config{
		Addresses:     []string{"localhost:9000"},
		Database:      "metrics_test",
		Username:      "default",
		Password:      "",
		BatchSize:     100,
		FlushInterval: 1 * time.Second,
		MaxIdleConns:  2,
		MaxOpenConns:  5,
	}

	// Create test database
	if err := createTestDatabase(chConfig); err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}

	// Initialize writer
	writer, err := clickhouse.NewWriter(chConfig, logger)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()

	// Initialize processor
	procConfig := &processor.Config{
		WorkspaceID:           "integration_test",
		ConvertToDelta:        true,
		BatchSize:             100,
		FlushInterval:         1 * time.Second,
		EnableExemplars:       true,
		MaxExemplarsPerMetric: 5,
	}

	metricProcessor := processor.NewMetricProcessor(procConfig, writer, logger)
	defer metricProcessor.Close()

	// Test different metric types
	testMetrics := []*models.Metric{
		// Gauge metric
		{
			MetricName:  "cpu_usage",
			ServiceName: "test-service",
			Type:        models.MetricTypeGauge,
			Temporality: models.TemporalityUnspecified,
			Value:       floatPtr(75.5),
			Timestamp:   time.Now(),
			Attributes: map[string]string{
				"host": "server-1",
				"env":  "test",
			},
		},
		
		// Sum metrics (cumulative sequence to test delta conversion)
		{
			MetricName:  "requests_total",
			ServiceName: "test-service",
			Type:        models.MetricTypeSum,
			Temporality: models.TemporalityCumulative,
			IsMonotonic: true,
			Value:       floatPtr(100),
			Timestamp:   time.Now(),
			Attributes: map[string]string{
				"endpoint": "/api/users",
				"method":   "GET",
			},
		},
		{
			MetricName:  "requests_total",
			ServiceName: "test-service",
			Type:        models.MetricTypeSum,
			Temporality: models.TemporalityCumulative,
			IsMonotonic: true,
			Value:       floatPtr(150),
			Timestamp:   time.Now().Add(time.Second),
			Attributes: map[string]string{
				"endpoint": "/api/users",
				"method":   "GET",
			},
		},
		
		// Histogram metric
		{
			MetricName:  "request_duration",
			ServiceName: "test-service",
			Type:        models.MetricTypeHistogram,
			Temporality: models.TemporalityDelta,
			Count:       uint64Ptr(1000),
			Sum:         floatPtr(5000.0),
			Buckets: []models.HistogramBucket{
				{UpperBound: 0.005, Count: 100},
				{UpperBound: 0.01, Count: 200},
				{UpperBound: 0.025, Count: 300},
				{UpperBound: 0.05, Count: 200},
				{UpperBound: 0.1, Count: 150},
				{UpperBound: 0.25, Count: 40},
				{UpperBound: 0.5, Count: 8},
				{UpperBound: 1.0, Count: 2},
				{UpperBound: math.Inf(1), Count: 0},
			},
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"endpoint": "/api/orders",
				"method":   "POST",
			},
			Exemplars: []models.Exemplar{
				{
					SpanID:    "abc123def456",
					TraceID:   "trace-12345",
					Value:     0.025,
					Timestamp: time.Now(),
					Attributes: map[string]string{
						"user_id": "12345",
					},
				},
			},
		},
	}

	// Process metrics
	if err := metricProcessor.Process(ctx, testMetrics); err != nil {
		t.Fatalf("Failed to process metrics: %v", err)
	}

	// Force flush to ensure data is written
	if err := metricProcessor.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush metrics: %v", err)
	}

	// Give some time for materialized views to process
	time.Sleep(2 * time.Second)

	// Verify data was written correctly
	t.Run("VerifyRawMetrics", func(t *testing.T) {
		verifyRawMetrics(t, chConfig)
	})

	t.Run("VerifyHistogramPercentiles", func(t *testing.T) {
		verifyHistogramPercentiles(t, chConfig)
	})

	t.Run("VerifyTemporalityConversion", func(t *testing.T) {
		verifyTemporalityConversion(t, chConfig)
	})

	t.Run("VerifyPrometheusRemoteRead", func(t *testing.T) {
		verifyPrometheusRemoteRead(t, chConfig)
	})
}

func createTestDatabase(cfg *clickhouse.Config) error {
	// Connect without specifying database
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	defer conn.Close()

	// Create test database
	_, err := conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.Database))
	if err != nil {
		return err
	}

	// Create tables in test database
	conn2 := clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	defer conn2.Close()

	// Create metrics_raw table (simplified version for testing)
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS metrics_raw
	(
		workspaceId String,
		series_hash UInt64,
		metric LowCardinality(String),
		serviceName LowCardinality(String),
		timestamp DateTime64(6),
		metric_type Enum8('unknown'=0, 'gauge'=1, 'sum'=2, 'histogram'=3, 'summary'=4),
		temporality Enum8('unspecified'=0, 'cumulative'=1, 'delta'=2),
		is_monotonic UInt8,
		value Nullable(Float64),
		count Nullable(UInt64),
		sum Nullable(Float64),
		buckets Nested (
			le Float64,
			count UInt64
		),
		attributes Map(String, String),
		exemplars Nested (
			spanId String,
			traceId String,
			value Float64,
			timestamp DateTime64(6),
			attributes Map(String, String)
		)
	)
	ENGINE = MergeTree
	ORDER BY (workspaceId, metric, timestamp)
	`
	
	_, err = conn2.Exec(createTableSQL)
	return err
}

func verifyRawMetrics(t *testing.T, cfg *clickhouse.Config) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	defer conn.Close()

	// Check that metrics were inserted
	var count int
	err := conn.QueryRow(`
		SELECT COUNT(*) 
		FROM metrics_raw 
		WHERE workspaceId = 'integration_test'
	`).Scan(&count)
	
	if err != nil {
		t.Fatalf("Failed to query metrics count: %v", err)
	}

	if count < 3 { // We expect at least 3 metrics (gauge + 2 sums + histogram)
		t.Errorf("Expected at least 3 metrics, got %d", count)
	}

	// Verify gauge metric
	var gaugeValue float64
	err = conn.QueryRow(`
		SELECT value 
		FROM metrics_raw 
		WHERE workspaceId = 'integration_test' AND metric = 'cpu_usage'
	`).Scan(&gaugeValue)
	
	if err != nil {
		t.Fatalf("Failed to query gauge metric: %v", err)
	}

	if gaugeValue != 75.5 {
		t.Errorf("Expected gauge value 75.5, got %f", gaugeValue)
	}

	// Verify histogram bucket data
	var bucketCount int
	err = conn.QueryRow(`
		SELECT length(buckets.le)
		FROM metrics_raw 
		WHERE workspaceId = 'integration_test' AND metric = 'request_duration'
	`).Scan(&bucketCount)
	
	if err != nil {
		t.Fatalf("Failed to query histogram buckets: %v", err)
	}

	if bucketCount != 9 { // 9 buckets including +Inf
		t.Errorf("Expected 9 histogram buckets, got %d", bucketCount)
	}
}

func verifyHistogramPercentiles(t *testing.T, cfg *clickhouse.Config) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	defer conn.Close()

	// Get histogram data
	rows, err := conn.Query(`
		SELECT buckets.le, buckets.count
		FROM metrics_raw 
		WHERE workspaceId = 'integration_test' AND metric = 'request_duration'
	`)
	if err != nil {
		t.Fatalf("Failed to query histogram data: %v", err)
	}
	defer rows.Close()

	var buckets []models.HistogramBucket
	for rows.Next() {
		var les []float64
		var counts []uint64
		
		err := rows.Scan(&les, &counts)
		if err != nil {
			t.Fatalf("Failed to scan histogram row: %v", err)
		}

		for i, le := range les {
			if i < len(counts) {
				buckets = append(buckets, models.HistogramBucket{
					UpperBound: le,
					Count:      counts[i],
				})
			}
		}
	}

	// Calculate percentiles
	calculator := histogram.NewPercentileCalculator()
	
	p50, err := calculator.CalculatePercentile(buckets, 50)
	if err != nil {
		t.Fatalf("Failed to calculate P50: %v", err)
	}

	p95, err := calculator.CalculatePercentile(buckets, 95)
	if err != nil {
		t.Fatalf("Failed to calculate P95: %v", err)
	}

	// Verify reasonable percentile values
	if p50 <= 0 || p50 > 1 {
		t.Errorf("P50 value seems unreasonable: %f", p50)
	}

	if p95 <= p50 {
		t.Errorf("P95 (%f) should be >= P50 (%f)", p95, p50)
	}

	t.Logf("Calculated percentiles: P50=%.4f, P95=%.4f", p50, p95)
}

func verifyTemporalityConversion(t *testing.T, cfg *clickhouse.Config) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	defer conn.Close()

	// Check that cumulative metrics were converted to delta
	rows, err := conn.Query(`
		SELECT value, temporality
		FROM metrics_raw 
		WHERE workspaceId = 'integration_test' 
			AND metric = 'requests_total'
		ORDER BY timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query sum metrics: %v", err)
	}
	defer rows.Close()

	var values []float64
	var temporalities []string
	
	for rows.Next() {
		var value float64
		var temporality string
		
		err := rows.Scan(&value, &temporality)
		if err != nil {
			t.Fatalf("Failed to scan sum metric row: %v", err)
		}
		
		values = append(values, value)
		temporalities = append(temporalities, temporality)
	}

	if len(values) != 2 {
		t.Fatalf("Expected 2 sum metrics, got %d", len(values))
	}

	// All should be converted to delta
	for i, temp := range temporalities {
		if temp != "delta" {
			t.Errorf("Metric %d: expected delta temporality, got %s", i, temp)
		}
	}

	// First value should be the original 100, second should be delta (50)
	if values[0] != 100 {
		t.Errorf("First delta value should be 100, got %f", values[0])
	}
	
	if values[1] != 50 {
		t.Errorf("Second delta value should be 50, got %f", values[1])
	}
}

func verifyPrometheusRemoteRead(t *testing.T, cfg *clickhouse.Config) {
	// This would test the Remote Read API, but requires setting up the HTTP server
	// For integration test, we'll just verify the reader can be created and query works
	
	reader, err := clickhouse.NewReader(&clickhouse.Config{
		Addresses:     cfg.Addresses,
		Database:      cfg.Database,
		Username:      cfg.Username,
		Password:      cfg.Password,
		MaxIdleConns:  2,
		MaxOpenConns:  5,
	}, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Simple query test
	metrics, err := reader.QueryMetrics(context.Background(), `
		SELECT workspaceId, metric, timestamp, value 
		FROM metrics_raw 
		WHERE workspaceId = ? 
		LIMIT 10
	`, "integration_test")
	
	if err != nil {
		t.Fatalf("Failed to query metrics via reader: %v", err)
	}

	if len(metrics) == 0 {
		t.Error("Expected some metrics from reader query")
	}

	t.Logf("Successfully queried %d metrics via reader", len(metrics))
}

func floatPtr(f float64) *float64 {
	return &f
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}