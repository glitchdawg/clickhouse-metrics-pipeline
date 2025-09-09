package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/pkg/histogram"
)

var (
	clickhouseAddr = flag.String("ch-addr", "localhost:9000", "ClickHouse address")
	database       = flag.String("database", "metrics", "Database name")
	workspaceID    = flag.String("workspace", "default", "Workspace ID")
	metricName     = flag.String("metric", "http_request_duration", "Metric name to validate")
)

type ValidationResult struct {
	Method      string
	P50         float64
	P90         float64
	P95         float64
	P99         float64
	P999        float64
	SampleCount uint64
	Error       error
}

func main() {
	flag.Parse()

	dsn := fmt.Sprintf("clickhouse://%s/%s", *clickhouseAddr, *database)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Validating histogram percentiles for metric: %s\n", *metricName)
	fmt.Printf("Workspace: %s\n", *workspaceID)
	fmt.Printf("ClickHouse: %s\n", *clickhouseAddr)
	fmt.Println()

	// Method 1: Direct ClickHouse quantile functions
	chResult := calculateClickHousePercentiles(ctx, db)
	
	// Method 2: Our histogram calculator using raw bucket data
	ourResult := calculateOurPercentiles(ctx, db)
	
	// Method 3: Mathematical calculation from raw samples (if available)
	mathResult := calculateMathematicalPercentiles(ctx, db)

	// Display results
	fmt.Println("Percentile Validation Results:")
	fmt.Println("==============================")
	fmt.Printf("%-15s %10s %10s %10s %10s %10s %10s\n", 
		"Method", "Samples", "P50", "P90", "P95", "P99", "P99.9")
	fmt.Println(strings.Repeat("-", 80))

	printResult("ClickHouse", chResult)
	printResult("Our Calc", ourResult)
	printResult("Mathematical", mathResult)

	fmt.Println()

	// Calculate and show differences
	if chResult.Error == nil && ourResult.Error == nil {
		fmt.Println("Differences (Our vs ClickHouse):")
		fmt.Printf("P50:  %.6f (%.2f%%)\n", ourResult.P50-chResult.P50, 
			percentDiff(ourResult.P50, chResult.P50))
		fmt.Printf("P90:  %.6f (%.2f%%)\n", ourResult.P90-chResult.P90,
			percentDiff(ourResult.P90, chResult.P90))
		fmt.Printf("P95:  %.6f (%.2f%%)\n", ourResult.P95-chResult.P95,
			percentDiff(ourResult.P95, chResult.P95))
		fmt.Printf("P99:  %.6f (%.2f%%)\n", ourResult.P99-chResult.P99,
			percentDiff(ourResult.P99, chResult.P99))
		fmt.Printf("P99.9: %.6f (%.2f%%)\n", ourResult.P999-chResult.P999,
			percentDiff(ourResult.P999, chResult.P999))
	}

	// Validate accuracy
	fmt.Println()
	validateAccuracy(chResult, ourResult)
}

func calculateClickHousePercentiles(ctx context.Context, db *sql.DB) ValidationResult {
	query := `
	SELECT 
		COUNT(*) as sample_count,
		quantile(0.5)(value) as p50,
		quantile(0.9)(value) as p90,
		quantile(0.95)(value) as p95,
		quantile(0.99)(value) as p99,
		quantile(0.999)(value) as p999
	FROM (
		SELECT arrayJoin(buckets.count) as value
		FROM metrics_raw
		WHERE workspaceId = ? 
			AND metric = ?
			AND metric_type = 'histogram'
			AND timestamp >= now() - interval 1 hour
	)`

	var result ValidationResult
	result.Method = "ClickHouse"

	err := db.QueryRowContext(ctx, query, *workspaceID, *metricName).Scan(
		&result.SampleCount,
		&result.P50,
		&result.P90,
		&result.P95,
		&result.P99,
		&result.P999,
	)

	if err != nil {
		result.Error = fmt.Errorf("ClickHouse query failed: %w", err)
	}

	return result
}

func calculateOurPercentiles(ctx context.Context, db *sql.DB) ValidationResult {
	var result ValidationResult
	result.Method = "Our Calculator"

	// Get histogram bucket data
	query := `
	SELECT 
		arrayZip(buckets.le, buckets.count) as bucket_data,
		count as total_count
	FROM metrics_raw
	WHERE workspaceId = ? 
		AND metric = ?
		AND metric_type = 'histogram'
		AND timestamp >= now() - interval 1 hour
	ORDER BY timestamp DESC
	LIMIT 1
	`

	rows, err := db.QueryContext(ctx, query, *workspaceID, *metricName)
	if err != nil {
		result.Error = fmt.Errorf("failed to query histogram data: %w", err)
		return result
	}
	defer rows.Close()

	if !rows.Next() {
		result.Error = fmt.Errorf("no histogram data found")
		return result
	}

	var bucketData [][]interface{}
	var totalCount uint64

	err = rows.Scan(&bucketData, &totalCount)
	if err != nil {
		result.Error = fmt.Errorf("failed to scan histogram data: %w", err)
		return result
	}

	// Convert to our bucket format
	buckets := make([]models.HistogramBucket, len(bucketData))
	for i, bucket := range bucketData {
		if len(bucket) >= 2 {
			upperBound, ok1 := bucket[0].(float64)
			count, ok2 := bucket[1].(uint64)
			if ok1 && ok2 {
				buckets[i] = models.HistogramBucket{
					UpperBound: upperBound,
					Count:      count,
				}
			}
		}
	}

	result.SampleCount = totalCount

	// Calculate percentiles using our implementation
	calculator := histogram.NewPercentileCalculator()

	percentiles := []float64{50, 90, 95, 99, 99.9}
	values, err := calculator.CalculateMultiplePercentiles(buckets, percentiles)
	if err != nil {
		result.Error = fmt.Errorf("percentile calculation failed: %w", err)
		return result
	}

	result.P50 = values[50]
	result.P90 = values[90]
	result.P95 = values[95]
	result.P99 = values[99]
	result.P999 = values[99.9]

	return result
}

func calculateMathematicalPercentiles(ctx context.Context, db *sql.DB) ValidationResult {
	var result ValidationResult
	result.Method = "Mathematical"

	// This would require raw sample data, which we might not have
	// For now, we'll simulate this using bucket midpoints weighted by counts
	query := `
	SELECT 
		arrayZip(buckets.le, buckets.count) as bucket_data,
		count as total_count
	FROM metrics_raw
	WHERE workspaceId = ? 
		AND metric = ?
		AND metric_type = 'histogram'
		AND timestamp >= now() - interval 1 hour
	ORDER BY timestamp DESC
	LIMIT 1
	`

	rows, err := db.QueryContext(ctx, query, *workspaceID, *metricName)
	if err != nil {
		result.Error = fmt.Errorf("failed to query for mathematical calculation: %w", err)
		return result
	}
	defer rows.Close()

	if !rows.Next() {
		result.Error = fmt.Errorf("no data for mathematical calculation")
		return result
	}

	var bucketData [][]interface{}
	var totalCount uint64

	err = rows.Scan(&bucketData, &totalCount)
	if err != nil {
		result.Error = fmt.Errorf("failed to scan for mathematical calculation: %w", err)
		return result
	}

	// Create sample data from histogram buckets (approximation)
	var samples []float64
	var prevBound float64 = 0

	for _, bucket := range bucketData {
		if len(bucket) >= 2 {
			upperBound, ok1 := bucket[0].(float64)
			count, ok2 := bucket[1].(uint64)
			
			if ok1 && ok2 && count > 0 {
				// Use midpoint of bucket as approximation
				var midpoint float64
				if math.IsInf(upperBound, 1) {
					midpoint = prevBound * 1.5 // Approximate for +Inf bucket
				} else {
					midpoint = (prevBound + upperBound) / 2
				}

				// Add 'count' samples at this midpoint
				for i := uint64(0); i < count; i++ {
					samples = append(samples, midpoint)
				}
				
				prevBound = upperBound
			}
		}
	}

	if len(samples) == 0 {
		result.Error = fmt.Errorf("no samples for mathematical calculation")
		return result
	}

	result.SampleCount = uint64(len(samples))

	// Sort samples for percentile calculation
	sort.Float64s(samples)

	// Calculate percentiles
	result.P50 = calculatePercentile(samples, 50)
	result.P90 = calculatePercentile(samples, 90)
	result.P95 = calculatePercentile(samples, 95)
	result.P99 = calculatePercentile(samples, 99)
	result.P999 = calculatePercentile(samples, 99.9)

	return result
}

func calculatePercentile(sortedSamples []float64, percentile float64) float64 {
	if len(sortedSamples) == 0 {
		return 0
	}

	if percentile <= 0 {
		return sortedSamples[0]
	}
	if percentile >= 100 {
		return sortedSamples[len(sortedSamples)-1]
	}

	index := (percentile / 100.0) * float64(len(sortedSamples)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		return sortedSamples[lower]
	}

	// Linear interpolation
	weight := index - float64(lower)
	return sortedSamples[lower]*(1-weight) + sortedSamples[upper]*weight
}

func printResult(method string, result ValidationResult) {
	if result.Error != nil {
		fmt.Printf("%-15s %10s %10s %10s %10s %10s %10s (ERROR: %v)\n",
			method, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", result.Error)
		return
	}

	fmt.Printf("%-15s %10d %10.6f %10.6f %10.6f %10.6f %10.6f\n",
		method, result.SampleCount, result.P50, result.P90, result.P95, result.P99, result.P999)
}

func percentDiff(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return (a - b) / b * 100
}

func validateAccuracy(chResult, ourResult ValidationResult) {
	if chResult.Error != nil || ourResult.Error != nil {
		fmt.Println("Cannot validate accuracy due to errors")
		return
	}

	tolerance := 5.0 // 5% tolerance

	percentiles := []struct {
		name string
		ch   float64
		our  float64
	}{
		{"P50", chResult.P50, ourResult.P50},
		{"P90", chResult.P90, ourResult.P90},
		{"P95", chResult.P95, ourResult.P95},
		{"P99", chResult.P99, ourResult.P99},
		{"P99.9", chResult.P999, ourResult.P999},
	}

	allPassed := true
	fmt.Println("Accuracy Validation (tolerance: ±5%):")
	fmt.Println("=====================================")

	for _, p := range percentiles {
		diff := math.Abs(percentDiff(p.our, p.ch))
		passed := diff <= tolerance
		
		status := "✓ PASS"
		if !passed {
			status = "✗ FAIL"
			allPassed = false
		}
		
		fmt.Printf("%-6s: %8.6f vs %8.6f (diff: %6.2f%%) %s\n",
			p.name, p.our, p.ch, percentDiff(p.our, p.ch), status)
	}

	fmt.Println()
	if allPassed {
		fmt.Println("🎉 All percentiles are within acceptable tolerance!")
	} else {
		fmt.Println("⚠️  Some percentiles exceed tolerance. Review histogram calculation logic.")
	}
}