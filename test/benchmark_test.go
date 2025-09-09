package test

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/kloudmate/metrics-pipeline/internal/converter"
	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/pkg/histogram"
)

func BenchmarkTemporalityConverter(b *testing.B) {
	converter := converter.NewTemporalityConverter()
	
	// Create test metric
	metric := &models.Metric{
		SeriesHash:  1,
		MetricName:  "benchmark_counter",
		Type:        models.MetricTypeSum,
		Temporality: models.TemporalityCumulative,
		IsMonotonic: true,
		Value:       floatPtr(100),
		Timestamp:   time.Now(),
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate incrementing counter
		*metric.Value += float64(i)
		metric.Timestamp = metric.Timestamp.Add(time.Second)
		
		_, err := converter.ConvertToDelta(metric)
		if err != nil {
			b.Fatalf("Conversion failed: %v", err)
		}
	}
}

func BenchmarkHistogramPercentileCalculation(b *testing.B) {
	calculator := histogram.NewPercentileCalculator()
	
	// Create realistic histogram buckets
	buckets := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 100},
		{UpperBound: 0.01, Count: 250},
		{UpperBound: 0.025, Count: 500},
		{UpperBound: 0.05, Count: 750},
		{UpperBound: 0.1, Count: 900},
		{UpperBound: 0.25, Count: 950},
		{UpperBound: 0.5, Count: 980},
		{UpperBound: 1.0, Count: 995},
		{UpperBound: 2.5, Count: 999},
		{UpperBound: 5.0, Count: 1000},
		{UpperBound: math.Inf(1), Count: 1000},
	}

	percentiles := []float64{50, 90, 95, 99, 99.9}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		for _, p := range percentiles {
			_, err := calculator.CalculatePercentile(buckets, p)
			if err != nil {
				b.Fatalf("Percentile calculation failed: %v", err)
			}
		}
	}
}

func BenchmarkHistogramBucketMerging(b *testing.B) {
	calculator := histogram.NewPercentileCalculator()
	rand.Seed(42)

	// Generate multiple histogram bucket groups to merge
	bucketGroups := make([][]models.HistogramBucket, 10)
	bounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, math.Inf(1)}
	
	for i := range bucketGroups {
		buckets := make([]models.HistogramBucket, len(bounds))
		for j, bound := range bounds {
			buckets[j] = models.HistogramBucket{
				UpperBound: bound,
				Count:      uint64(rand.Intn(100) + 1),
			}
		}
		bucketGroups[i] = buckets
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = calculator.MergeBuckets(bucketGroups)
	}
}

func BenchmarkResetDetection(b *testing.B) {
	detector := converter.NewResetDetector()
	
	baseMetric := models.Metric{
		SeriesHash:  123,
		MetricName:  "benchmark_counter",
		Type:        models.MetricTypeSum,
		IsMonotonic: true,
		Timestamp:   time.Now(),
	}

	b.ResetTimer()
	
	currentValue := 0.0
	for i := 0; i < b.N; i++ {
		// Simulate occasional resets (every 1000 iterations)
		if i%1000 == 0 && i > 0 {
			currentValue = 0.0 // Reset
		} else {
			currentValue += rand.Float64() * 10 // Normal increment
		}
		
		metric := baseMetric
		metric.Value = &currentValue
		metric.Timestamp = metric.Timestamp.Add(time.Millisecond)
		
		detector.CheckReset(metric.SeriesHash, &metric)
	}
}

func BenchmarkSeriesHashCalculation(b *testing.B) {
	// This simulates what happens in the writer for series hash calculation
	metrics := make([]*models.Metric, 1000)
	
	// Generate diverse metrics
	services := []string{"api", "web", "worker", "db", "cache"}
	endpoints := []string{"/users", "/orders", "/products", "/health", "/metrics"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	
	for i := range metrics {
		metrics[i] = &models.Metric{
			MetricName:  "http_requests",
			WorkspaceID: "benchmark",
			Attributes: map[string]string{
				"service":  services[i%len(services)],
				"endpoint": endpoints[i%len(endpoints)],
				"method":   methods[i%len(methods)],
				"status":   "200",
			},
		}
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		metric := metrics[i%len(metrics)]
		// This would be done in the writer
		_ = calculateSeriesHash(metric)
	}
}

func BenchmarkLargeHistogramPercentiles(b *testing.B) {
	calculator := histogram.NewPercentileCalculator()
	
	// Create a large histogram with 1000 buckets (simulating high cardinality)
	buckets := make([]models.HistogramBucket, 1000)
	for i := range buckets {
		buckets[i] = models.HistogramBucket{
			UpperBound: math.Pow(2, float64(i)/100.0), // Exponential bounds
			Count:      uint64(rand.Intn(1000)),
		}
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := calculator.CalculatePercentile(buckets, 95)
		if err != nil {
			b.Fatalf("Large histogram percentile calculation failed: %v", err)
		}
	}
}

func BenchmarkExponentialHistogramPercentiles(b *testing.B) {
	calculator := histogram.NewExponentialHistogramCalculator()
	
	// Create exponential histogram buckets
	positiveBuckets := []models.ExponentialHistogramBucket{
		{Index: -5, Count: 10},
		{Index: -4, Count: 25},
		{Index: -3, Count: 100},
		{Index: -2, Count: 250},
		{Index: -1, Count: 500},
		{Index: 0, Count: 1000},
		{Index: 1, Count: 750},
		{Index: 2, Count: 500},
		{Index: 3, Count: 200},
		{Index: 4, Count: 50},
		{Index: 5, Count: 10},
	}

	negativeBuckets := []models.ExponentialHistogramBucket{
		{Index: -2, Count: 5},
		{Index: -1, Count: 10},
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := calculator.CalculatePercentile(
			0,    // scale
			100,  // zeroCount
			0.001, // zeroThreshold
			positiveBuckets,
			negativeBuckets,
			95, // percentile
		)
		if err != nil {
			b.Fatalf("Exponential histogram percentile calculation failed: %v", err)
		}
	}
}

func BenchmarkConcurrentTemporalityConversion(b *testing.B) {
	converter := converter.NewTemporalityConverter()
	
	// Create metrics for different series
	metrics := make([]*models.Metric, 100)
	for i := range metrics {
		metrics[i] = &models.Metric{
			SeriesHash:  uint64(i),
			MetricName:  "concurrent_counter",
			Type:        models.MetricTypeSum,
			Temporality: models.TemporalityCumulative,
			IsMonotonic: true,
			Value:       floatPtr(float64(i * 100)),
			Timestamp:   time.Now(),
		}
	}

	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			metric := metrics[i%len(metrics)]
			// Simulate incrementing counter
			*metric.Value += float64(i)
			metric.Timestamp = metric.Timestamp.Add(time.Millisecond)
			
			_, err := converter.ConvertToDelta(metric)
			if err != nil {
				b.Fatalf("Concurrent conversion failed: %v", err)
			}
			i++
		}
	})
}

// Memory allocation benchmark
func BenchmarkMetricAllocation(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		metric := &models.Metric{
			MetricName:  "allocation_test",
			ServiceName: "benchmark-service",
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
				"endpoint": "/api/test",
				"method":   "GET",
				"status":   "200",
			},
		}
		
		// Prevent compiler optimization
		_ = metric
	}
}

// Helper function to simulate series hash calculation
func calculateSeriesHash(metric *models.Metric) uint64 {
	// This would use xxhash in real implementation
	hash := uint64(0)
	for _, r := range metric.MetricName {
		hash = hash*31 + uint64(r)
	}
	for k, v := range metric.Attributes {
		for _, r := range k {
			hash = hash*31 + uint64(r)
		}
		for _, r := range v {
			hash = hash*31 + uint64(r)
		}
	}
	return hash
}