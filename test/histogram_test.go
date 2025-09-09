package test

import (
	"math"
	"testing"

	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/pkg/histogram"
)

func TestPercentileCalculator(t *testing.T) {
	calculator := histogram.NewPercentileCalculator()

	tests := []struct {
		name       string
		buckets    []models.HistogramBucket
		percentile float64
		expected   float64
		tolerance  float64
	}{
		{
			name: "P50 calculation",
			buckets: []models.HistogramBucket{
				{UpperBound: 0.005, Count: 100},
				{UpperBound: 0.01, Count: 200},
				{UpperBound: 0.025, Count: 300},
				{UpperBound: 0.05, Count: 200},
				{UpperBound: 0.1, Count: 150},
				{UpperBound: 0.25, Count: 30},
				{UpperBound: 0.5, Count: 15},
				{UpperBound: 1.0, Count: 5},
				{UpperBound: math.Inf(1), Count: 0},
			},
			percentile: 50,
			expected:   0.0167,
			tolerance:  0.001,
		},
		{
			name: "P95 calculation",
			buckets: []models.HistogramBucket{
				{UpperBound: 0.005, Count: 100},
				{UpperBound: 0.01, Count: 200},
				{UpperBound: 0.025, Count: 300},
				{UpperBound: 0.05, Count: 200},
				{UpperBound: 0.1, Count: 150},
				{UpperBound: 0.25, Count: 30},
				{UpperBound: 0.5, Count: 15},
				{UpperBound: 1.0, Count: 5},
				{UpperBound: math.Inf(1), Count: 0},
			},
			percentile: 95,
			expected:   0.0933,
			tolerance:  0.01,
		},
		{
			name: "P99 calculation",
			buckets: []models.HistogramBucket{
				{UpperBound: 0.005, Count: 100},
				{UpperBound: 0.01, Count: 200},
				{UpperBound: 0.025, Count: 300},
				{UpperBound: 0.05, Count: 200},
				{UpperBound: 0.1, Count: 150},
				{UpperBound: 0.25, Count: 30},
				{UpperBound: 0.5, Count: 15},
				{UpperBound: 1.0, Count: 5},
				{UpperBound: math.Inf(1), Count: 0},
			},
			percentile: 99,
			expected:   0.4,
			tolerance:  0.1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculator.CalculatePercentile(tt.buckets, tt.percentile)
			if err != nil {
				t.Fatalf("CalculatePercentile failed: %v", err)
			}

			if math.Abs(result-tt.expected) > tt.tolerance {
				t.Errorf("Expected P%.0f to be %.4f (±%.4f), got %.4f",
					tt.percentile, tt.expected, tt.tolerance, result)
			}
		})
	}
}

func TestDeltaToCumulativeConversion(t *testing.T) {
	calculator := histogram.NewPercentileCalculator()

	deltaBuckets := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 10},
		{UpperBound: 0.01, Count: 20},
		{UpperBound: 0.025, Count: 30},
		{UpperBound: 0.05, Count: 15},
		{UpperBound: 0.1, Count: 5},
	}

	cumulative := calculator.ConvertDeltaToCumulative(deltaBuckets)

	expectedCumulative := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 10},
		{UpperBound: 0.01, Count: 30},
		{UpperBound: 0.025, Count: 60},
		{UpperBound: 0.05, Count: 75},
		{UpperBound: 0.1, Count: 80},
	}

	if len(cumulative) != len(expectedCumulative) {
		t.Fatalf("Expected %d buckets, got %d", len(expectedCumulative), len(cumulative))
	}

	for i := range cumulative {
		if cumulative[i].UpperBound != expectedCumulative[i].UpperBound {
			t.Errorf("Bucket %d: expected upper bound %.4f, got %.4f",
				i, expectedCumulative[i].UpperBound, cumulative[i].UpperBound)
		}
		if cumulative[i].Count != expectedCumulative[i].Count {
			t.Errorf("Bucket %d: expected count %d, got %d",
				i, expectedCumulative[i].Count, cumulative[i].Count)
		}
	}
}

func TestCumulativeToDeltaConversion(t *testing.T) {
	calculator := histogram.NewPercentileCalculator()

	cumulativeBuckets := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 10},
		{UpperBound: 0.01, Count: 30},
		{UpperBound: 0.025, Count: 60},
		{UpperBound: 0.05, Count: 75},
		{UpperBound: 0.1, Count: 80},
	}

	delta := calculator.ConvertCumulativeToDelta(cumulativeBuckets)

	expectedDelta := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 10},
		{UpperBound: 0.01, Count: 20},
		{UpperBound: 0.025, Count: 30},
		{UpperBound: 0.05, Count: 15},
		{UpperBound: 0.1, Count: 5},
	}

	if len(delta) != len(expectedDelta) {
		t.Fatalf("Expected %d buckets, got %d", len(expectedDelta), len(delta))
	}

	for i := range delta {
		if delta[i].UpperBound != expectedDelta[i].UpperBound {
			t.Errorf("Bucket %d: expected upper bound %.4f, got %.4f",
				i, expectedDelta[i].UpperBound, delta[i].UpperBound)
		}
		if delta[i].Count != expectedDelta[i].Count {
			t.Errorf("Bucket %d: expected count %d, got %d",
				i, expectedDelta[i].Count, delta[i].Count)
		}
	}
}

func TestMergeBuckets(t *testing.T) {
	calculator := histogram.NewPercentileCalculator()

	bucketGroups := [][]models.HistogramBucket{
		{
			{UpperBound: 0.005, Count: 10},
			{UpperBound: 0.01, Count: 20},
			{UpperBound: 0.025, Count: 30},
		},
		{
			{UpperBound: 0.005, Count: 5},
			{UpperBound: 0.01, Count: 10},
			{UpperBound: 0.025, Count: 15},
			{UpperBound: 0.05, Count: 20},
		},
		{
			{UpperBound: 0.01, Count: 5},
			{UpperBound: 0.025, Count: 10},
			{UpperBound: 0.05, Count: 15},
			{UpperBound: 0.1, Count: 20},
		},
	}

	merged := calculator.MergeBuckets(bucketGroups)

	expectedMerged := map[float64]uint64{
		0.005: 15,
		0.01:  35,
		0.025: 55,
		0.05:  35,
		0.1:   20,
	}

	if len(merged) != len(expectedMerged) {
		t.Fatalf("Expected %d buckets, got %d", len(expectedMerged), len(merged))
	}

	for _, bucket := range merged {
		expectedCount, exists := expectedMerged[bucket.UpperBound]
		if !exists {
			t.Errorf("Unexpected bucket with upper bound %.4f", bucket.UpperBound)
			continue
		}
		if bucket.Count != expectedCount {
			t.Errorf("Bucket %.4f: expected count %d, got %d",
				bucket.UpperBound, expectedCount, bucket.Count)
		}
	}
}

func TestExponentialHistogramPercentile(t *testing.T) {
	calculator := histogram.NewExponentialHistogramCalculator()

	positiveBuckets := []models.ExponentialHistogramBucket{
		{Index: -2, Count: 10},
		{Index: -1, Count: 20},
		{Index: 0, Count: 100},
		{Index: 1, Count: 50},
		{Index: 2, Count: 15},
		{Index: 3, Count: 5},
	}

	negativeBuckets := []models.ExponentialHistogramBucket{}

	tests := []struct {
		name       string
		percentile float64
		scale      int32
		zeroCount  uint64
	}{
		{
			name:       "P50 exponential histogram",
			percentile: 50,
			scale:      0,
			zeroCount:  0,
		},
		{
			name:       "P95 exponential histogram",
			percentile: 95,
			scale:      0,
			zeroCount:  0,
		},
		{
			name:       "P99 exponential histogram with zero count",
			percentile: 99,
			scale:      0,
			zeroCount:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculator.CalculatePercentile(
				tt.scale,
				tt.zeroCount,
				0.001,
				positiveBuckets,
				negativeBuckets,
				tt.percentile,
			)
			if err != nil {
				t.Fatalf("CalculatePercentile failed: %v", err)
			}

			if result < 0 {
				t.Errorf("Expected positive result for P%.0f, got %.4f",
					tt.percentile, result)
			}
		})
	}
}