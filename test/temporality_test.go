package test

import (
	"testing"
	"time"

	"github.com/kloudmate/metrics-pipeline/internal/converter"
	"github.com/kloudmate/metrics-pipeline/internal/models"
)

func TestTemporalityConverter_ConvertToDelta(t *testing.T) {
	conv := converter.NewTemporalityConverter()
	now := time.Now()

	tests := []struct {
		name     string
		metrics  []models.Metric
		expected []float64
	}{
		{
			name: "Sum metric conversion",
			metrics: []models.Metric{
				{
					SeriesHash:  1,
					MetricName:  "test_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(100),
					Timestamp:   now,
				},
				{
					SeriesHash:  1,
					MetricName:  "test_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(150),
					Timestamp:   now.Add(time.Minute),
				},
				{
					SeriesHash:  1,
					MetricName:  "test_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(200),
					Timestamp:   now.Add(2 * time.Minute),
				},
			},
			expected: []float64{100, 50, 50},
		},
		{
			name: "Sum metric with reset",
			metrics: []models.Metric{
				{
					SeriesHash:  2,
					MetricName:  "test_counter_reset",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(100),
					Timestamp:   now,
				},
				{
					SeriesHash:  2,
					MetricName:  "test_counter_reset",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(150),
					Timestamp:   now.Add(time.Minute),
				},
				{
					SeriesHash:  2,
					MetricName:  "test_counter_reset",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(20),
					Timestamp:   now.Add(2 * time.Minute),
				},
				{
					SeriesHash:  2,
					MetricName:  "test_counter_reset",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityCumulative,
					IsMonotonic: true,
					Value:       floatPtr(50),
					Timestamp:   now.Add(3 * time.Minute),
				},
			},
			expected: []float64{100, 50, 20, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, metric := range tt.metrics {
				delta, err := conv.ConvertToDelta(&metric)
				if err != nil {
					t.Fatalf("ConvertToDelta failed for metric %d: %v", i, err)
				}

				if delta.Temporality != models.TemporalityDelta {
					t.Errorf("Expected delta temporality, got %v", delta.Temporality)
				}

				if delta.Value == nil {
					t.Fatalf("Delta value is nil for metric %d", i)
				}

				if *delta.Value != tt.expected[i] {
					t.Errorf("Metric %d: expected delta value %.2f, got %.2f",
						i, tt.expected[i], *delta.Value)
				}
			}
		})
	}
}

func TestTemporalityConverter_ConvertToCumulative(t *testing.T) {
	conv := converter.NewTemporalityConverter()
	now := time.Now()

	tests := []struct {
		name     string
		metrics  []models.Metric
		expected []float64
	}{
		{
			name: "Sum metric conversion",
			metrics: []models.Metric{
				{
					SeriesHash:  3,
					MetricName:  "test_delta_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityDelta,
					IsMonotonic: true,
					Value:       floatPtr(10),
					Timestamp:   now,
				},
				{
					SeriesHash:  3,
					MetricName:  "test_delta_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityDelta,
					IsMonotonic: true,
					Value:       floatPtr(20),
					Timestamp:   now.Add(time.Minute),
				},
				{
					SeriesHash:  3,
					MetricName:  "test_delta_counter",
					Type:        models.MetricTypeSum,
					Temporality: models.TemporalityDelta,
					IsMonotonic: true,
					Value:       floatPtr(30),
					Timestamp:   now.Add(2 * time.Minute),
				},
			},
			expected: []float64{10, 30, 60},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, metric := range tt.metrics {
				cumulative, err := conv.ConvertToCumulative(&metric)
				if err != nil {
					t.Fatalf("ConvertToCumulative failed for metric %d: %v", i, err)
				}

				if cumulative.Temporality != models.TemporalityCumulative {
					t.Errorf("Expected cumulative temporality, got %v", cumulative.Temporality)
				}

				if cumulative.Value == nil {
					t.Fatalf("Cumulative value is nil for metric %d", i)
				}

				if *cumulative.Value != tt.expected[i] {
					t.Errorf("Metric %d: expected cumulative value %.2f, got %.2f",
						i, tt.expected[i], *cumulative.Value)
				}
			}
		})
	}
}

func TestTemporalityConverter_HistogramConversion(t *testing.T) {
	conv := converter.NewTemporalityConverter()
	now := time.Now()

	cumulativeHistogram := models.Metric{
		SeriesHash:  4,
		MetricName:  "test_histogram",
		Type:        models.MetricTypeHistogram,
		Temporality: models.TemporalityCumulative,
		Count:       uint64Ptr(100),
		Sum:         floatPtr(500.0),
		Buckets: []models.HistogramBucket{
			{UpperBound: 0.005, Count: 10},
			{UpperBound: 0.01, Count: 30},
			{UpperBound: 0.025, Count: 60},
			{UpperBound: 0.05, Count: 80},
			{UpperBound: 0.1, Count: 100},
		},
		Timestamp: now,
	}

	cumulativeHistogram2 := models.Metric{
		SeriesHash:  4,
		MetricName:  "test_histogram",
		Type:        models.MetricTypeHistogram,
		Temporality: models.TemporalityCumulative,
		Count:       uint64Ptr(200),
		Sum:         floatPtr(1000.0),
		Buckets: []models.HistogramBucket{
			{UpperBound: 0.005, Count: 15},
			{UpperBound: 0.01, Count: 50},
			{UpperBound: 0.025, Count: 100},
			{UpperBound: 0.05, Count: 150},
			{UpperBound: 0.1, Count: 200},
		},
		Timestamp: now.Add(time.Minute),
	}

	delta1, err := conv.ConvertToDelta(&cumulativeHistogram)
	if err != nil {
		t.Fatalf("Failed to convert first histogram: %v", err)
	}

	if delta1.Temporality != models.TemporalityDelta {
		t.Errorf("Expected delta temporality, got %v", delta1.Temporality)
	}

	if *delta1.Count != 100 {
		t.Errorf("First delta: expected count 100, got %d", *delta1.Count)
	}

	delta2, err := conv.ConvertToDelta(&cumulativeHistogram2)
	if err != nil {
		t.Fatalf("Failed to convert second histogram: %v", err)
	}

	if *delta2.Count != 100 {
		t.Errorf("Second delta: expected count 100, got %d", *delta2.Count)
	}

	if *delta2.Sum != 500.0 {
		t.Errorf("Second delta: expected sum 500.0, got %.2f", *delta2.Sum)
	}

	expectedDeltaBuckets := []models.HistogramBucket{
		{UpperBound: 0.005, Count: 5},
		{UpperBound: 0.01, Count: 20},
		{UpperBound: 0.025, Count: 40},
		{UpperBound: 0.05, Count: 70},
		{UpperBound: 0.1, Count: 100},
	}

	for i, bucket := range delta2.Buckets {
		if bucket.Count != expectedDeltaBuckets[i].Count {
			t.Errorf("Bucket %d: expected count %d, got %d",
				i, expectedDeltaBuckets[i].Count, bucket.Count)
		}
	}
}

func TestResetDetector(t *testing.T) {
	detector := converter.NewResetDetector()
	
	tests := []struct {
		name         string
		seriesHash   uint64
		metrics      []models.Metric
		expectResets []bool
	}{
		{
			name:       "Monotonic counter reset",
			seriesHash: 10,
			metrics: []models.Metric{
				{Value: floatPtr(100), Type: models.MetricTypeSum, IsMonotonic: true},
				{Value: floatPtr(150), Type: models.MetricTypeSum, IsMonotonic: true},
				{Value: floatPtr(50), Type: models.MetricTypeSum, IsMonotonic: true},
				{Value: floatPtr(75), Type: models.MetricTypeSum, IsMonotonic: true},
			},
			expectResets: []bool{false, false, true, false},
		},
		{
			name:       "No reset in increasing values",
			seriesHash: 11,
			metrics: []models.Metric{
				{Value: floatPtr(100), Type: models.MetricTypeSum, IsMonotonic: true},
				{Value: floatPtr(200), Type: models.MetricTypeSum, IsMonotonic: true},
				{Value: floatPtr(300), Type: models.MetricTypeSum, IsMonotonic: true},
			},
			expectResets: []bool{false, false, false},
		},
		{
			name:       "Non-monotonic metric (no reset detection)",
			seriesHash: 12,
			metrics: []models.Metric{
				{Value: floatPtr(100), Type: models.MetricTypeSum, IsMonotonic: false},
				{Value: floatPtr(50), Type: models.MetricTypeSum, IsMonotonic: false},
				{Value: floatPtr(75), Type: models.MetricTypeSum, IsMonotonic: false},
			},
			expectResets: []bool{false, false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, metric := range tt.metrics {
				metric.Timestamp = time.Now().Add(time.Duration(i) * time.Minute)
				resetDetected := detector.CheckReset(tt.seriesHash, &metric)
				
				if resetDetected != tt.expectResets[i] {
					t.Errorf("Metric %d: expected reset=%v, got=%v",
						i, tt.expectResets[i], resetDetected)
				}
			}
		})
	}
}

func floatPtr(f float64) *float64 {
	return &f
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}