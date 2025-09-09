package converter

import (
	"fmt"
	"sync"
	"time"

	"github.com/kloudmate/metrics-pipeline/internal/models"
)

type TemporalityConverter struct {
	mu          sync.RWMutex
	stateStore  map[uint64]*ConversionState
	resetDetector *ResetDetector
}

type ConversionState struct {
	LastValue     float64
	LastTimestamp time.Time
	LastCount     uint64
	LastSum       float64
	LastBuckets   []models.HistogramBucket
}

type ResetDetector struct {
	mu     sync.RWMutex
	states map[uint64]*models.ResetInfo
}

func NewTemporalityConverter() *TemporalityConverter {
	return &TemporalityConverter{
		stateStore:    make(map[uint64]*ConversionState),
		resetDetector: NewResetDetector(),
	}
}

func NewResetDetector() *ResetDetector {
	return &ResetDetector{
		states: make(map[uint64]*models.ResetInfo),
	}
}

func (tc *TemporalityConverter) ConvertToDelta(metric *models.Metric) (*models.Metric, error) {
	if metric.Temporality == models.TemporalityDelta {
		return metric, nil
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	state, exists := tc.stateStore[metric.SeriesHash]
	if !exists {
		state = &ConversionState{
			LastTimestamp: metric.Timestamp,
		}
		tc.stateStore[metric.SeriesHash] = state
		
		deltaMetric := *metric
		deltaMetric.Temporality = models.TemporalityDelta
		return &deltaMetric, nil
	}

	deltaMetric := *metric
	deltaMetric.Temporality = models.TemporalityDelta

	resetDetected := tc.resetDetector.CheckReset(metric.SeriesHash, metric)

	switch metric.Type {
	case models.MetricTypeSum:
		if metric.Value != nil {
			if resetDetected {
				deltaValue := *metric.Value
				deltaMetric.Value = &deltaValue
			} else {
				deltaValue := *metric.Value - state.LastValue
				deltaMetric.Value = &deltaValue
			}
			state.LastValue = *metric.Value
		}

	case models.MetricTypeHistogram:
		if resetDetected {
			deltaMetric.Count = metric.Count
			deltaMetric.Sum = metric.Sum
			deltaMetric.Buckets = metric.Buckets
		} else {
			if metric.Count != nil && state.LastCount > 0 {
				deltaCount := *metric.Count - state.LastCount
				deltaMetric.Count = &deltaCount
			}
			if metric.Sum != nil && state.LastSum > 0 {
				deltaSum := *metric.Sum - state.LastSum
				deltaMetric.Sum = &deltaSum
			}
			
			deltaMetric.Buckets = tc.computeDeltaBuckets(metric.Buckets, state.LastBuckets)
		}
		
		if metric.Count != nil {
			state.LastCount = *metric.Count
		}
		if metric.Sum != nil {
			state.LastSum = *metric.Sum
		}
		state.LastBuckets = metric.Buckets

	case models.MetricTypeGauge:
		return metric, nil
	}

	state.LastTimestamp = metric.Timestamp
	return &deltaMetric, nil
}

func (tc *TemporalityConverter) computeDeltaBuckets(current, previous []models.HistogramBucket) []models.HistogramBucket {
	if len(previous) == 0 {
		return current
	}

	deltaBuckets := make([]models.HistogramBucket, len(current))
	prevMap := make(map[float64]uint64)
	
	for _, bucket := range previous {
		prevMap[bucket.UpperBound] = bucket.Count
	}

	for i, bucket := range current {
		prevCount, exists := prevMap[bucket.UpperBound]
		if exists {
			deltaBuckets[i] = models.HistogramBucket{
				UpperBound: bucket.UpperBound,
				Count:      bucket.Count - prevCount,
			}
		} else {
			deltaBuckets[i] = bucket
		}
	}

	return deltaBuckets
}

func (rd *ResetDetector) CheckReset(seriesHash uint64, metric *models.Metric) bool {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	state, exists := rd.states[seriesHash]
	if !exists {
		rd.states[seriesHash] = &models.ResetInfo{
			MetricHash:    seriesHash,
			LastTimestamp: metric.Timestamp,
		}
		return false
	}

	resetDetected := false

	switch metric.Type {
	case models.MetricTypeSum:
		if metric.Value != nil && metric.IsMonotonic {
			if *metric.Value < state.LastValue {
				resetDetected = true
			}
			state.LastValue = *metric.Value
		}

	case models.MetricTypeHistogram:
		if metric.Count != nil && *metric.Count < state.LastValue {
			resetDetected = true
		}
		if metric.Count != nil {
			state.LastValue = float64(*metric.Count)
		}
	}

	state.LastTimestamp = metric.Timestamp
	state.ResetDetected = resetDetected

	return resetDetected
}

func (tc *TemporalityConverter) ConvertToCumulative(metric *models.Metric) (*models.Metric, error) {
	if metric.Temporality == models.TemporalityCumulative {
		return metric, nil
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	state, exists := tc.stateStore[metric.SeriesHash]
	if !exists {
		state = &ConversionState{
			LastTimestamp: metric.Timestamp,
		}
		tc.stateStore[metric.SeriesHash] = state
		
		cumulativeMetric := *metric
		cumulativeMetric.Temporality = models.TemporalityCumulative
		return &cumulativeMetric, nil
	}

	cumulativeMetric := *metric
	cumulativeMetric.Temporality = models.TemporalityCumulative

	switch metric.Type {
	case models.MetricTypeSum:
		if metric.Value != nil {
			cumulativeValue := state.LastValue + *metric.Value
			cumulativeMetric.Value = &cumulativeValue
			state.LastValue = cumulativeValue
		}

	case models.MetricTypeHistogram:
		if metric.Count != nil {
			cumulativeCount := state.LastCount + *metric.Count
			cumulativeMetric.Count = &cumulativeCount
			state.LastCount = cumulativeCount
		}
		if metric.Sum != nil {
			cumulativeSum := state.LastSum + *metric.Sum
			cumulativeMetric.Sum = &cumulativeSum
			state.LastSum = cumulativeSum
		}
		
		cumulativeMetric.Buckets = tc.computeCumulativeBuckets(metric.Buckets, state.LastBuckets)
		state.LastBuckets = cumulativeMetric.Buckets

	case models.MetricTypeGauge:
		return metric, nil

	default:
		return nil, fmt.Errorf("unsupported metric type for conversion: %v", metric.Type)
	}

	state.LastTimestamp = metric.Timestamp
	return &cumulativeMetric, nil
}

func (tc *TemporalityConverter) computeCumulativeBuckets(delta, previousCumulative []models.HistogramBucket) []models.HistogramBucket {
	if len(previousCumulative) == 0 {
		return delta
	}

	cumulativeBuckets := make([]models.HistogramBucket, len(delta))
	prevMap := make(map[float64]uint64)
	
	for _, bucket := range previousCumulative {
		prevMap[bucket.UpperBound] = bucket.Count
	}

	for i, bucket := range delta {
		prevCount, exists := prevMap[bucket.UpperBound]
		if exists {
			cumulativeBuckets[i] = models.HistogramBucket{
				UpperBound: bucket.UpperBound,
				Count:      bucket.Count + prevCount,
			}
		} else {
			cumulativeBuckets[i] = bucket
		}
	}

	return cumulativeBuckets
}