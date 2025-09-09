package histogram

import (
	"fmt"
	"math"
	"sort"

	"github.com/kloudmate/metrics-pipeline/internal/models"
)

type PercentileCalculator struct{}

func NewPercentileCalculator() *PercentileCalculator {
	return &PercentileCalculator{}
}

func (pc *PercentileCalculator) CalculatePercentile(buckets []models.HistogramBucket, percentile float64) (float64, error) {
	if percentile < 0 || percentile > 100 {
		return 0, fmt.Errorf("percentile must be between 0 and 100, got %f", percentile)
	}

	if len(buckets) == 0 {
		return 0, fmt.Errorf("no buckets provided")
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].UpperBound < buckets[j].UpperBound
	})

	var totalCount uint64
	for _, bucket := range buckets {
		totalCount += bucket.Count
	}

	if totalCount == 0 {
		return 0, fmt.Errorf("total count is zero")
	}

	targetCount := float64(totalCount) * (percentile / 100.0)
	var cumulativeCount uint64
	var previousBound float64 = 0

	for _, bucket := range buckets {
		cumulativeCount += bucket.Count

		if float64(cumulativeCount) >= targetCount {
			if bucket.Count == 0 {
				return bucket.UpperBound, nil
			}

			fraction := (targetCount - float64(cumulativeCount-bucket.Count)) / float64(bucket.Count)
			
			if math.IsInf(bucket.UpperBound, 1) {
				return previousBound, nil
			}

			return previousBound + fraction*(bucket.UpperBound-previousBound), nil
		}
		previousBound = bucket.UpperBound
	}

	if !math.IsInf(buckets[len(buckets)-1].UpperBound, 1) {
		return buckets[len(buckets)-1].UpperBound, nil
	}
	
	return previousBound, nil
}

func (pc *PercentileCalculator) CalculateMultiplePercentiles(buckets []models.HistogramBucket, percentiles []float64) (map[float64]float64, error) {
	results := make(map[float64]float64)
	
	for _, p := range percentiles {
		value, err := pc.CalculatePercentile(buckets, p)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate percentile %f: %w", p, err)
		}
		results[p] = value
	}
	
	return results, nil
}

func (pc *PercentileCalculator) MergeBuckets(bucketGroups [][]models.HistogramBucket) []models.HistogramBucket {
	mergedMap := make(map[float64]uint64)
	
	for _, buckets := range bucketGroups {
		for _, bucket := range buckets {
			mergedMap[bucket.UpperBound] += bucket.Count
		}
	}
	
	merged := make([]models.HistogramBucket, 0, len(mergedMap))
	for upperBound, count := range mergedMap {
		merged = append(merged, models.HistogramBucket{
			UpperBound: upperBound,
			Count:      count,
		})
	}
	
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].UpperBound < merged[j].UpperBound
	})
	
	return merged
}

func (pc *PercentileCalculator) ConvertDeltaToCumulative(deltaBuckets []models.HistogramBucket) []models.HistogramBucket {
	if len(deltaBuckets) == 0 {
		return deltaBuckets
	}

	sort.Slice(deltaBuckets, func(i, j int) bool {
		return deltaBuckets[i].UpperBound < deltaBuckets[j].UpperBound
	})

	cumulative := make([]models.HistogramBucket, len(deltaBuckets))
	var cumulativeCount uint64
	
	for i, bucket := range deltaBuckets {
		cumulativeCount += bucket.Count
		cumulative[i] = models.HistogramBucket{
			UpperBound: bucket.UpperBound,
			Count:      cumulativeCount,
		}
	}
	
	return cumulative
}

func (pc *PercentileCalculator) ConvertCumulativeToDelta(cumulativeBuckets []models.HistogramBucket) []models.HistogramBucket {
	if len(cumulativeBuckets) == 0 {
		return cumulativeBuckets
	}

	sort.Slice(cumulativeBuckets, func(i, j int) bool {
		return cumulativeBuckets[i].UpperBound < cumulativeBuckets[j].UpperBound
	})

	delta := make([]models.HistogramBucket, len(cumulativeBuckets))
	var previousCount uint64
	
	for i, bucket := range cumulativeBuckets {
		delta[i] = models.HistogramBucket{
			UpperBound: bucket.UpperBound,
			Count:      bucket.Count - previousCount,
		}
		previousCount = bucket.Count
	}
	
	return delta
}

type ExponentialHistogramCalculator struct{}

func NewExponentialHistogramCalculator() *ExponentialHistogramCalculator {
	return &ExponentialHistogramCalculator{}
}

func (ehc *ExponentialHistogramCalculator) CalculatePercentile(
	scale int32,
	zeroCount uint64,
	zeroThreshold float64,
	positiveBuckets []models.ExponentialHistogramBucket,
	negativeBuckets []models.ExponentialHistogramBucket,
	percentile float64,
) (float64, error) {
	if percentile < 0 || percentile > 100 {
		return 0, fmt.Errorf("percentile must be between 0 and 100")
	}

	var totalCount uint64 = zeroCount
	for _, b := range positiveBuckets {
		totalCount += b.Count
	}
	for _, b := range negativeBuckets {
		totalCount += b.Count
	}

	if totalCount == 0 {
		return 0, fmt.Errorf("total count is zero")
	}

	targetCount := float64(totalCount) * (percentile / 100.0)
	var cumulativeCount uint64

	for _, bucket := range negativeBuckets {
		cumulativeCount += bucket.Count
		if float64(cumulativeCount) >= targetCount {
			return ehc.bucketToValue(bucket.Index, scale, false), nil
		}
	}

	cumulativeCount += zeroCount
	if float64(cumulativeCount) >= targetCount {
		return 0, nil
	}

	for _, bucket := range positiveBuckets {
		cumulativeCount += bucket.Count
		if float64(cumulativeCount) >= targetCount {
			return ehc.bucketToValue(bucket.Index, scale, true), nil
		}
	}

	if len(positiveBuckets) > 0 {
		return ehc.bucketToValue(positiveBuckets[len(positiveBuckets)-1].Index, scale, true), nil
	}

	return 0, nil
}

func (ehc *ExponentialHistogramCalculator) bucketToValue(index int32, scale int32, positive bool) float64 {
	base := math.Pow(2, math.Pow(2, float64(-scale)))
	
	lowerBound := math.Pow(base, float64(index))
	upperBound := math.Pow(base, float64(index+1))
	
	value := (lowerBound + upperBound) / 2
	
	if !positive {
		value = -value
	}
	
	return value
}

func (ehc *ExponentialHistogramCalculator) MergeExponentialHistograms(
	histograms []struct {
		Scale           int32
		ZeroCount       uint64
		ZeroThreshold   float64
		PositiveBuckets []models.ExponentialHistogramBucket
		NegativeBuckets []models.ExponentialHistogramBucket
	},
) (int32, uint64, float64, []models.ExponentialHistogramBucket, []models.ExponentialHistogramBucket) {
	if len(histograms) == 0 {
		return 0, 0, 0, nil, nil
	}

	minScale := histograms[0].Scale
	for _, h := range histograms[1:] {
		if h.Scale < minScale {
			minScale = h.Scale
		}
	}

	var totalZeroCount uint64
	maxZeroThreshold := histograms[0].ZeroThreshold
	
	positiveMerged := make(map[int32]uint64)
	negativeMerged := make(map[int32]uint64)

	for _, h := range histograms {
		totalZeroCount += h.ZeroCount
		
		if h.ZeroThreshold > maxZeroThreshold {
			maxZeroThreshold = h.ZeroThreshold
		}

		scaleDiff := h.Scale - minScale
		bucketShift := int32(1 << scaleDiff)

		for _, bucket := range h.PositiveBuckets {
			newIndex := bucket.Index / bucketShift
			positiveMerged[newIndex] += bucket.Count
		}

		for _, bucket := range h.NegativeBuckets {
			newIndex := bucket.Index / bucketShift
			negativeMerged[newIndex] += bucket.Count
		}
	}

	positiveBuckets := make([]models.ExponentialHistogramBucket, 0, len(positiveMerged))
	for index, count := range positiveMerged {
		positiveBuckets = append(positiveBuckets, models.ExponentialHistogramBucket{
			Index: index,
			Count: count,
		})
	}
	sort.Slice(positiveBuckets, func(i, j int) bool {
		return positiveBuckets[i].Index < positiveBuckets[j].Index
	})

	negativeBuckets := make([]models.ExponentialHistogramBucket, 0, len(negativeMerged))
	for index, count := range negativeMerged {
		negativeBuckets = append(negativeBuckets, models.ExponentialHistogramBucket{
			Index: index,
			Count: count,
		})
	}
	sort.Slice(negativeBuckets, func(i, j int) bool {
		return negativeBuckets[i].Index < negativeBuckets[j].Index
	})

	return minScale, totalZeroCount, maxZeroThreshold, positiveBuckets, negativeBuckets
}