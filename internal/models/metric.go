package models

import (
	"time"
)

type MetricType int8

const (
	MetricTypeUnknown MetricType = iota
	MetricTypeGauge
	MetricTypeSum
	MetricTypeHistogram
	MetricTypeSummary
	MetricTypeExponentialHistogram
)

type Temporality int8

const (
	TemporalityUnspecified Temporality = iota
	TemporalityCumulative
	TemporalityDelta
)

type Metric struct {
	WorkspaceID  string
	SeriesHash   uint64
	MetricName   string
	ServiceName  string
	Timestamp    time.Time
	Type         MetricType
	Temporality  Temporality
	IsMonotonic  bool
	Value        *float64
	Count        *uint64
	Sum          *float64
	Buckets      []HistogramBucket
	Attributes   map[string]string
	Exemplars    []Exemplar
	TTL          time.Time
}

type HistogramBucket struct {
	UpperBound float64
	Count      uint64
}

type ExponentialHistogramBucket struct {
	Index int32
	Count uint64
}

type ExponentialHistogram struct {
	Scale        int32
	ZeroCount    uint64
	ZeroThreshold float64
	PositiveBuckets []ExponentialHistogramBucket
	NegativeBuckets []ExponentialHistogramBucket
}

type Exemplar struct {
	SpanID     string
	TraceID    string
	Value      float64
	Timestamp  time.Time
	Attributes map[string]string
}

type MetricPoint struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

type ResetInfo struct {
	MetricHash    uint64
	LastValue     float64
	LastTimestamp time.Time
	ResetDetected bool
}