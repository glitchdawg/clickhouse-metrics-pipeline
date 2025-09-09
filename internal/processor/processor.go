package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kloudmate/metrics-pipeline/internal/clickhouse"
	"github.com/kloudmate/metrics-pipeline/internal/converter"
	"github.com/kloudmate/metrics-pipeline/internal/models"
	"go.uber.org/zap"
)

type MetricProcessor struct {
	logger              *zap.Logger
	writer              *clickhouse.Writer
	temporalityConverter *converter.TemporalityConverter
	config              *Config
	
	mu              sync.RWMutex
	metricsBuffer   []*models.Metric
	lastFlush       time.Time
	processingStats *ProcessingStats
}

type Config struct {
	WorkspaceID          string
	ConvertToDelta       bool
	BatchSize            int
	FlushInterval        time.Duration
	EnableExemplars      bool
	MaxExemplarsPerMetric int
}

type ProcessingStats struct {
	ProcessedCount   uint64
	DroppedCount     uint64
	ErrorCount       uint64
	LastProcessTime  time.Time
}

func NewMetricProcessor(cfg *Config, writer *clickhouse.Writer, logger *zap.Logger) *MetricProcessor {
	return &MetricProcessor{
		logger:              logger,
		writer:              writer,
		temporalityConverter: converter.NewTemporalityConverter(),
		config:              cfg,
		metricsBuffer:       make([]*models.Metric, 0, cfg.BatchSize),
		lastFlush:           time.Now(),
		processingStats:     &ProcessingStats{},
	}
}

func (p *MetricProcessor) Process(ctx context.Context, metrics []*models.Metric) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	processedMetrics := make([]*models.Metric, 0, len(metrics))

	for _, metric := range metrics {
		metric.WorkspaceID = p.config.WorkspaceID
		
		processed, err := p.processMetric(metric)
		if err != nil {
			p.logger.Warn("Failed to process metric",
				zap.String("metric", metric.MetricName),
				zap.Error(err))
			p.processingStats.DroppedCount++
			continue
		}

		if processed != nil {
			processedMetrics = append(processedMetrics, processed)
			p.processingStats.ProcessedCount++
		}
	}

	p.metricsBuffer = append(p.metricsBuffer, processedMetrics...)

	if len(p.metricsBuffer) >= p.config.BatchSize || 
	   time.Since(p.lastFlush) >= p.config.FlushInterval {
		if err := p.flush(ctx); err != nil {
			p.processingStats.ErrorCount++
			return fmt.Errorf("failed to flush metrics: %w", err)
		}
	}

	p.processingStats.LastProcessTime = time.Now()
	return nil
}

func (p *MetricProcessor) processMetric(metric *models.Metric) (*models.Metric, error) {
	if err := p.validateMetric(metric); err != nil {
		return nil, fmt.Errorf("metric validation failed: %w", err)
	}

	metric.TTL = time.Now().Add(3 * time.Hour)

	if !p.config.EnableExemplars {
		metric.Exemplars = nil
	} else if len(metric.Exemplars) > p.config.MaxExemplarsPerMetric {
		metric.Exemplars = metric.Exemplars[:p.config.MaxExemplarsPerMetric]
	}

	if p.config.ConvertToDelta && metric.Temporality == models.TemporalityCumulative {
		switch metric.Type {
		case models.MetricTypeSum, models.MetricTypeHistogram:
			converted, err := p.temporalityConverter.ConvertToDelta(metric)
			if err != nil {
				return nil, fmt.Errorf("failed to convert to delta: %w", err)
			}
			return converted, nil
		}
	}

	return metric, nil
}

func (p *MetricProcessor) validateMetric(metric *models.Metric) error {
	if metric.MetricName == "" {
		return fmt.Errorf("metric name is empty")
	}

	if metric.Timestamp.IsZero() {
		return fmt.Errorf("metric timestamp is zero")
	}

	if metric.Timestamp.After(time.Now().Add(24 * time.Hour)) {
		return fmt.Errorf("metric timestamp is too far in the future")
	}

	if metric.Timestamp.Before(time.Now().Add(-7 * 24 * time.Hour)) {
		return fmt.Errorf("metric timestamp is too old")
	}

	switch metric.Type {
	case models.MetricTypeGauge:
		if metric.Value == nil {
			return fmt.Errorf("gauge metric missing value")
		}

	case models.MetricTypeSum:
		if metric.Value == nil {
			return fmt.Errorf("sum metric missing value")
		}

	case models.MetricTypeHistogram:
		if metric.Count == nil && metric.Sum == nil && len(metric.Buckets) == 0 {
			return fmt.Errorf("histogram metric missing data")
		}
		
		if metric.Count != nil && metric.Sum != nil {
			avgValue := *metric.Sum / float64(*metric.Count)
			if avgValue < 0 && !metric.IsMonotonic {
				p.logger.Warn("Negative average value in histogram",
					zap.String("metric", metric.MetricName),
					zap.Float64("avg", avgValue))
			}
		}

	case models.MetricTypeSummary:
		if metric.Count == nil || metric.Sum == nil {
			return fmt.Errorf("summary metric missing count or sum")
		}

	default:
		return fmt.Errorf("unknown metric type: %v", metric.Type)
	}

	return nil
}

func (p *MetricProcessor) flush(ctx context.Context) error {
	if len(p.metricsBuffer) == 0 {
		return nil
	}

	if err := p.writer.Write(ctx, p.metricsBuffer); err != nil {
		return fmt.Errorf("failed to write metrics: %w", err)
	}

	p.logger.Info("Flushed metrics",
		zap.Int("count", len(p.metricsBuffer)),
		zap.Uint64("total_processed", p.processingStats.ProcessedCount))

	p.metricsBuffer = p.metricsBuffer[:0]
	p.lastFlush = time.Now()

	return nil
}

func (p *MetricProcessor) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flush(ctx)
}

func (p *MetricProcessor) GetStats() ProcessingStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return *p.processingStats
}

func (p *MetricProcessor) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush on close: %w", err)
	}

	return nil
}