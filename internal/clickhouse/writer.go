package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cespare/xxhash/v2"
	"github.com/kloudmate/metrics-pipeline/internal/models"
	"go.uber.org/zap"
)

type Writer struct {
	conn          driver.Conn
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration
	
	mu      sync.Mutex
	batch   []*models.Metric
	lastFlush time.Time
	
	stopCh  chan struct{}
	doneCh  chan struct{}
}

type Config struct {
	Addresses     []string
	Database      string
	Username      string
	Password      string
	BatchSize     int
	FlushInterval time.Duration
	MaxIdleConns  int
	MaxOpenConns  int
}

func NewWriter(cfg *Config, logger *zap.Logger) (*Writer, error) {
	options := &clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 10,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	w := &Writer{
		conn:          conn,
		logger:        logger,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		batch:         make([]*models.Metric, 0, cfg.BatchSize),
		lastFlush:     time.Now(),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	go w.periodicFlush()

	return w, nil
}

func (w *Writer) Write(ctx context.Context, metrics []*models.Metric) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, metric := range metrics {
		metric.SeriesHash = w.calculateSeriesHash(metric)
		w.batch = append(w.batch, metric)
		
		if len(w.batch) >= w.batchSize {
			if err := w.flushLocked(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *Writer) calculateSeriesHash(metric *models.Metric) uint64 {
	h := xxhash.New()
	h.WriteString(metric.MetricName)
	h.WriteString(metric.WorkspaceID)
	
	for k, v := range metric.Attributes {
		h.WriteString(k)
		h.WriteString(v)
	}
	
	return h.Sum64()
}

func (w *Writer) periodicFlush() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	defer close(w.doneCh)

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if time.Since(w.lastFlush) >= w.flushInterval && len(w.batch) > 0 {
				if err := w.flushLocked(context.Background()); err != nil {
					w.logger.Error("periodic flush failed", zap.Error(err))
				}
			}
			w.mu.Unlock()

		case <-w.stopCh:
			w.mu.Lock()
			if len(w.batch) > 0 {
				if err := w.flushLocked(context.Background()); err != nil {
					w.logger.Error("final flush failed", zap.Error(err))
				}
			}
			w.mu.Unlock()
			return
		}
	}
}

func (w *Writer) flushLocked(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}

	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO metrics_raw (
		workspaceId,
		series_hash,
		metric,
		serviceName,
		timestamp,
		metric_type,
		temporality,
		is_monotonic,
		value,
		count,
		sum,
		buckets.le,
		buckets.count,
		exp_scale,
		exp_zero_count,
		exp_zero_threshold,
		exp_positive_buckets.index,
		exp_positive_buckets.count,
		exp_negative_buckets.index,
		exp_negative_buckets.count,
		attributes,
		exemplars.spanId,
		exemplars.traceId,
		exemplars.value,
		exemplars.timestamp,
		exemplars.attributes
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, metric := range w.batch {
		bucketLes := make([]float64, 0, len(metric.Buckets))
		bucketCounts := make([]uint64, 0, len(metric.Buckets))
		for _, b := range metric.Buckets {
			bucketLes = append(bucketLes, b.UpperBound)
			bucketCounts = append(bucketCounts, b.Count)
		}

		exemplarSpanIds := make([]string, 0, len(metric.Exemplars))
		exemplarTraceIds := make([]string, 0, len(metric.Exemplars))
		exemplarValues := make([]float64, 0, len(metric.Exemplars))
		exemplarTimestamps := make([]time.Time, 0, len(metric.Exemplars))
		exemplarAttributes := make([]map[string]string, 0, len(metric.Exemplars))
		
		for _, e := range metric.Exemplars {
			exemplarSpanIds = append(exemplarSpanIds, e.SpanID)
			exemplarTraceIds = append(exemplarTraceIds, e.TraceID)
			exemplarValues = append(exemplarValues, e.Value)
			exemplarTimestamps = append(exemplarTimestamps, e.Timestamp)
			exemplarAttributes = append(exemplarAttributes, e.Attributes)
		}

		var expScale *int32
		var expZeroCount *uint64
		var expZeroThreshold *float64
		expPosIndexes := []int32{}
		expPosCounts := []uint64{}
		expNegIndexes := []int32{}
		expNegCounts := []uint64{}

		err := batch.Append(
			metric.WorkspaceID,
			metric.SeriesHash,
			metric.MetricName,
			metric.ServiceName,
			metric.Timestamp,
			int8(metric.Type),
			int8(metric.Temporality),
			boolToUint8(metric.IsMonotonic),
			metric.Value,
			metric.Count,
			metric.Sum,
			bucketLes,
			bucketCounts,
			expScale,
			expZeroCount,
			expZeroThreshold,
			expPosIndexes,
			expPosCounts,
			expNegIndexes,
			expNegCounts,
			metric.Attributes,
			exemplarSpanIds,
			exemplarTraceIds,
			exemplarValues,
			exemplarTimestamps,
			exemplarAttributes,
		)
		if err != nil {
			return fmt.Errorf("failed to append metric to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	w.logger.Info("flushed metrics batch", 
		zap.Int("batch_size", len(w.batch)),
		zap.Time("timestamp", time.Now()))

	w.batch = w.batch[:0]
	w.lastFlush = time.Now()
	return nil
}

func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

func (w *Writer) Close() error {
	close(w.stopCh)
	<-w.doneCh
	return w.conn.Close()
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

type Reader struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewReader(cfg *Config, logger *zap.Logger) (*Reader, error) {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s",
		cfg.Username,
		cfg.Password,
		cfg.Addresses[0],
		cfg.Database,
	)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	return &Reader{
		db:     db,
		logger: logger,
	}, nil
}

func (r *Reader) QueryMetrics(ctx context.Context, query string, args ...interface{}) ([]*models.Metric, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var metrics []*models.Metric
	for rows.Next() {
		metric := &models.Metric{}
		
		err := rows.Scan(
			&metric.WorkspaceID,
			&metric.SeriesHash,
			&metric.MetricName,
			&metric.ServiceName,
			&metric.Timestamp,
			&metric.Type,
			&metric.Temporality,
			&metric.IsMonotonic,
			&metric.Value,
			&metric.Count,
			&metric.Sum,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return metrics, nil
}

func (r *Reader) Close() error {
	return r.db.Close()
}