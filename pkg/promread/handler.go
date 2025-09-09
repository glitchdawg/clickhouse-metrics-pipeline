package promread

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/kloudmate/metrics-pipeline/internal/converter"
	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/pkg/histogram"
)

type RemoteReadHandler struct {
	db                   *sql.DB
	logger               *zap.Logger
	converter            *converter.TemporalityConverter
	histogramCalculator  *histogram.PercentileCalculator
	workspaceID          string
}

type Config struct {
	ClickHouseAddr string
	Database       string
	Username       string
	Password       string
	WorkspaceID    string
}

func NewRemoteReadHandler(cfg *Config, logger *zap.Logger) (*RemoteReadHandler, error) {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s",
		cfg.Username,
		cfg.Password,
		cfg.ClickHouseAddr,
		cfg.Database,
	)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	return &RemoteReadHandler{
		db:                  db,
		logger:              logger,
		converter:           converter.NewTemporalityConverter(),
		histogramCalculator: histogram.NewPercentileCalculator(),
		workspaceID:         cfg.WorkspaceID,
	}, nil
}

func (h *RemoteReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("Failed to read body", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reqBuf, err := remote.DecodeReadRequest(compressed)
	if err != nil {
		h.logger.Error("Failed to decode request", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		h.logger.Error("Failed to unmarshal request", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := h.handleReadRequest(r.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to handle read request", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		h.logger.Error("Failed to marshal response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	compressed = remote.EncodeReadResponse(data)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	if _, err := w.Write(compressed); err != nil {
		h.logger.Error("Failed to write response", zap.Error(err))
	}
}

func (h *RemoteReadHandler) handleReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resp := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, 0, len(req.Queries)),
	}

	for _, query := range req.Queries {
		result, err := h.executeQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("query execution failed: %w", err)
		}
		resp.Results = append(resp.Results, result)
	}

	return resp, nil
}

func (h *RemoteReadHandler) executeQuery(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	startMs := query.StartTimestampMs
	endMs := query.EndTimestampMs

	sqlQuery, params := h.buildQuery(query, startMs, endMs)

	rows, err := h.db.QueryContext(ctx, sqlQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	timeseriesMap := make(map[string]*prompb.TimeSeries)

	for rows.Next() {
		var (
			metricName  string
			timestamp   time.Time
			value       *float64
			count       *uint64
			sum         *float64
			attributes  map[string]string
			metricType  models.MetricType
			temporality models.Temporality
		)

		err := rows.Scan(
			&metricName,
			&timestamp,
			&value,
			&count,
			&sum,
			&attributes,
			&metricType,
			&temporality,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		labels := h.buildLabels(metricName, attributes)
		seriesKey := h.getSeriesKey(labels)

		ts, exists := timeseriesMap[seriesKey]
		if !exists {
			ts = &prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{},
			}
			timeseriesMap[seriesKey] = ts
		}

		var sampleValue float64
		switch metricType {
		case models.MetricTypeGauge:
			if value != nil {
				sampleValue = *value
			}
		case models.MetricTypeSum:
			if temporality == models.TemporalityDelta && value != nil {
				metric := &models.Metric{
					Type:        metricType,
					Temporality: temporality,
					Value:       value,
					Timestamp:   timestamp,
				}
				cumulative, err := h.converter.ConvertToCumulative(metric)
				if err != nil {
					h.logger.Warn("Failed to convert to cumulative", zap.Error(err))
					sampleValue = *value
				} else if cumulative.Value != nil {
					sampleValue = *cumulative.Value
				}
			} else if value != nil {
				sampleValue = *value
			}
		case models.MetricTypeHistogram:
			if count != nil && sum != nil && *count > 0 {
				sampleValue = *sum / float64(*count)
			}
		}

		ts.Samples = append(ts.Samples, prompb.Sample{
			Value:     sampleValue,
			Timestamp: timestamp.UnixMilli(),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	timeseries := make([]*prompb.TimeSeries, 0, len(timeseriesMap))
	for _, ts := range timeseriesMap {
		timeseries = append(timeseries, ts)
	}

	return &prompb.QueryResult{
		Timeseries: timeseries,
	}, nil
}

func (h *RemoteReadHandler) buildQuery(query *prompb.Query, startMs, endMs int64) (string, []interface{}) {
	var conditions []string
	params := []interface{}{h.workspaceID}

	conditions = append(conditions, "workspaceId = ?")

	if startMs > 0 {
		conditions = append(conditions, "timestamp >= ?")
		params = append(params, time.UnixMilli(startMs))
	}

	if endMs > 0 {
		conditions = append(conditions, "timestamp <= ?")
		params = append(params, time.UnixMilli(endMs))
	}

	for _, matcher := range query.Matchers {
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if matcher.Name == "__name__" {
				conditions = append(conditions, "metric = ?")
				params = append(params, matcher.Value)
			} else {
				conditions = append(conditions, "attributes[?] = ?")
				params = append(params, matcher.Name, matcher.Value)
			}

		case prompb.LabelMatcher_NEQ:
			if matcher.Name == "__name__" {
				conditions = append(conditions, "metric != ?")
				params = append(params, matcher.Value)
			} else {
				conditions = append(conditions, "attributes[?] != ?")
				params = append(params, matcher.Name, matcher.Value)
			}

		case prompb.LabelMatcher_RE:
			if matcher.Name == "__name__" {
				conditions = append(conditions, "match(metric, ?)")
				params = append(params, matcher.Value)
			} else {
				conditions = append(conditions, "match(attributes[?], ?)")
				params = append(params, matcher.Name, matcher.Value)
			}

		case prompb.LabelMatcher_NRE:
			if matcher.Name == "__name__" {
				conditions = append(conditions, "NOT match(metric, ?)")
				params = append(params, matcher.Value)
			} else {
				conditions = append(conditions, "NOT match(attributes[?], ?)")
				params = append(params, matcher.Name, matcher.Value)
			}
		}
	}

	table := h.selectTable(startMs, endMs)
	
	sqlQuery := fmt.Sprintf(`
		SELECT
			metric,
			timestamp,
			value,
			count,
			sum,
			attributes,
			metric_type,
			temporality
		FROM %s
		WHERE %s
		ORDER BY metric, timestamp
		LIMIT 100000
	`, table, strings.Join(conditions, " AND "))

	return sqlQuery, params
}

func (h *RemoteReadHandler) selectTable(startMs, endMs int64) string {
	if startMs == 0 && endMs == 0 {
		return "metrics_raw"
	}

	duration := time.Duration(endMs-startMs) * time.Millisecond
	age := time.Since(time.UnixMilli(startMs))

	if age < 3*time.Hour && duration < 1*time.Hour {
		return "metrics_raw"
	} else if age < 15*24*time.Hour && duration < 24*time.Hour {
		return "metrics_1m"
	} else if age < 63*24*time.Hour && duration < 7*24*time.Hour {
		return "metrics_5m"
	}
	
	return "metrics_1h"
}

func (h *RemoteReadHandler) buildLabels(metricName string, attributes map[string]string) []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(attributes)+1)
	
	labels = append(labels, &prompb.Label{
		Name:  "__name__",
		Value: metricName,
	})

	for k, v := range attributes {
		labels = append(labels, &prompb.Label{
			Name:  k,
			Value: v,
		})
	}

	return labels
}

func (h *RemoteReadHandler) getSeriesKey(labels []*prompb.Label) string {
	parts := make([]string, len(labels))
	for i, label := range labels {
		parts[i] = fmt.Sprintf("%s=%s", label.Name, label.Value)
	}
	return strings.Join(parts, ",")
}

func (h *RemoteReadHandler) Close() error {
	return h.db.Close()
}