package receiver

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kloudmate/metrics-pipeline/internal/models"
	"github.com/kloudmate/metrics-pipeline/internal/processor"
)

type OTLPReceiver struct {
	logger    *zap.Logger
	processor *processor.MetricProcessor
	server    *grpc.Server
	address   string
}

type Config struct {
	Address        string
	MaxMessageSize int
	WorkspaceID    string
}

func NewOTLPReceiver(cfg *Config, processor *processor.MetricProcessor, logger *zap.Logger) *OTLPReceiver {
	return &OTLPReceiver{
		logger:    logger,
		processor: processor,
		address:   cfg.Address,
	}
}

func (r *OTLPReceiver) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", r.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	r.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(100 * 1024 * 1024),
		grpc.MaxSendMsgSize(100 * 1024 * 1024),
	)

	pmetricotlp.RegisterGRPCServer(r.server, r)

	r.logger.Info("Starting OTLP receiver", zap.String("address", r.address))

	go func() {
		<-ctx.Done()
		r.logger.Info("Shutting down OTLP receiver")
		r.server.GracefulStop()
	}()

	if err := r.server.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func (r *OTLPReceiver) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	md := req.Metrics()
	
	if md.DataPointCount() == 0 {
		return pmetricotlp.NewExportResponse(), nil
	}

	metrics, err := r.convertPDataToModels(md)
	if err != nil {
		r.logger.Error("Failed to convert metrics", zap.Error(err))
		return pmetricotlp.NewExportResponse(), status.Error(codes.InvalidArgument, err.Error())
	}

	if err := r.processor.Process(ctx, metrics); err != nil {
		r.logger.Error("Failed to process metrics", zap.Error(err))
		return pmetricotlp.NewExportResponse(), status.Error(codes.Internal, err.Error())
	}

	return pmetricotlp.NewExportResponse(), nil
}

func (r *OTLPReceiver) convertPDataToModels(md pmetric.Metrics) ([]*models.Metric, error) {
	var result []*models.Metric

	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		resource := rm.Resource()
		
		serviceName := ""
		if serviceNameAttr, ok := resource.Attributes().Get("service.name"); ok {
			serviceName = serviceNameAttr.AsString()
		}

		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()
			
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				converted, err := r.convertMetric(metric, serviceName, resource.Attributes().AsRaw())
				if err != nil {
					r.logger.Warn("Failed to convert metric", 
						zap.String("metric", metric.Name()),
						zap.Error(err))
					continue
				}
				result = append(result, converted...)
			}
		}
	}

	return result, nil
}

func (r *OTLPReceiver) convertMetric(metric pmetric.Metric, serviceName string, resourceAttrs map[string]interface{}) ([]*models.Metric, error) {
	var result []*models.Metric

	baseMetric := models.Metric{
		MetricName:  metric.Name(),
		ServiceName: serviceName,
		Attributes:  r.mergeAttributes(resourceAttrs, nil),
	}

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		gauge := metric.Gauge()
		dataPoints := gauge.DataPoints()
		
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			m := baseMetric
			m.Type = models.MetricTypeGauge
			m.Temporality = models.TemporalityUnspecified
			m.Timestamp = dp.Timestamp().AsTime()
			m.Attributes = r.mergeAttributes(resourceAttrs, dp.Attributes().AsRaw())
			
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				val := float64(dp.IntValue())
				m.Value = &val
			case pmetric.NumberDataPointValueTypeDouble:
				val := dp.DoubleValue()
				m.Value = &val
			}
			
			m.Exemplars = r.convertExemplars(dp.Exemplars())
			result = append(result, &m)
		}

	case pmetric.MetricTypeSum:
		sum := metric.Sum()
		dataPoints := sum.DataPoints()
		
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			m := baseMetric
			m.Type = models.MetricTypeSum
			m.Temporality = r.convertTemporality(sum.AggregationTemporality())
			m.IsMonotonic = sum.IsMonotonic()
			m.Timestamp = dp.Timestamp().AsTime()
			m.Attributes = r.mergeAttributes(resourceAttrs, dp.Attributes().AsRaw())
			
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				val := float64(dp.IntValue())
				m.Value = &val
			case pmetric.NumberDataPointValueTypeDouble:
				val := dp.DoubleValue()
				m.Value = &val
			}
			
			m.Exemplars = r.convertExemplars(dp.Exemplars())
			result = append(result, &m)
		}

	case pmetric.MetricTypeHistogram:
		histogram := metric.Histogram()
		dataPoints := histogram.DataPoints()
		
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			m := baseMetric
			m.Type = models.MetricTypeHistogram
			m.Temporality = r.convertTemporality(histogram.AggregationTemporality())
			m.Timestamp = dp.Timestamp().AsTime()
			m.Attributes = r.mergeAttributes(resourceAttrs, dp.Attributes().AsRaw())
			
			if dp.HasCount() {
				count := dp.Count()
				m.Count = &count
			}
			
			if dp.HasSum() {
				sum := dp.Sum()
				m.Sum = &sum
			}
			
			bucketCounts := dp.BucketCounts()
			explicitBounds := dp.ExplicitBounds()
			
			if bucketCounts.Len() > 0 {
				m.Buckets = make([]models.HistogramBucket, bucketCounts.Len())
				
				for j := 0; j < bucketCounts.Len(); j++ {
					upperBound := float64(0)
					if j < explicitBounds.Len() {
						upperBound = explicitBounds.At(j)
					} else {
						upperBound = math.Inf(1)
					}
					
					m.Buckets[j] = models.HistogramBucket{
						UpperBound: upperBound,
						Count:      bucketCounts.At(j),
					}
				}
			}
			
			m.Exemplars = r.convertExemplars(dp.Exemplars())
			result = append(result, &m)
		}

	case pmetric.MetricTypeExponentialHistogram:
		expHistogram := metric.ExponentialHistogram()
		dataPoints := expHistogram.DataPoints()
		
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			m := baseMetric
			m.Type = models.MetricTypeExponentialHistogram
			m.Temporality = r.convertTemporality(expHistogram.AggregationTemporality())
			m.Timestamp = dp.Timestamp().AsTime()
			m.Attributes = r.mergeAttributes(resourceAttrs, dp.Attributes().AsRaw())
			
			if dp.HasCount() {
				count := dp.Count()
				m.Count = &count
			}
			
			if dp.HasSum() {
				sum := dp.Sum()
				m.Sum = &sum
			}
			
			m.Exemplars = r.convertExemplars(dp.Exemplars())
			result = append(result, &m)
		}

	case pmetric.MetricTypeSummary:
		summary := metric.Summary()
		dataPoints := summary.DataPoints()
		
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)
			m := baseMetric
			m.Type = models.MetricTypeSummary
			m.Timestamp = dp.Timestamp().AsTime()
			m.Attributes = r.mergeAttributes(resourceAttrs, dp.Attributes().AsRaw())
			
			count := dp.Count()
			m.Count = &count
			
			sum := dp.Sum()
			m.Sum = &sum
			
			result = append(result, &m)
		}

	default:
		return nil, fmt.Errorf("unsupported metric type: %v", metric.Type())
	}

	return result, nil
}

func (r *OTLPReceiver) convertTemporality(t pmetric.AggregationTemporality) models.Temporality {
	switch t {
	case pmetric.AggregationTemporalityCumulative:
		return models.TemporalityCumulative
	case pmetric.AggregationTemporalityDelta:
		return models.TemporalityDelta
	default:
		return models.TemporalityUnspecified
	}
}

func (r *OTLPReceiver) convertExemplars(exemplars pmetric.ExemplarSlice) []models.Exemplar {
	if exemplars.Len() == 0 {
		return nil
	}

	result := make([]models.Exemplar, 0, exemplars.Len())
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		exemplar := models.Exemplar{
			Timestamp:  e.Timestamp().AsTime(),
			Attributes: e.FilteredAttributes().AsRaw(),
		}

		if e.HasTraceID() {
			exemplar.TraceID = e.TraceID().String()
		}
		
		if e.HasSpanID() {
			exemplar.SpanID = e.SpanID().String()
		}

		switch e.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			exemplar.Value = float64(e.IntValue())
		case pmetric.ExemplarValueTypeDouble:
			exemplar.Value = e.DoubleValue()
		}

		result = append(result, exemplar)
	}

	return result
}

func (r *OTLPReceiver) mergeAttributes(resourceAttrs, dataPointAttrs map[string]interface{}) map[string]string {
	result := make(map[string]string)

	for k, v := range resourceAttrs {
		result[k] = fmt.Sprintf("%v", v)
	}

	for k, v := range dataPointAttrs {
		result[k] = fmt.Sprintf("%v", v)
	}

	return result
}

func (r *OTLPReceiver) Stop() error {
	if r.server != nil {
		r.server.GracefulStop()
	}
	return nil
}