package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	endpoint      = flag.String("endpoint", "localhost:4317", "OTLP endpoint")
	serviceName   = flag.String("service", "test-service", "Service name")
	duration      = flag.Duration("duration", 5*time.Minute, "Test duration")
	interval      = flag.Duration("interval", 10*time.Second, "Metric send interval")
	numCounters   = flag.Int("counters", 5, "Number of counter metrics")
	numGauges     = flag.Int("gauges", 5, "Number of gauge metrics")
	numHistograms = flag.Int("histograms", 5, "Number of histogram metrics")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Initialize OTLP exporter
	conn, err := grpc.DialContext(ctx, *endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("Failed to create gRPC connection: %v", err)
	}
	defer conn.Close()

	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithGRPCConn(conn),
		otlpmetricgrpc.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector),
	)
	if err != nil {
		log.Fatalf("Failed to create metric exporter: %v", err)
	}

	// Create resource
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(*serviceName),
		semconv.ServiceVersionKey.String("1.0.0"),
		attribute.String("environment", "testing"),
		attribute.String("region", "us-east-1"),
	)

	// Create meter provider
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(metricExporter, 
				sdkmetric.WithInterval(*interval),
			),
		),
		sdkmetric.WithResource(res),
	)
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	otel.SetMeterProvider(meterProvider)
	meter := meterProvider.Meter("test-meter")

	fmt.Printf("Starting metrics generator...\n")
	fmt.Printf("Endpoint: %s\n", *endpoint)
	fmt.Printf("Duration: %s\n", *duration)
	fmt.Printf("Interval: %s\n", *interval)

	// Create and register metrics
	if err := createAndRunMetrics(ctx, meter); err != nil {
		log.Fatalf("Failed to create metrics: %v", err)
	}

	<-ctx.Done()
	fmt.Println("Metrics generation completed")
}

func createAndRunMetrics(ctx context.Context, meter metric.Meter) error {
	rand.Seed(time.Now().UnixNano())

	// Create counters
	counters := make([]metric.Int64Counter, *numCounters)
	for i := 0; i < *numCounters; i++ {
		counter, err := meter.Int64Counter(
			fmt.Sprintf("test_counter_%d", i),
			metric.WithDescription(fmt.Sprintf("Test counter metric %d", i)),
			metric.WithUnit("1"),
		)
		if err != nil {
			return fmt.Errorf("failed to create counter %d: %w", i, err)
		}
		counters[i] = counter
	}

	// Create gauges (using async gauge pattern)
	for i := 0; i < *numGauges; i++ {
		gaugeIndex := i
		_, err := meter.Float64ObservableGauge(
			fmt.Sprintf("test_gauge_%d", gaugeIndex),
			metric.WithDescription(fmt.Sprintf("Test gauge metric %d", gaugeIndex)),
			metric.WithUnit("1"),
			metric.WithFloat64Callback(func(ctx context.Context, o metric.Float64Observer) error {
				// Simulate varying gauge values
				value := 50 + 30*math.Sin(float64(time.Now().Unix())/10+float64(gaugeIndex))
				o.Observe(value, metric.WithAttributes(
					attribute.String("gauge_type", fmt.Sprintf("type_%d", gaugeIndex%3)),
					attribute.Int("index", gaugeIndex),
				))
				return nil
			}),
		)
		if err != nil {
			return fmt.Errorf("failed to create gauge %d: %w", i, err)
		}
	}

	// Create histograms
	histograms := make([]metric.Float64Histogram, *numHistograms)
	for i := 0; i < *numHistograms; i++ {
		histogram, err := meter.Float64Histogram(
			fmt.Sprintf("http_request_duration_%d", i),
			metric.WithDescription(fmt.Sprintf("HTTP request duration for endpoint %d", i)),
			metric.WithUnit("ms"),
		)
		if err != nil {
			return fmt.Errorf("failed to create histogram %d: %w", i, err)
		}
		histograms[i] = histogram
	}

	// Start generating metric data
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	endpoints := []string{"/api/users", "/api/products", "/api/orders", "/api/metrics", "/api/health"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statusCodes := []string{"200", "201", "400", "404", "500"}

	startTime := time.Now()
	eventCount := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			eventCount++

			// Update counters
			for i, counter := range counters {
				if rand.Float32() < 0.7 { // 70% chance to increment
					increment := int64(rand.Intn(10) + 1)
					counter.Add(ctx, increment, metric.WithAttributes(
						attribute.String("counter_type", fmt.Sprintf("type_%d", i%3)),
						attribute.String("status", statusCodes[rand.Intn(len(statusCodes))]),
					))
				}
			}

			// Update histograms with realistic latency distribution
			for i, histogram := range histograms {
				if rand.Float32() < 0.8 { // 80% chance to record
					// Simulate realistic latency distribution
					var latency float64
					r := rand.Float64()
					
					switch {
					case r < 0.5: // 50% fast requests (5-50ms)
						latency = 5 + rand.Float64()*45
					case r < 0.85: // 35% medium requests (50-200ms)
						latency = 50 + rand.Float64()*150
					case r < 0.95: // 10% slow requests (200-1000ms)
						latency = 200 + rand.Float64()*800
					default: // 5% very slow requests (1000-5000ms)
						latency = 1000 + rand.Float64()*4000
					}

					histogram.Record(ctx, latency, metric.WithAttributes(
						attribute.String("endpoint", endpoints[rand.Intn(len(endpoints))]),
						attribute.String("method", methods[rand.Intn(len(methods))]),
						attribute.String("status", statusCodes[rand.Intn(len(statusCodes))]),
					))
				}
			}

			// Simulate reset for testing (every 1000 events for counter 0)
			if eventCount%1000 == 0 && len(counters) > 0 {
				fmt.Printf("Simulating reset at event %d (elapsed: %s)\n", 
					eventCount, time.Since(startTime).Round(time.Second))
			}

			// Progress update every 10 seconds
			if eventCount%100 == 0 {
				elapsed := time.Since(startTime)
				fmt.Printf("Generated %d events in %s\n", eventCount, elapsed.Round(time.Second))
			}
		}
	}
}

func simulateExponentialHistogram(ctx context.Context, meter metric.Meter) error {
	// Create an exponential histogram for response times
	responseTime, err := meter.Float64Histogram(
		"response_time_exponential",
		metric.WithDescription("Response time with exponential buckets"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	// Generate exponentially distributed values
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Generate exponentially distributed latencies
				lambda := 0.01 // Rate parameter
				value := -math.Log(1-rand.Float64()) / lambda
				
				responseTime.Record(ctx, value, metric.WithAttributes(
					attribute.String("service", "api"),
					attribute.String("operation", "query"),
				))
			}
		}
	}()

	return nil
}