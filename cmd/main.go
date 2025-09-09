package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/kloudmate/metrics-pipeline/internal/clickhouse"
	"github.com/kloudmate/metrics-pipeline/internal/processor"
	"github.com/kloudmate/metrics-pipeline/internal/receiver"
	"github.com/kloudmate/metrics-pipeline/pkg/promread"
)

type Config struct {
	Receiver struct {
		OTLP struct {
			Address string `yaml:"address"`
		} `yaml:"otlp"`
	} `yaml:"receiver"`

	ClickHouse struct {
		Addresses     []string      `yaml:"addresses"`
		Database      string        `yaml:"database"`
		Username      string        `yaml:"username"`
		Password      string        `yaml:"password"`
		BatchSize     int           `yaml:"batch_size"`
		FlushInterval time.Duration `yaml:"flush_interval"`
		MaxIdleConns  int           `yaml:"max_idle_conns"`
		MaxOpenConns  int           `yaml:"max_open_conns"`
	} `yaml:"clickhouse"`

	Processor struct {
		WorkspaceID           string        `yaml:"workspace_id"`
		ConvertToDelta        bool          `yaml:"convert_to_delta"`
		BatchSize             int           `yaml:"batch_size"`
		FlushInterval         time.Duration `yaml:"flush_interval"`
		EnableExemplars       bool          `yaml:"enable_exemplars"`
		MaxExemplarsPerMetric int           `yaml:"max_exemplars_per_metric"`
	} `yaml:"processor"`

	RemoteRead struct {
		Enabled bool   `yaml:"enabled"`
		Address string `yaml:"address"`
	} `yaml:"remote_read"`

	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := initLogger(cfg.Logging.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chWriter, err := clickhouse.NewWriter(&clickhouse.Config{
		Addresses:     cfg.ClickHouse.Addresses,
		Database:      cfg.ClickHouse.Database,
		Username:      cfg.ClickHouse.Username,
		Password:      cfg.ClickHouse.Password,
		BatchSize:     cfg.ClickHouse.BatchSize,
		FlushInterval: cfg.ClickHouse.FlushInterval,
		MaxIdleConns:  cfg.ClickHouse.MaxIdleConns,
		MaxOpenConns:  cfg.ClickHouse.MaxOpenConns,
	}, logger)
	if err != nil {
		logger.Fatal("Failed to create ClickHouse writer", zap.Error(err))
	}
	defer chWriter.Close()

	metricProcessor := processor.NewMetricProcessor(&processor.Config{
		WorkspaceID:           cfg.Processor.WorkspaceID,
		ConvertToDelta:        cfg.Processor.ConvertToDelta,
		BatchSize:             cfg.Processor.BatchSize,
		FlushInterval:         cfg.Processor.FlushInterval,
		EnableExemplars:       cfg.Processor.EnableExemplars,
		MaxExemplarsPerMetric: cfg.Processor.MaxExemplarsPerMetric,
	}, chWriter, logger)
	defer metricProcessor.Close()

	otlpReceiver := receiver.NewOTLPReceiver(&receiver.Config{
		Address:     cfg.Receiver.OTLP.Address,
		WorkspaceID: cfg.Processor.WorkspaceID,
	}, metricProcessor, logger)

	go func() {
		if err := otlpReceiver.Start(ctx); err != nil {
			logger.Fatal("Failed to start OTLP receiver", zap.Error(err))
		}
	}()

	if cfg.RemoteRead.Enabled {
		remoteReadHandler, err := promread.NewRemoteReadHandler(&promread.Config{
			ClickHouseAddr: cfg.ClickHouse.Addresses[0],
			Database:       cfg.ClickHouse.Database,
			Username:       cfg.ClickHouse.Username,
			Password:       cfg.ClickHouse.Password,
			WorkspaceID:    cfg.Processor.WorkspaceID,
		}, logger)
		if err != nil {
			logger.Fatal("Failed to create remote read handler", zap.Error(err))
		}
		defer remoteReadHandler.Close()

		http.Handle("/api/v1/read", remoteReadHandler)
		
		go func() {
			logger.Info("Starting Prometheus remote read API", zap.String("address", cfg.RemoteRead.Address))
			if err := http.ListenAndServe(cfg.RemoteRead.Address, nil); err != nil {
				logger.Fatal("Failed to start remote read server", zap.Error(err))
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Metrics pipeline started successfully")

	<-sigChan
	logger.Info("Shutting down metrics pipeline...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := metricProcessor.Flush(shutdownCtx); err != nil {
		logger.Error("Failed to flush metrics on shutdown", zap.Error(err))
	}

	if err := otlpReceiver.Stop(); err != nil {
		logger.Error("Failed to stop OTLP receiver", zap.Error(err))
	}

	logger.Info("Metrics pipeline shutdown complete")
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	setDefaults(&cfg)
	
	return &cfg, nil
}

func setDefaults(cfg *Config) {
	if cfg.Receiver.OTLP.Address == "" {
		cfg.Receiver.OTLP.Address = ":4317"
	}

	if cfg.ClickHouse.BatchSize == 0 {
		cfg.ClickHouse.BatchSize = 1000
	}

	if cfg.ClickHouse.FlushInterval == 0 {
		cfg.ClickHouse.FlushInterval = 10 * time.Second
	}

	if cfg.ClickHouse.MaxIdleConns == 0 {
		cfg.ClickHouse.MaxIdleConns = 5
	}

	if cfg.ClickHouse.MaxOpenConns == 0 {
		cfg.ClickHouse.MaxOpenConns = 10
	}

	if cfg.Processor.BatchSize == 0 {
		cfg.Processor.BatchSize = 1000
	}

	if cfg.Processor.FlushInterval == 0 {
		cfg.Processor.FlushInterval = 10 * time.Second
	}

	if cfg.Processor.MaxExemplarsPerMetric == 0 {
		cfg.Processor.MaxExemplarsPerMetric = 10
	}

	if cfg.RemoteRead.Address == "" {
		cfg.RemoteRead.Address = ":9201"
	}

	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
}

func initLogger(level string) (*zap.Logger, error) {
	var zapLevel zap.AtomicLevel
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}

	config := zap.NewProductionConfig()
	config.Level = zapLevel
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zap.ISO8601TimeEncoder

	return config.Build()
}