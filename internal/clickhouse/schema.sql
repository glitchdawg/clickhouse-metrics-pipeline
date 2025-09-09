-- Raw metrics table for high-resolution data
CREATE TABLE IF NOT EXISTS metrics_raw
(
    workspaceId String CODEC(ZSTD(1)),
    series_hash UInt64 CODEC(ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime64(6) CODEC(Delta(8), ZSTD(1)),
    
    metric_type Enum8(
        'unknown' = 0,
        'gauge' = 1,
        'sum' = 2,
        'histogram' = 3,
        'summary' = 4,
        'exponential_histogram' = 5
    ) CODEC(ZSTD(1)),
    
    temporality Enum8(
        'unspecified' = 0,
        'cumulative' = 1,
        'delta' = 2
    ) CODEC(ZSTD(1)),
    
    is_monotonic UInt8 CODEC(ZSTD(1)),
    
    -- Scalar values
    value Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    sum Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    -- Histogram buckets
    buckets Nested (
        le Float64,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    -- Exponential histogram fields
    exp_scale Nullable(Int32) CODEC(ZSTD(1)),
    exp_zero_count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    exp_zero_threshold Nullable(Float64) CODEC(ZSTD(1)),
    exp_positive_buckets Nested (
        index Int32,
        count UInt64
    ) CODEC(ZSTD(1)),
    exp_negative_buckets Nested (
        index Int32,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    -- Attributes
    attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    
    -- Exemplars
    exemplars Nested (
        spanId String,
        traceId String,
        value Float64,
        timestamp DateTime64(6),
        attributes Map(String, String)
    ) CODEC(ZSTD(1)),
    
    _ttl DateTime DEFAULT now() + INTERVAL 3 HOUR,
    
    INDEX idx_metric metric TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_serviceName serviceName TYPE set(1000) GRANULARITY 1,
    INDEX idx_series_hash series_hash TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (workspaceId, metric, series_hash, timestamp)
TTL _ttl
SETTINGS index_granularity = 8192;

-- 1-minute aggregation table
CREATE TABLE IF NOT EXISTS metrics_1m
(
    workspaceId String CODEC(ZSTD(1)),
    series_hash UInt64 CODEC(ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    
    metric_type Enum8(
        'unknown' = 0,
        'gauge' = 1,
        'sum' = 2,
        'histogram' = 3,
        'summary' = 4,
        'exponential_histogram' = 5
    ) CODEC(ZSTD(1)),
    
    temporality Enum8(
        'unspecified' = 0,
        'cumulative' = 1,
        'delta' = 2
    ) CODEC(ZSTD(1)),
    
    is_monotonic UInt8 CODEC(ZSTD(1)),
    
    -- Aggregated values
    value_min Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_max Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_avg Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_last Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    sum Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    -- Merged histogram buckets
    buckets Nested (
        le Float64,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    -- Exponential histogram fields (merged)
    exp_scale Nullable(Int32) CODEC(ZSTD(1)),
    exp_zero_count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    exp_zero_threshold Nullable(Float64) CODEC(ZSTD(1)),
    exp_positive_buckets Nested (
        index Int32,
        count UInt64
    ) CODEC(ZSTD(1)),
    exp_negative_buckets Nested (
        index Int32,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    
    samples_count UInt32 CODEC(T64, ZSTD(1)),
    
    _ttl DateTime DEFAULT now() + INTERVAL 15 DAY,
    
    INDEX idx_metric metric TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_serviceName serviceName TYPE set(1000) GRANULARITY 1,
    INDEX idx_series_hash series_hash TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (workspaceId, metric, series_hash, timestamp)
TTL _ttl
SETTINGS index_granularity = 8192;

-- Materialized view for 1-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m_mv TO metrics_1m AS
SELECT
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfMinute(timestamp) AS timestamp,
    metric_type,
    temporality,
    is_monotonic,
    
    -- Gauge aggregations
    min(value) AS value_min,
    max(value) AS value_max,
    avg(value) AS value_avg,
    anyLast(value) AS value_last,
    
    -- Sum/Counter aggregations
    sum(count) AS count,
    sum(sum) AS sum,
    
    -- Histogram bucket merging
    groupArray(buckets.le) AS buckets.le,
    groupArray(buckets.count) AS buckets.count,
    
    -- Exponential histogram merging
    any(exp_scale) AS exp_scale,
    sum(exp_zero_count) AS exp_zero_count,
    any(exp_zero_threshold) AS exp_zero_threshold,
    groupArray(exp_positive_buckets.index) AS exp_positive_buckets.index,
    groupArray(exp_positive_buckets.count) AS exp_positive_buckets.count,
    groupArray(exp_negative_buckets.index) AS exp_negative_buckets.index,
    groupArray(exp_negative_buckets.count) AS exp_negative_buckets.count,
    
    any(attributes) AS attributes,
    count() AS samples_count
FROM metrics_raw
WHERE timestamp >= now() - INTERVAL 1 DAY
GROUP BY
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfMinute(timestamp),
    metric_type,
    temporality,
    is_monotonic;

-- 5-minute aggregation table
CREATE TABLE IF NOT EXISTS metrics_5m
(
    workspaceId String CODEC(ZSTD(1)),
    series_hash UInt64 CODEC(ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime CODEC(Delta(4), ZSTD(1)),
    
    metric_type Enum8(
        'unknown' = 0,
        'gauge' = 1,
        'sum' = 2,
        'histogram' = 3,
        'summary' = 4,
        'exponential_histogram' = 5
    ) CODEC(ZSTD(1)),
    
    temporality Enum8(
        'unspecified' = 0,
        'cumulative' = 1,
        'delta' = 2
    ) CODEC(ZSTD(1)),
    
    is_monotonic UInt8 CODEC(ZSTD(1)),
    
    value_min Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_max Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_avg Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_last Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    sum Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    buckets Nested (
        le Float64,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    
    samples_count UInt32 CODEC(T64, ZSTD(1)),
    
    _ttl DateTime DEFAULT now() + INTERVAL 63 DAY,
    
    INDEX idx_metric metric TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_serviceName serviceName TYPE set(1000) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (workspaceId, metric, series_hash, timestamp)
TTL _ttl
SETTINGS index_granularity = 8192;

-- Materialized view for 5-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m_mv TO metrics_5m AS
SELECT
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfFiveMinute(timestamp) AS timestamp,
    metric_type,
    temporality,
    is_monotonic,
    
    min(value_min) AS value_min,
    max(value_max) AS value_max,
    avg(value_avg) AS value_avg,
    anyLast(value_last) AS value_last,
    
    sum(count) AS count,
    sum(sum) AS sum,
    
    groupArray(buckets.le) AS buckets.le,
    groupArray(buckets.count) AS buckets.count,
    
    any(attributes) AS attributes,
    sum(samples_count) AS samples_count
FROM metrics_1m
WHERE timestamp >= now() - INTERVAL 15 DAY
GROUP BY
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfFiveMinute(timestamp),
    metric_type,
    temporality,
    is_monotonic;

-- 1-hour aggregation table
CREATE TABLE IF NOT EXISTS metrics_1h
(
    workspaceId String CODEC(ZSTD(1)),
    series_hash UInt64 CODEC(ZSTD(1)),
    metric LowCardinality(String) CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime CODEC(Delta(4), ZSTD(1)),
    
    metric_type Enum8(
        'unknown' = 0,
        'gauge' = 1,
        'sum' = 2,
        'histogram' = 3,
        'summary' = 4,
        'exponential_histogram' = 5
    ) CODEC(ZSTD(1)),
    
    temporality Enum8(
        'unspecified' = 0,
        'cumulative' = 1,
        'delta' = 2
    ) CODEC(ZSTD(1)),
    
    is_monotonic UInt8 CODEC(ZSTD(1)),
    
    value_min Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_max Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_avg Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    value_last Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    count Nullable(UInt64) CODEC(T64, ZSTD(1)),
    sum Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    
    buckets Nested (
        le Float64,
        count UInt64
    ) CODEC(ZSTD(1)),
    
    attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    
    samples_count UInt32 CODEC(T64, ZSTD(1)),
    
    _ttl DateTime DEFAULT now() + INTERVAL 455 DAY,
    
    INDEX idx_metric metric TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_serviceName serviceName TYPE set(1000) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (workspaceId, metric, series_hash, timestamp)
TTL _ttl
SETTINGS index_granularity = 8192;

-- Materialized view for 1-hour aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h_mv TO metrics_1h AS
SELECT
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfHour(timestamp) AS timestamp,
    metric_type,
    temporality,
    is_monotonic,
    
    min(value_min) AS value_min,
    max(value_max) AS value_max,
    avg(value_avg) AS value_avg,
    anyLast(value_last) AS value_last,
    
    sum(count) AS count,
    sum(sum) AS sum,
    
    groupArray(buckets.le) AS buckets.le,
    groupArray(buckets.count) AS buckets.count,
    
    any(attributes) AS attributes,
    sum(samples_count) AS samples_count
FROM metrics_5m
WHERE timestamp >= now() - INTERVAL 63 DAY
GROUP BY
    workspaceId,
    series_hash,
    metric,
    serviceName,
    toStartOfHour(timestamp),
    metric_type,
    temporality,
    is_monotonic;