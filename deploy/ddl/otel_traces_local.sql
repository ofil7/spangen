-- Cluster local (per-shard) replicated table.
-- Apply ON CLUSTER so every node gets the local table. The {cluster}, {shard}
-- and {replica} macros are expected to be defined in your ClickHouse config
-- (the clickhouse-operator / Altinity operator define them by default).
--
-- Used as the INSERT target for:
--   -ch.mode=shard-roundrobin  -ch.table=otel_traces_local   (per-shard endpoints)
--   -ch.mode=local             -ch.table=otel_traces_local   (single replica set)
-- and as the backing table for the Distributed table (see otel_traces_distributed.sql).

CREATE DATABASE IF NOT EXISTS otel ON CLUSTER '{cluster}';

CREATE TABLE IF NOT EXISTS otel.otel_traces_local ON CLUSTER '{cluster}'
(
    Timestamp          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId            String CODEC(ZSTD(1)),
    SpanId             String CODEC(ZSTD(1)),
    ParentSpanId       String CODEC(ZSTD(1)),
    TraceState         String CODEC(ZSTD(1)),
    SpanName           LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind           LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName          String CODEC(ZSTD(1)),
    ScopeVersion       String CODEC(ZSTD(1)),
    SpanAttributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration           UInt64 CODEC(ZSTD(1)),
    StatusCode         LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage      String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp  DateTime64(9),
        Name       LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId    String,
        SpanId     String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/otel_traces_local', '{replica}')
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
