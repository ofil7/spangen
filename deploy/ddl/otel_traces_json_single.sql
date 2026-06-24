-- Single-node JSON-attribute schema (smoke testing / one shard).
--
-- This matches the newer OTel/ClickStack-style traces schema where attribute
-- columns are the native ClickHouse `JSON` type (not Map) and Duration is Int64.
-- Use it with spangen's  -ch.schema=json  (SPANGEN_CH_SCHEMA=json).
--
-- The JSON data type requires ClickHouse >= 25.6 for string-serialized inserts.
-- On versions where JSON is still experimental you must allow it before CREATE;
-- on GA versions this SET is a harmless no-op.
SET allow_experimental_json_type = 1;

CREATE DATABASE IF NOT EXISTS otel;

CREATE TABLE IF NOT EXISTS otel.otel_traces
(
    Timestamp          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId            String CODEC(ZSTD(1)),
    SpanId             String CODEC(ZSTD(1)),
    ParentSpanId       String CODEC(ZSTD(1)),
    TraceState         String CODEC(ZSTD(1)),
    SpanName           LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind           LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String) CODEC(ZSTD(1)),
    Duration           Int64 CODEC(ZSTD(1)),
    StatusCode         LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage      String CODEC(ZSTD(1)),
    ResourceAttributes JSON CODEC(ZSTD(1)),
    SpanAttributes     JSON CODEC(ZSTD(1)),
    ScopeAttributes    JSON CODEC(ZSTD(1)),
    ScopeName          String CODEC(ZSTD(1)),
    ScopeVersion       String CODEC(ZSTD(1)),
    `Links.TraceId`    Array(String) CODEC(ZSTD(1)),
    `Links.SpanId`     Array(String) CODEC(ZSTD(1)),
    `Links.TraceState` Array(String) CODEC(ZSTD(1)),
    `Links.Attributes` Array(JSON) CODEC(ZSTD(1)),
    `Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name`      Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(JSON) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_status_code StatusCode TYPE set(0) GRANULARITY 1,
    INDEX idx_span_name SpanName TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toStartOfHour(Timestamp)
ORDER BY toUnixTimestamp(Timestamp)
TTL toDateTime(Timestamp) + toIntervalHour(5)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
