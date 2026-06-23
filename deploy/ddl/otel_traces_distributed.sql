-- Distributed table over the per-shard local tables (otel_traces_local).
-- Run AFTER otel_traces_local.sql.
--
-- The sharding key is cityHash64(TraceId). Because spangen mints fully random
-- 16-byte TraceIds, this hash is uniform => shards stay balanced (no skew),
-- while all spans of a given trace co-locate on one shard (good query locality).
-- `rand()` would also balance evenly but scatters a trace across shards.
--
-- Used as the INSERT target for:
--   -ch.mode=distributed  -ch.table=otel_traces

CREATE TABLE IF NOT EXISTS otel.otel_traces ON CLUSTER '{cluster}'
AS otel.otel_traces_local
ENGINE = Distributed('{cluster}', otel, otel_traces_local, cityHash64(TraceId));
