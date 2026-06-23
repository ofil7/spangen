-- Benchmark verification + skew checks. Run against any cluster node.
-- Replace otel_traces_local / otel_traces as needed for your topology.

-- 1) Total rows ingested (query the Distributed table for a cluster-wide count).
SELECT count() AS total_spans FROM otel.otel_traces;

-- 2) Per-shard row counts — the key skew check. Counts should be within a few
--    percent of each other. Uses the local table via clusterAllReplicas so each
--    shard reports its own partition.
SELECT
    hostName()                AS host,
    count()                   AS rows_on_node
FROM clusterAllReplicas('{cluster}', otel.otel_traces_local)
GROUP BY host
ORDER BY host;

-- 3) Per-service distribution — should be roughly uniform across the catalog
--    (no single service dominating), confirming generation isn't skewed.
SELECT ServiceName, count() AS spans
FROM otel.otel_traces
GROUP BY ServiceName
ORDER BY spans DESC
LIMIT 50;

-- 4) Span-kind / status mix sanity check.
SELECT SpanKind, StatusCode, count() AS n
FROM otel.otel_traces
GROUP BY SpanKind, StatusCode
ORDER BY n DESC;

-- 5) Ingest rate over the last 5 minutes (rows per second), per minute bucket.
SELECT
    toStartOfMinute(Timestamp) AS minute,
    count()                    AS spans,
    count() / 60.0             AS spans_per_sec
FROM otel.otel_traces
WHERE Timestamp > now() - INTERVAL 5 MINUTE
GROUP BY minute
ORDER BY minute;

-- 6) Active parts per partition per shard — watch for part explosion under load
--    (async_insert with server-side batching should keep this reasonable).
SELECT
    hostName()  AS host,
    partition,
    count()     AS parts,
    sum(rows)   AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM clusterAllReplicas('{cluster}', system.parts)
WHERE database = 'otel' AND table = 'otel_traces_local' AND active
GROUP BY host, partition
ORDER BY host, partition;
