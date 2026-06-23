# spangen — ClickHouse trace-span load generator

A high-throughput OpenTelemetry **trace-span generator** for benchmarking
ClickHouse. Each replica emits realistic, bounded-cardinality spans at a target
rate; run 10 replicas at 100k spans/s each to drive **~1,000,000 spans/s**.

It sends to ClickHouse two ways, from the **same** in-memory span data so the
paths are directly comparable:

1. **Direct → ClickHouse** over the native protocol (`clickhouse-go/v2` batch API).
2. **OTLP/gRPC → OpenTelemetry Collector** → the collector's `clickhouse`
   exporter → ClickHouse.

Built for an **air-gapped** network: all Go dependencies are vendored into the
image and there are **no calls to the public internet** at build-time (after
vendoring) or runtime. Packaged as a tiny **distroless, non-root** image
(OpenShift `restricted` SCC compatible).

## Why it avoids ClickHouse skew

- **Random 16-byte TraceIds** (per-worker PCG RNG) → uniform hashing, so shards
  stay balanced under any sharding key.
- **Distributed mode** ships DDL sharding on `cityHash64(TraceId)` (uniform).
- **shard-roundrobin mode** rotates batches across shard endpoints and **offsets
  the starting shard by the pod's replica index**, so the 10 replicas don't all
  hit shard 0 first.
- **Bounded attribute cardinality** (semconv values drawn from fixed pools) keeps
  `LowCardinality`/`Map` dictionaries and compression even across shards.
- Each (replica, worker) gets a unique RNG seed → no duplicate IDs, no correlated
  patterns.

## Schema

Both paths target the **standard OTel `clickhouseexporter` `otel_traces`
schema** (exact columns/codecs/skip-indexes). DDL is in [`deploy/ddl`](deploy/ddl):

| File | Topology | Use with |
|---|---|---|
| `otel_traces_single.sql` | one node, `MergeTree` | `-ch.mode=local -ch.table=otel_traces` |
| `otel_traces_local.sql` | per-shard `ReplicatedMergeTree` (`ON CLUSTER`) | `-ch.mode=shard-roundrobin` or `local`, `-ch.table=otel_traces_local` |
| `otel_traces_distributed.sql` | `Distributed` over the local tables | `-ch.mode=distributed -ch.table=otel_traces` |

The generator is **insert-only** — it never runs DDL. Apply the SQL yourself
(or let the collector's exporter create it with `create_schema: true`, then set
`create_schema: false` once it exists). For an apples-to-apples comparison,
**both paths must use the same table** — pre-create it and point the collector's
`traces_table_name` at it.

## Air-gapped quick start (prebuilt image included)

This repo ships a **prebuilt `linux/amd64` image** at
[`spangen-image.tar.gz`](spangen-image.tar.gz) (~8 MB), so you don't need to
build anything. Download the repo ZIP, copy it across the air gap, then:

```bash
# 1. load the image on a host inside the air-gap
gunzip -c spangen-image.tar.gz | docker load          # -> spangen:latest

# 2. retag + push to your internal registry
docker tag spangen:latest image-registry.internal/benchmark/spangen:latest
docker push image-registry.internal/benchmark/spangen:latest

# 3. create the table (pick your topology)
clickhouse-client < deploy/ddl/otel_traces_single.sql   # or local + distributed

# 4. deploy 10 replicas with Helm
helm install bench deploy/helm/spangen -n benchmark --create-namespace \
  -f deploy/helm/spangen/examples/values-clickhouse-distributed.yaml \
  --set image.repository=image-registry.internal/benchmark/spangen
```

Want to rebuild from source instead? See below.

## Build & transfer (air-gapped workflow)

On a **connected** build host (Go 1.23+, Docker):

```bash
cd spangen
make vendor        # resolves deps, writes go.sum + vendor/  (needs internet)
make image         # builds spangen:latest fully offline from vendor/
make save          # -> spangen-image.tar.gz
```

Copy `spangen-image.tar.gz` to the air-gapped registry host and load it:

```bash
make load TAR=spangen-image.tar          # or: gunzip -c spangen-image.tar.gz | docker load
docker tag spangen:latest image-registry.internal/benchmark/spangen:latest
docker push image-registry.internal/benchmark/spangen:latest
```

Then deploy. **Helm (recommended for OpenShift)** — every `SPANGEN_*` knob is a
value, defaults are the 10×100k setup, pods spread across nodes, and the
ClickHouse password goes through a Secret:

```bash
# render to check first
helm template bench deploy/helm/spangen -f deploy/helm/spangen/examples/values-otlp.yaml

# install (clickhouse distributed example)
helm install bench deploy/helm/spangen \
  -n benchmark --create-namespace \
  -f deploy/helm/spangen/examples/values-clickhouse-distributed.yaml \
  --set image.repository=image-registry.internal/benchmark/spangen

# change rate / replicas live
helm upgrade bench deploy/helm/spangen --reuse-values --set generator.rate=150000
```

Or the plain manifest (no Helm):

```bash
kubectl apply -f deploy/deployment.yaml   # 10 replicas + Service + ServiceMonitor
```

See [`deploy/helm/spangen/values.yaml`](deploy/helm/spangen/values.yaml) for all
options and [`examples/`](deploy/helm/spangen/examples) for ready-made profiles.

## Configuration

Every option is a flag **and** an env var (env is the default, flags override).
Key ones (see [`internal/config`](internal/config/config.go) for all):

| Env / flag | Default | Meaning |
|---|---|---|
| `SPANGEN_SINK` / `-sink` | `clickhouse` | `clickhouse` or `otlp` |
| `SPANGEN_RATE` / `-rate` | `100000` | target spans/s for this replica (`<=0` = unbounded) |
| `SPANGEN_RAMP` / `-ramp` | `30s` | linear ramp to target |
| `SPANGEN_DURATION` / `-duration` | `0` | stop after this long (`0` = until signalled) |
| `SPANGEN_MAX_SPANS` / `-max-spans` | `0` | stop after N spans |
| `SPANGEN_WORKERS` / `-workers` | `0` | generator goroutines (`0`=GOMAXPROCS) |
| `SPANGEN_CH_ENDPOINTS` | `localhost:9000` | comma-separated `host:9000` |
| `SPANGEN_CH_MODE` | `local` | `local` / `distributed` / `shard-roundrobin` |
| `SPANGEN_CH_TABLE` | `otel_traces` | target table (see table above) |
| `SPANGEN_CH_ASYNC` | `true` | `async_insert=1` |
| `SPANGEN_CH_WAIT_FOR_ASYNC` | `true` | `wait_for_async_insert` (1/0) |
| `SPANGEN_CH_BATCH_SIZE` | `5000` | rows per insert |
| `SPANGEN_CH_COMPRESSION` | `lz4` | `none` / `lz4` / `zstd` |
| `SPANGEN_OTLP_ENDPOINT` | `localhost:4317` | collector OTLP/gRPC |
| `SPANGEN_OTLP_CONNECTIONS` | `4` | gRPC connections (concurrency) |
| `SPANGEN_OTLP_SPANS_PER_REQUEST` | `2000` | spans/request (keep < 4MB) |
| `SPANGEN_METRICS_ADDR` | `:8888` | Prometheus endpoint |

### `async_insert` (you asked to use it)

Enabled by default via **connection settings** (`async_insert=1`,
`wait_for_async_insert=1`) on the native batch path — not the inline
`WithAsync()` API, which can't carry the `Map`/`Nested` columns. Server-side
batching keeps part counts down (less merge pressure, less skew). To probe the
ceiling, set `SPANGEN_CH_WAIT_FOR_ASYNC=false` (fire-and-forget — faster but
errors are not surfaced; ClickHouse docs advise against it for real workloads).
Optionally pin `SPANGEN_CH_ASYNC_MAX_DATA_SIZE` / `..._BUSY_TIMEOUT_MS`.

## Observability

Each replica serves Prometheus at `:8888/metrics` (and `/healthz`). Highlights:

- `spangen_achieved_rate_spans_per_sec` — measured rate; sum across the 10 pods ≈ 1M.
- `spangen_target_rate_spans_per_sec` — current target after ramp.
- `spangen_send_duration_seconds` — per-batch insert/export latency histogram.
- `spangen_spans_sent_total`, `spangen_batches_error_total`, `spangen_send_errors_total`,
  `spangen_dropped_spans_total`, `spangen_inflight_batches`.

It also logs a status line every 5s for operators without Prometheus.

## Verify the benchmark

1. **Smoke** (single node): apply `otel_traces_single.sql`, then
   `make run-local-ch` — watch `SELECT count() FROM otel.otel_traces` grow and
   `/metrics` show achieved ≈ target with zero errors.
2. **OTLP path**: `make run-local-otlp` against a collector using
   [`deploy/collector-config.yaml`](deploy/collector-config.yaml); confirm rows
   land in the **same** table.
3. **Skew + rate at scale**: deploy 10 replicas, then run
   [`deploy/ddl/verify.sql`](deploy/ddl/verify.sql) — per-shard counts within a
   few %, uniform per-service distribution, sane part counts, and per-minute
   ingest ≈ 1M/s.

## Throughput tuning (hitting 100k/s/replica)

- Span generation is CPU-bound. Give each pod **2–4 vCPU**; `-workers=0` uses all.
- Larger `-ch.batch-size` (e.g. 5k–20k) = fewer, bigger inserts. For OTLP raise
  `-otlp.connections` and keep `-otlp.spans-per-request` so a request stays < 4MB.
- If `spangen_achieved_rate` < target with low send latency, you're
  generation-bound → add workers/CPU. If send latency is high → ClickHouse or the
  collector is the bottleneck (that's the thing you're benchmarking).

## SDK notes / known issues handled

- **clickhouse-go/v2**: native batch API (required for `Map`/`Nested`);
  `async_insert` via settings; single compression codec (no double-compress).
- **OTLP/gRPC** (`collector/pdata` + `ptraceotlp`): spans are built as pdata and
  exported directly — **bypassing the SDK BatchSpanProcessor**, so no SDK
  queue-drop. The gRPC **4MB** message limit is handled by bounding
  spans-per-request and raising `MaxCallSendMsgSize`; partial-success responses
  are surfaced as errors.

## Layout

```
cmd/spangen/         entrypoint, flags/env, lifecycle
internal/config/     configuration (flags + env)
internal/model/      service catalog + realistic trace-tree generator (pdata)
internal/sink/       clickhouse (native batch) + otlp (gRPC) sinks
internal/generate/   worker pool + token-bucket pacer + ramp + samplers
internal/metrics/    Prometheus collectors + HTTP server
deploy/              Dockerfile, ddl/, deployment.yaml, collector-config.yaml
```
