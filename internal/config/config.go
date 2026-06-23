// Package config parses the generator's configuration from command-line flags
// with environment-variable fallbacks. In Kubernetes/OpenShift the env vars are
// the primary configuration channel; flags are convenient for local runs and
// always override the corresponding env default.
package config

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-resolved runtime configuration.
type Config struct {
	Sink         string // "clickhouse" | "otlp"
	ReplicaIndex int    // pod ordinal; offsets shard rotation
	Seed         uint64 // base PRNG seed for the (shared) topology
	HostID       uint64 // strong per-pod hash; salts worker seeds so ID streams never collide

	Metrics MetricsConfig
	Gen     GenConfig
	CH      CHConfig
	OTLP    OTLPConfig
}

// MetricsConfig controls the Prometheus endpoint.
type MetricsConfig struct {
	Addr string // e.g. ":8888"
}

// GenConfig controls the data shape and the rate/duration of the run.
type GenConfig struct {
	Rate             float64       // target spans/sec for THIS replica; <=0 means unbounded (max throughput)
	Ramp             time.Duration // linear ramp from ~0 to Rate; 0 means start at full rate
	Duration         time.Duration // stop after this long; 0 means run until signalled
	MaxSpans         uint64        // stop after this many spans; 0 means unlimited
	Workers          int           // generator goroutines; 0 means GOMAXPROCS
	Services         int           // size of the synthetic service catalog
	SpansPerTraceMin int
	SpansPerTraceMax int
	ErrorRate        float64 // fraction of spans with ERROR status (0..1)
	EventRate        float64 // fraction of spans carrying at least one event (0..1)
}

// CHConfig controls the direct ClickHouse native sink.
type CHConfig struct {
	Endpoints    []string // host:port list (9000 native / 8123 http, or TLS 9440 / 8443)
	Database     string
	Table        string
	Protocol     string // "native" (TCP 9000) | "http" (8123)
	Mode         string // "local" | "distributed" | "shard-roundrobin"
	Username     string
	Password     string
	TLS          bool
	Compression  string // "none" | "lz4" | "zstd"
	BatchSize    int    // rows per batch (per Send)
	MaxOpenConns int
	BlockBuffer  int

	Async              bool // sets async_insert=1
	WaitForAsync       bool // sets wait_for_async_insert (1 if true, 0 if false)
	AsyncMaxDataSize   int  // optional async_insert_max_data_size (bytes); 0 = server default
	AsyncBusyTimeoutMs int  // optional async_insert_busy_timeout_ms; 0 = server default

	DialTimeout time.Duration
	SendTimeout time.Duration
}

// OTLPConfig controls the OTLP/gRPC sink (to existing collectors).
type OTLPConfig struct {
	Endpoint        string // host:4317
	Insecure        bool   // plaintext (no TLS)
	Headers         map[string]string
	Gzip            bool
	SpansPerRequest int // also the effective batch size in OTLP mode (keep request < 4MB)
	Connections     int // number of gRPC client connections (concurrency)
	Timeout         time.Duration
}

var podOrdinalRe = regexp.MustCompile(`(\d+)$`)

// Parse builds a Config from os.Args and the environment.
func Parse() (*Config, error) {
	c := &Config{}

	flag.StringVar(&c.Sink, "sink", env("SPANGEN_SINK", "clickhouse"), "sink: clickhouse|otlp")
	flag.IntVar(&c.ReplicaIndex, "replica-index", envInt("SPANGEN_REPLICA_INDEX", detectReplicaIndex()), "replica ordinal (offsets seed + shard rotation)")
	flag.Uint64Var(&c.Seed, "seed", envUint("SPANGEN_SEED", 0x9E3779B97F4A7C15), "base PRNG seed")

	flag.StringVar(&c.Metrics.Addr, "metrics.addr", env("SPANGEN_METRICS_ADDR", ":8888"), "Prometheus listen address")

	// Generation
	flag.Float64Var(&c.Gen.Rate, "rate", envFloat("SPANGEN_RATE", 100000), "target spans/sec for this replica (<=0 = unbounded)")
	flag.DurationVar(&c.Gen.Ramp, "ramp", envDur("SPANGEN_RAMP", 30*time.Second), "linear ramp-up to target rate")
	flag.DurationVar(&c.Gen.Duration, "duration", envDur("SPANGEN_DURATION", 0), "run duration (0 = until signalled)")
	flag.Uint64Var(&c.Gen.MaxSpans, "max-spans", envUint("SPANGEN_MAX_SPANS", 0), "stop after N spans (0 = unlimited)")
	flag.IntVar(&c.Gen.Workers, "workers", envInt("SPANGEN_WORKERS", 0), "generator goroutines (0 = GOMAXPROCS)")
	flag.IntVar(&c.Gen.Services, "services", envInt("SPANGEN_SERVICES", 30), "number of synthetic services")
	flag.IntVar(&c.Gen.SpansPerTraceMin, "spans-per-trace-min", envInt("SPANGEN_SPANS_PER_TRACE_MIN", 5), "min spans per trace")
	flag.IntVar(&c.Gen.SpansPerTraceMax, "spans-per-trace-max", envInt("SPANGEN_SPANS_PER_TRACE_MAX", 20), "max spans per trace")
	flag.Float64Var(&c.Gen.ErrorRate, "error-rate", envFloat("SPANGEN_ERROR_RATE", 0.015), "fraction of spans with ERROR status")
	flag.Float64Var(&c.Gen.EventRate, "event-rate", envFloat("SPANGEN_EVENT_RATE", 0.10), "fraction of spans carrying events")

	// ClickHouse
	var chEndpoints string
	flag.StringVar(&chEndpoints, "ch.endpoints", env("SPANGEN_CH_ENDPOINTS", "localhost:9000"), "comma-separated host:port list (native 9000 / http 8123)")
	flag.StringVar(&c.CH.Database, "ch.database", env("SPANGEN_CH_DATABASE", "otel"), "ClickHouse database")
	flag.StringVar(&c.CH.Table, "ch.table", env("SPANGEN_CH_TABLE", "otel_traces"), "target table")
	flag.StringVar(&c.CH.Protocol, "ch.protocol", env("SPANGEN_CH_PROTOCOL", "native"), "native|http (native=TCP 9000, http=8123)")
	flag.StringVar(&c.CH.Mode, "ch.mode", env("SPANGEN_CH_MODE", "local"), "local|distributed|shard-roundrobin")
	flag.StringVar(&c.CH.Username, "ch.username", env("SPANGEN_CH_USERNAME", "default"), "username")
	flag.StringVar(&c.CH.Password, "ch.password", env("SPANGEN_CH_PASSWORD", ""), "password")
	flag.BoolVar(&c.CH.TLS, "ch.tls", envBool("SPANGEN_CH_TLS", false), "enable TLS to ClickHouse")
	flag.StringVar(&c.CH.Compression, "ch.compression", env("SPANGEN_CH_COMPRESSION", "lz4"), "none|lz4|zstd")
	flag.IntVar(&c.CH.BatchSize, "ch.batch-size", envInt("SPANGEN_CH_BATCH_SIZE", 5000), "rows per batch")
	flag.IntVar(&c.CH.MaxOpenConns, "ch.max-conns", envInt("SPANGEN_CH_MAX_CONNS", 16), "max open connections per endpoint")
	flag.IntVar(&c.CH.BlockBuffer, "ch.block-buffer", envInt("SPANGEN_CH_BLOCK_BUFFER", 16), "native block buffer size")
	flag.BoolVar(&c.CH.Async, "ch.async", envBool("SPANGEN_CH_ASYNC", true), "enable async_insert")
	flag.BoolVar(&c.CH.WaitForAsync, "ch.wait-for-async", envBool("SPANGEN_CH_WAIT_FOR_ASYNC", true), "wait_for_async_insert (1/0)")
	flag.IntVar(&c.CH.AsyncMaxDataSize, "ch.async-max-data-size", envInt("SPANGEN_CH_ASYNC_MAX_DATA_SIZE", 0), "async_insert_max_data_size bytes (0=server default)")
	flag.IntVar(&c.CH.AsyncBusyTimeoutMs, "ch.async-busy-timeout-ms", envInt("SPANGEN_CH_ASYNC_BUSY_TIMEOUT_MS", 0), "async_insert_busy_timeout_ms (0=server default)")
	flag.DurationVar(&c.CH.DialTimeout, "ch.dial-timeout", envDur("SPANGEN_CH_DIAL_TIMEOUT", 10*time.Second), "dial timeout")
	flag.DurationVar(&c.CH.SendTimeout, "ch.send-timeout", envDur("SPANGEN_CH_SEND_TIMEOUT", 30*time.Second), "per-batch send timeout")

	// OTLP
	flag.StringVar(&c.OTLP.Endpoint, "otlp.endpoint", env("SPANGEN_OTLP_ENDPOINT", "localhost:4317"), "collector OTLP/gRPC host:port")
	flag.BoolVar(&c.OTLP.Insecure, "otlp.insecure", envBool("SPANGEN_OTLP_INSECURE", true), "plaintext (no TLS)")
	var otlpHeaders string
	flag.StringVar(&otlpHeaders, "otlp.headers", env("SPANGEN_OTLP_HEADERS", ""), "comma-separated k=v gRPC metadata")
	flag.BoolVar(&c.OTLP.Gzip, "otlp.gzip", envBool("SPANGEN_OTLP_GZIP", false), "gzip-compress OTLP requests")
	flag.IntVar(&c.OTLP.SpansPerRequest, "otlp.spans-per-request", envInt("SPANGEN_OTLP_SPANS_PER_REQUEST", 2000), "spans per OTLP request (keep <4MB)")
	flag.IntVar(&c.OTLP.Connections, "otlp.connections", envInt("SPANGEN_OTLP_CONNECTIONS", 4), "number of gRPC connections")
	flag.DurationVar(&c.OTLP.Timeout, "otlp.timeout", envDur("SPANGEN_OTLP_TIMEOUT", 30*time.Second), "per-request timeout")

	flag.Parse()

	c.CH.Endpoints = splitCSV(chEndpoints)
	c.OTLP.Headers = parseKV(otlpHeaders)
	c.HostID = hostID()

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	switch c.Sink {
	case "clickhouse", "otlp":
	default:
		return fmt.Errorf("invalid -sink %q (want clickhouse|otlp)", c.Sink)
	}
	if c.Sink == "clickhouse" {
		switch c.CH.Protocol {
		case "native", "http":
		default:
			return fmt.Errorf("invalid -ch.protocol %q (want native|http)", c.CH.Protocol)
		}
		switch c.CH.Mode {
		case "local", "distributed", "shard-roundrobin":
		default:
			return fmt.Errorf("invalid -ch.mode %q", c.CH.Mode)
		}
		if len(c.CH.Endpoints) == 0 {
			return fmt.Errorf("-ch.endpoints is empty")
		}
		switch c.CH.Compression {
		case "none", "lz4", "zstd", "gzip":
		default:
			return fmt.Errorf("invalid -ch.compression %q (want none|lz4|zstd|gzip)", c.CH.Compression)
		}
		if c.CH.Compression == "gzip" && c.CH.Protocol != "http" {
			return fmt.Errorf("-ch.compression=gzip is only valid with -ch.protocol=http")
		}
	}
	if c.Sink == "otlp" && c.OTLP.Endpoint == "" {
		return fmt.Errorf("-otlp.endpoint is empty")
	}
	if c.Gen.SpansPerTraceMin < 1 || c.Gen.SpansPerTraceMax < c.Gen.SpansPerTraceMin {
		return fmt.Errorf("invalid spans-per-trace range [%d,%d]", c.Gen.SpansPerTraceMin, c.Gen.SpansPerTraceMax)
	}
	if c.Gen.Services < 1 {
		return fmt.Errorf("-services must be >= 1")
	}
	return nil
}

// EffectiveBatchSize is the number of spans the engine assembles per Send for
// the active sink. For OTLP this also bounds the gRPC message size.
func (c *Config) EffectiveBatchSize() int {
	if c.Sink == "otlp" {
		return c.OTLP.SpansPerRequest
	}
	return c.CH.BatchSize
}

// ---- env helpers ----

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envUint(k string, def uint64) uint64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 0, 64); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, ok := os.LookupEnv(k); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseKV(s string) map[string]string {
	m := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if i := strings.IndexByte(p, '='); i > 0 {
			m[strings.TrimSpace(p[:i])] = strings.TrimSpace(p[i+1:])
		}
	}
	return m
}

// detectReplicaIndex derives a stable per-pod ordinal from HOSTNAME. For a
// StatefulSet the trailing number is the ordinal; for a Deployment we fall back
// to a hash of the pod name so the 10 replicas still differ.
func detectReplicaIndex() int {
	h, _ := os.Hostname()
	if h == "" {
		h = env("HOSTNAME", "")
	}
	if m := podOrdinalRe.FindStringSubmatch(h); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			return n
		}
	}
	// FNV-1a hash fallback.
	var hsh uint32 = 2166136261
	for i := 0; i < len(h); i++ {
		hsh ^= uint32(h[i])
		hsh *= 16777619
	}
	return int(hsh % 1024)
}

// hostID returns a strong 64-bit identifier unique to this pod. Pod names are
// unique in Kubernetes, so an FNV-1a-64 hash of the hostname gives every replica
// a distinct value used to salt worker RNG seeds — guaranteeing no two pods ever
// emit the same TraceId/SpanId stream (even when their replica index collides).
// If the hostname is unavailable, fall back to a random value.
func hostID() uint64 {
	h, _ := os.Hostname()
	if h == "" {
		h = env("HOSTNAME", "")
	}
	if h == "" {
		var b [8]byte
		if _, err := crand.Read(b[:]); err == nil {
			return binary.LittleEndian.Uint64(b[:])
		}
		return uint64(time.Now().UnixNano())
	}
	var hsh uint64 = 1469598103934665603 // FNV-1a-64 offset basis
	for i := 0; i < len(h); i++ {
		hsh ^= uint64(h[i])
		hsh *= 1099511628211 // FNV-1a-64 prime
	}
	return hsh
}
