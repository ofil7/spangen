package sink

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"spangen/internal/config"
)

// chSink writes spans directly to ClickHouse over the native protocol using the
// batch API (required for the Map and Nested columns). async_insert is enabled
// via connection settings, NOT the inline WithAsync path.
type chSink struct {
	conns     []driver.Conn
	insertSQL string
	next      atomic.Uint64
	sendTO    time.Duration
}

// NewClickHouse builds the sink according to cfg.CH.Mode:
//
//   - local / distributed: a single pooled connection addressed at all
//     endpoints; ClickHouse load-balances and (for distributed) shards by the
//     Distributed table's sharding key.
//   - shard-roundrobin: one connection per shard endpoint; batches are rotated
//     across shards (offset by replica index) for explicit even distribution.
func NewClickHouse(cfg *config.Config, replicaIndex int) (Sink, error) {
	ch := cfg.CH
	mkOpts := func(addrs []string) *clickhouse.Options {
		opt := &clickhouse.Options{
			Addr: addrs,
			Auth: clickhouse.Auth{
				Database: ch.Database,
				Username: ch.Username,
				Password: ch.Password,
			},
			Protocol:        chProtocol(ch.Protocol),
			DialTimeout:     ch.DialTimeout,
			MaxOpenConns:    ch.MaxOpenConns,
			MaxIdleConns:    ch.MaxOpenConns,
			BlockBufferSize: uint8(clampU8(ch.BlockBuffer)),
			Settings:        buildSettings(ch),
			Compression:     buildCompression(ch.Compression),
		}
		if ch.TLS {
			// Benchmark tool on a trusted, air-gapped network: skip CA verification
			// to avoid shipping the cluster's internal CA into the image.
			opt.TLS = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		}
		return opt
	}

	s := &chSink{
		insertSQL: buildInsertSQL(ch.Database, ch.Table),
		sendTO:    ch.SendTimeout,
	}

	switch ch.Mode {
	case "shard-roundrobin":
		for _, ep := range ch.Endpoints {
			conn, err := clickhouse.Open(mkOpts([]string{ep}))
			if err != nil {
				return nil, fmt.Errorf("open clickhouse %s: %w", ep, err)
			}
			s.conns = append(s.conns, conn)
		}
		// Offset the starting shard by replica index so the 10 replicas don't all
		// hammer shard 0 first.
		if len(s.conns) > 0 {
			s.next.Store(uint64(replicaIndex % len(s.conns)))
		}
	default: // local, distributed
		conn, err := clickhouse.Open(mkOpts(ch.Endpoints))
		if err != nil {
			return nil, fmt.Errorf("open clickhouse: %w", err)
		}
		s.conns = []driver.Conn{conn}
	}

	// Fail fast if the cluster is unreachable / table is missing.
	pingCtx, cancel := context.WithTimeout(context.Background(), ch.DialTimeout)
	defer cancel()
	if err := s.conns[0].Ping(pingCtx); err != nil {
		hint := ""
		if strings.Contains(err.Error(), "unexpected packet [72]") {
			hint = " — got an HTTP response on a native connection: you are using -ch.protocol=native against an HTTP port. Use -ch.protocol=http with port 8123, or point at the native port 9000"
		}
		return nil, fmt.Errorf("clickhouse ping (protocol=%s endpoints=%v): %w%s", ch.Protocol, ch.Endpoints, err, hint)
	}
	log.Printf("clickhouse connected: protocol=%s mode=%s endpoints=%v db=%s table=%s",
		ch.Protocol, ch.Mode, ch.Endpoints, ch.Database, ch.Table)
	return s, nil
}

func (s *chSink) Name() string { return "clickhouse" }

func (s *chSink) Close() error {
	var first error
	for _, c := range s.conns {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *chSink) pick() driver.Conn {
	if len(s.conns) == 1 {
		return s.conns[0]
	}
	i := s.next.Add(1)
	return s.conns[i%uint64(len(s.conns))]
}

func (s *chSink) Send(ctx context.Context, td ptrace.Traces) error {
	ctx, cancel := context.WithTimeout(ctx, s.sendTO)
	defer cancel()

	conn := s.pick()
	batch, err := conn.PrepareBatch(ctx, s.insertSQL, driver.WithReleaseConnection())
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resAttrs := attrMap(rs.Resource().Attributes())
		svcName := resAttrs["service.name"]
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scopeName := ss.Scope().Name()
			scopeVer := ss.Scope().Version()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				if err := appendSpan(batch, spans.At(k), resAttrs, svcName, scopeName, scopeVer); err != nil {
					_ = batch.Abort()
					return fmt.Errorf("append: %w", err)
				}
			}
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// appendSpan maps one pdata span to the exact column order of the OTel
// clickhouseexporter otel_traces schema.
func appendSpan(batch driver.Batch, sp ptrace.Span, resAttrs map[string]string, svcName, scopeName, scopeVer string) error {
	evTs, evNames, evAttrs := events(sp)
	lkTraceIDs, lkSpanIDs, lkStates, lkAttrs := links(sp)

	return batch.Append(
		sp.StartTimestamp().AsTime(),                                  // Timestamp
		traceHex(sp.TraceID()),                                        // TraceId
		spanHex(sp.SpanID()),                                          // SpanId
		spanHex(sp.ParentSpanID()),                                    // ParentSpanId
		sp.TraceState().AsRaw(),                                       // TraceState
		sp.Name(),                                                     // SpanName
		sp.Kind().String(),                                            // SpanKind
		svcName,                                                       // ServiceName
		resAttrs,                                                      // ResourceAttributes
		scopeName,                                                     // ScopeName
		scopeVer,                                                      // ScopeVersion
		attrMap(sp.Attributes()),                                      // SpanAttributes
		uint64(sp.EndTimestamp()-sp.StartTimestamp()),                 // Duration (ns)
		sp.Status().Code().String(),                                  // StatusCode
		sp.Status().Message(),                                         // StatusMessage
		evTs, evNames, evAttrs,                                        // Events.*
		lkTraceIDs, lkSpanIDs, lkStates, lkAttrs,                      // Links.*
	)
}

func events(sp ptrace.Span) (ts []time.Time, names []string, attrs []map[string]string) {
	ev := sp.Events()
	n := ev.Len()
	if n == 0 {
		return
	}
	ts = make([]time.Time, n)
	names = make([]string, n)
	attrs = make([]map[string]string, n)
	for i := 0; i < n; i++ {
		e := ev.At(i)
		ts[i] = e.Timestamp().AsTime()
		names[i] = e.Name()
		attrs[i] = attrMap(e.Attributes())
	}
	return
}

func links(sp ptrace.Span) (traceIDs, spanIDs, states []string, attrs []map[string]string) {
	lk := sp.Links()
	n := lk.Len()
	if n == 0 {
		return
	}
	traceIDs = make([]string, n)
	spanIDs = make([]string, n)
	states = make([]string, n)
	attrs = make([]map[string]string, n)
	for i := 0; i < n; i++ {
		l := lk.At(i)
		traceIDs[i] = traceHex(l.TraceID())
		spanIDs[i] = spanHex(l.SpanID())
		states[i] = l.TraceState().AsRaw()
		attrs[i] = attrMap(l.Attributes())
	}
	return
}

func attrMap(m pcommon.Map) map[string]string {
	out := make(map[string]string, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		out[k] = v.AsString()
		return true
	})
	return out
}

func traceHex(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func spanHex(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func buildInsertSQL(db, table string) string {
	return fmt.Sprintf("INSERT INTO `%s`.`%s` (\n"+`
		Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind,
		ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes,
		Duration, StatusCode, StatusMessage,
		Events.Timestamp, Events.Name, Events.Attributes,
		Links.TraceId, Links.SpanId, Links.TraceState, Links.Attributes)`, db, table)
}

func buildSettings(ch config.CHConfig) clickhouse.Settings {
	st := clickhouse.Settings{}
	if ch.Async {
		st["async_insert"] = 1
		if ch.WaitForAsync {
			st["wait_for_async_insert"] = 1
		} else {
			st["wait_for_async_insert"] = 0
		}
		if ch.AsyncMaxDataSize > 0 {
			st["async_insert_max_data_size"] = ch.AsyncMaxDataSize
		}
		if ch.AsyncBusyTimeoutMs > 0 {
			st["async_insert_busy_timeout_ms"] = ch.AsyncBusyTimeoutMs
		}
	}
	return st
}

// chProtocol selects the wire protocol. native = TCP (9000); http = 8123.
func chProtocol(p string) clickhouse.Protocol {
	if p == "http" {
		return clickhouse.HTTP
	}
	return clickhouse.Native
}

func buildCompression(method string) *clickhouse.Compression {
	switch method {
	case "zstd":
		return &clickhouse.Compression{Method: clickhouse.CompressionZSTD}
	case "gzip": // HTTP only
		return &clickhouse.Compression{Method: clickhouse.CompressionGZIP}
	case "none":
		return &clickhouse.Compression{Method: clickhouse.CompressionNone}
	default: // lz4 (valid on both native and http)
		return &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}
}

func clampU8(n int) int {
	if n < 1 {
		return 1
	}
	if n > 255 {
		return 255
	}
	return n
}
