package sink

import (
	"context"
	"crypto/tls"
	"database/sql"
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

// chSink writes spans directly to ClickHouse using the batch API (required for
// the Map and Nested columns). async_insert is enabled via connection settings,
// NOT the inline WithAsync path.
//
// Two protocols are supported and use DIFFERENT client APIs, because
// clickhouse-go only speaks HTTP through its database/sql interface:
//
//   - native (TCP 9000): clickhouse.Open() -> driver.Conn, PrepareBatch/Append/Send.
//   - http   (8123):     clickhouse.OpenDB() -> *sql.DB, Begin/Prepare/Exec/Commit.
//
// clickhouse.Open() ignores Options.Protocol and always dials native, so the
// HTTP path MUST go through OpenDB — otherwise a native handshake is attempted
// against the HTTP port (server replies with HTTP, producing the classic
// "unexpected packet [72]" handshake error). Both APIs carry the identical
// 22-column INSERT (Map/Nested), Settings, and Compression.
type chSink struct {
	conns     []chConn
	insertSQL string
	next      atomic.Uint64
	sendTO    time.Duration
}

// chConn abstracts a single pooled connection over either protocol. newBatch
// starts one insert batch; the span-mapping in Send is shared across both.
type chConn interface {
	Ping(ctx context.Context) error
	Close() error
	newBatch(ctx context.Context, insertSQL string) (chBatch, error)
}

// chBatch is one in-progress insert. Append adds a row (column order per
// buildInsertSQL); Send commits; Abort discards on error.
type chBatch interface {
	Append(args ...any) error
	Send() error
	Abort() error
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

	// openOne builds one connection over the configured protocol. native returns
	// an error eagerly from Open(); http (OpenDB) defers errors to Ping.
	openOne := func(addrs []string) (chConn, error) {
		opt := mkOpts(addrs)
		if chProtocol(ch.Protocol) == clickhouse.HTTP {
			// OpenDB rejects pool sizing in Options — it must be set on the
			// *sql.DB instead. Zero them here and apply via Set*Conns below.
			opt.MaxOpenConns, opt.MaxIdleConns = 0, 0
			db := clickhouse.OpenDB(opt)
			db.SetMaxOpenConns(clampMin1(ch.MaxOpenConns))
			db.SetMaxIdleConns(clampMin1(ch.MaxOpenConns))
			return &httpConn{db: db}, nil
		}
		conn, err := clickhouse.Open(opt)
		if err != nil {
			return nil, err
		}
		return &nativeConn{conn: conn}, nil
	}

	s := &chSink{
		insertSQL: buildInsertSQL(ch.Database, ch.Table),
		sendTO:    ch.SendTimeout,
	}

	switch ch.Mode {
	case "shard-roundrobin":
		for _, ep := range ch.Endpoints {
			c, err := openOne([]string{ep})
			if err != nil {
				return nil, fmt.Errorf("open clickhouse %s: %w", ep, err)
			}
			s.conns = append(s.conns, c)
		}
		// Offset the starting shard by replica index so the 10 replicas don't all
		// hammer shard 0 first.
		if len(s.conns) > 0 {
			s.next.Store(uint64(replicaIndex % len(s.conns)))
		}
	default: // local, distributed
		c, err := openOne(ch.Endpoints)
		if err != nil {
			return nil, fmt.Errorf("open clickhouse: %w", err)
		}
		s.conns = []chConn{c}
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

func (s *chSink) pick() chConn {
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
	batch, err := conn.newBatch(ctx, s.insertSQL)
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

// --- native protocol (clickhouse.Open / driver.Conn) ---

type nativeConn struct{ conn driver.Conn }

func (c *nativeConn) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }
func (c *nativeConn) Close() error                   { return c.conn.Close() }

func (c *nativeConn) newBatch(ctx context.Context, insertSQL string) (chBatch, error) {
	b, err := c.conn.PrepareBatch(ctx, insertSQL, driver.WithReleaseConnection())
	if err != nil {
		return nil, err
	}
	return nativeBatch{b: b}, nil
}

type nativeBatch struct{ b driver.Batch }

func (n nativeBatch) Append(args ...any) error { return n.b.Append(args...) }
func (n nativeBatch) Send() error              { return n.b.Send() }
func (n nativeBatch) Abort() error             { return n.b.Abort() }

// --- http protocol (clickhouse.OpenDB / database/sql) ---

type httpConn struct{ db *sql.DB }

func (c *httpConn) Ping(ctx context.Context) error { return c.db.PingContext(ctx) }
func (c *httpConn) Close() error                   { return c.db.Close() }

func (c *httpConn) newBatch(ctx context.Context, insertSQL string) (chBatch, error) {
	// clickhouse-go's std driver batches rows on a Tx: Begin -> Prepare(INSERT)
	// -> Exec per row -> Commit. This works over HTTP and carries Map/Nested
	// columns identically to the native batch.
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return &httpBatch{ctx: ctx, tx: tx, stmt: stmt}, nil
}

type httpBatch struct {
	ctx  context.Context
	tx   *sql.Tx
	stmt *sql.Stmt
}

func (h *httpBatch) Append(args ...any) error {
	_, err := h.stmt.ExecContext(h.ctx, args...)
	return err
}

func (h *httpBatch) Send() error {
	defer h.stmt.Close()
	return h.tx.Commit()
}

func (h *httpBatch) Abort() error {
	defer h.stmt.Close()
	return h.tx.Rollback()
}

// appendSpan maps one pdata span to the exact column order of the OTel
// clickhouseexporter otel_traces schema.
func appendSpan(batch chBatch, sp ptrace.Span, resAttrs map[string]string, svcName, scopeName, scopeVer string) error {
	evTs, evNames, evAttrs := events(sp)
	lkTraceIDs, lkSpanIDs, lkStates, lkAttrs := links(sp)

	return batch.Append(
		sp.StartTimestamp().AsTime(),                  // Timestamp
		traceHex(sp.TraceID()),                        // TraceId
		spanHex(sp.SpanID()),                          // SpanId
		spanHex(sp.ParentSpanID()),                    // ParentSpanId
		sp.TraceState().AsRaw(),                       // TraceState
		sp.Name(),                                     // SpanName
		sp.Kind().String(),                            // SpanKind
		svcName,                                       // ServiceName
		resAttrs,                                      // ResourceAttributes
		scopeName,                                     // ScopeName
		scopeVer,                                      // ScopeVersion
		attrMap(sp.Attributes()),                      // SpanAttributes
		uint64(sp.EndTimestamp()-sp.StartTimestamp()), // Duration (ns)
		sp.Status().Code().String(),                   // StatusCode
		sp.Status().Message(),                         // StatusMessage
		evTs, evNames, evAttrs,                        // Events.*
		lkTraceIDs, lkSpanIDs, lkStates, lkAttrs,      // Links.*
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

func clampMin1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
