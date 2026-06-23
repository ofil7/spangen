package model

import (
	"encoding/binary"
	"fmt"
	mrand "math/rand/v2"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Generator renders traces for one worker. It is NOT safe for concurrent use;
// each worker owns its own Generator (and thus its own RNG stream), which keeps
// span/trace IDs unique and uncorrelated across workers and replicas.
type Generator struct {
	cat *Catalog
	rng *mrand.Rand

	spansPerTraceMin int
	spansPerTraceMax int
	errorRate        float64
	eventRate        float64

	// per-batch grouping of spans by service into ResourceSpans
	scopes map[int]ptrace.SpanSlice

	// lastSpanID holds the SpanID minted by the most recent emitSpan, so a
	// parent can be linked to its children without threading it through the
	// recursive call signatures.
	lastSpanID pcommon.SpanID
}

// NewGenerator seeds a worker generator. seed should be unique per (replica,
// worker) so streams never collide.
func NewGenerator(cat *Catalog, seed uint64, spMin, spMax int, errorRate, eventRate float64) *Generator {
	return &Generator{
		cat:              cat,
		rng:              mrand.New(mrand.NewPCG(seed, ^seed)),
		spansPerTraceMin: spMin,
		spansPerTraceMax: spMax,
		errorRate:        errorRate,
		eventRate:        eventRate,
		scopes:           make(map[int]ptrace.SpanSlice, 64),
	}
}

// FillBatch generates whole traces into `td` until at least `target` spans have
// been produced, grouping spans by their owning service into ResourceSpans.
// Returns the number of spans written. `td` should be empty on entry.
func (g *Generator) FillBatch(td ptrace.Traces, target int) int {
	clear(g.scopes)
	count := 0
	for count < target {
		count += g.genTrace(td, target-count)
	}
	return count
}

// scopeFor returns (creating if needed) the SpanSlice for a service within the
// current batch, lazily materialising one ResourceSpans+ScopeSpans per service.
func (g *Generator) scopeFor(td ptrace.Traces, svcIdx int) ptrace.SpanSlice {
	if ss, ok := g.scopes[svcIdx]; ok {
		return ss
	}
	s := &g.cat.Services[svcIdx]
	rs := td.ResourceSpans().AppendEmpty()
	ra := rs.Resource().Attributes()
	ra.EnsureCapacity(len(s.resourceAttrs))
	for k, v := range s.resourceAttrs {
		ra.PutStr(k, v)
	}
	scope := rs.ScopeSpans().AppendEmpty()
	scope.Scope().SetName(g.cat.ScopeName)
	scope.Scope().SetVersion(g.cat.ScopeVer)
	spans := scope.Spans()
	g.scopes[svcIdx] = spans
	return spans
}

// genTrace builds one trace tree (bounded by budget) and returns spans emitted.
func (g *Generator) genTrace(td ptrace.Traces, budget int) int {
	var traceID pcommon.TraceID
	g.fillTraceID(traceID[:])

	maxSpans := g.spansPerTraceMin + g.rng.IntN(g.spansPerTraceMax-g.spansPerTraceMin+1)
	if maxSpans > budget {
		maxSpans = budget
	}
	if maxSpans < 1 {
		maxSpans = 1
	}

	// Root entry span.
	root := &g.cat.Services[g.rng.IntN(len(g.cat.Services))]
	entry := root.entryOps[g.rng.IntN(len(root.entryOps))]
	start := time.Now().Add(-time.Duration(g.rng.IntN(2000)) * time.Millisecond)
	dur := g.sampleDur(entry)

	remaining := maxSpans - 1
	var emptyParent pcommon.SpanID
	g.emitSpan(td, traceID, emptyParent, root, entry, start, dur)

	// Recursively add children sharing the trace, depth-first within budget.
	rootSpanID := g.lastSpanID // emitSpan records the id it generated
	g.addChildren(td, traceID, rootSpanID, root.idx, start, dur, &remaining, 1)
	return maxSpans - remaining
}

// addChildren attaches child spans beneath a parent until the per-trace budget
// (*remaining) is exhausted or the depth cap is hit.
func (g *Generator) addChildren(td ptrace.Traces, traceID pcommon.TraceID, parentID pcommon.SpanID, svcIdx int, pStart time.Time, pDur time.Duration, remaining *int, depth int) {
	if *remaining <= 0 || depth > 6 {
		return
	}
	s := &g.cat.Services[svcIdx]
	if len(s.childOps) == 0 {
		return
	}
	nChildren := 1 + g.rng.IntN(3)
	for i := 0; i < nChildren && *remaining > 0; i++ {
		op := s.childOps[g.rng.IntN(len(s.childOps))]
		// child starts within the first 70% of the parent's duration
		offset := time.Duration(float64(pDur) * 0.7 * g.rng.Float64())
		cStart := pStart.Add(offset)
		cDur := g.sampleDur(op)
		if rem := pDur - offset; cDur > rem && rem > 0 {
			cDur = rem
		}

		*remaining--
		g.emitSpan(td, traceID, parentID, s, op, cStart, cDur)
		clientSpanID := g.lastSpanID

		if op.peer >= 0 && *remaining > 0 {
			// Remote call: emit the downstream SERVER span in the peer service,
			// as a child of this CLIENT span, then recurse into the peer.
			peer := &g.cat.Services[op.peer]
			srvOp := serverCounterpart(op, peer)
			sStart := cStart.Add(time.Duration(float64(cDur) * 0.1))
			sDur := time.Duration(float64(cDur) * 0.85)
			*remaining--
			g.emitSpan(td, traceID, clientSpanID, peer, srvOp, sStart, sDur)
			peerServerID := g.lastSpanID
			g.addChildren(td, traceID, peerServerID, peer.idx, sStart, sDur, remaining, depth+1)
		} else if op.typ == opInternal && *remaining > 0 && depth < 3 {
			g.addChildren(td, traceID, clientSpanID, svcIdx, cStart, cDur, remaining, depth+1)
		}
	}
}

// serverCounterpart derives the callee's SERVER span from a caller's CLIENT op.
func serverCounterpart(client operation, peer *service) operation {
	if client.typ == opRPCClient {
		return operation{
			name: client.rpcService + "/" + client.rpcMethod, typ: opRPCServer, kind: ptrace.SpanKindServer,
			rpcService: client.rpcService, rpcMethod: client.rpcMethod, peer: -1,
			muLog: client.muLog - 0.3, sigmaLog: client.sigmaLog,
		}
	}
	return operation{
		name: client.method + " " + client.route, typ: opHTTPServer, kind: ptrace.SpanKindServer,
		method: client.method, route: client.route, peer: -1,
		muLog: client.muLog - 0.3, sigmaLog: client.sigmaLog,
	}
}

func (g *Generator) emitSpan(td ptrace.Traces, traceID pcommon.TraceID, parent pcommon.SpanID, s *service, op operation, start time.Time, dur time.Duration) {
	spans := g.scopeFor(td, s.idx)
	sp := spans.AppendEmpty()

	var spanID pcommon.SpanID
	g.fillSpanID(spanID[:])
	g.lastSpanID = spanID

	sp.SetTraceID(traceID)
	sp.SetSpanID(spanID)
	if !parent.IsEmpty() {
		sp.SetParentSpanID(parent)
	}
	sp.SetName(op.name)
	sp.SetKind(op.kind)
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(dur)))

	isErr := g.rng.Float64() < g.errorRate
	g.applyAttributes(sp, op, isErr)

	if isErr {
		sp.Status().SetCode(ptrace.StatusCodeError)
		sp.Status().SetMessage("operation failed")
		g.addExceptionEvent(sp, start.Add(dur/2))
	} else {
		sp.Status().SetCode(ptrace.StatusCodeUnset)
		if g.rng.Float64() < g.eventRate {
			g.addInfoEvent(sp, op, start.Add(dur/3))
		}
	}
}

func (g *Generator) applyAttributes(sp ptrace.Span, op operation, isErr bool) {
	a := sp.Attributes()
	switch op.typ {
	case opHTTPServer, opHTTPClient:
		a.PutStr("http.request.method", op.method)
		a.PutStr("url.path", op.route)
		a.PutStr("url.scheme", "http")
		a.PutStr("network.protocol.version", "1.1")
		a.PutStr("server.address", peerHost(op))
		a.PutInt("server.port", 8080)
		a.PutInt("http.response.status_code", int64(g.statusCode(isErr)))
		if op.typ == opHTTPServer {
			a.PutStr("client.address", g.clientIP())
			a.PutStr("user_agent.original", g.userAgent())
			a.PutStr("url.full", "http://"+peerHost(op)+op.route)
		}
	case opDBClient:
		a.PutStr("db.system", op.dbSystem)
		a.PutStr("db.namespace", op.dbName)
		a.PutStr("db.operation.name", op.dbOp)
		a.PutStr("db.collection.name", op.collection)
		a.PutStr("server.address", op.dbSystem+".db.svc.cluster.local")
		a.PutInt("server.port", dbPort(op.dbSystem))
		a.PutStr("db.query.text", fmt.Sprintf("%s FROM %s WHERE id = ?", op.dbOp, op.collection))
	case opRPCServer, opRPCClient:
		a.PutStr("rpc.system", "grpc")
		a.PutStr("rpc.service", op.rpcService)
		a.PutStr("rpc.method", op.rpcMethod)
		a.PutInt("rpc.grpc.status_code", int64(g.grpcStatus(isErr)))
	case opMsgProducer, opMsgConsumer:
		a.PutStr("messaging.system", "kafka")
		a.PutStr("messaging.destination.name", op.destination)
		a.PutStr("messaging.operation", msgOp(op.typ))
		a.PutInt("messaging.message.body.size", int64(256+g.rng.IntN(8192)))
	case opInternal:
		a.PutStr("code.namespace", op.name)
		a.PutStr("thread.name", fmt.Sprintf("worker-%d", g.rng.IntN(16)))
	}
}

func (g *Generator) addExceptionEvent(sp ptrace.Span, t time.Time) {
	ev := sp.Events().AppendEmpty()
	ev.SetName("exception")
	ev.SetTimestamp(pcommon.NewTimestampFromTime(t))
	ea := ev.Attributes()
	ea.PutStr("exception.type", exceptionTypes[g.rng.IntN(len(exceptionTypes))])
	ea.PutStr("exception.message", "request processing failed")
	ea.PutBool("exception.escaped", false)
}

func (g *Generator) addInfoEvent(sp ptrace.Span, op operation, t time.Time) {
	ev := sp.Events().AppendEmpty()
	ev.SetName("processing")
	ev.SetTimestamp(pcommon.NewTimestampFromTime(t))
	ev.Attributes().PutStr("stage", infoStages[g.rng.IntN(len(infoStages))])
}

// ---- bounded value pools (keep cardinality realistic, avoid skew) ----

var exceptionTypes = []string{"java.lang.RuntimeException", "context.DeadlineExceeded", "sql.ErrNoRows", "net.OpError", "ValidationError"}
var infoStages = []string{"validate", "enrich", "persist", "publish", "respond"}
var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15)",
	"curl/8.4.0", "okhttp/4.12.0", "Go-http-client/2.0", "PostmanRuntime/7.36.0",
}

func (g *Generator) statusCode(isErr bool) int {
	if isErr {
		return []int{500, 502, 503, 504, 400, 404}[g.rng.IntN(6)]
	}
	return []int{200, 200, 200, 201, 204, 304}[g.rng.IntN(6)]
}

func (g *Generator) grpcStatus(isErr bool) int {
	if isErr {
		return []int{2, 4, 13, 14}[g.rng.IntN(4)] // UNKNOWN, DEADLINE, INTERNAL, UNAVAILABLE
	}
	return 0 // OK
}

func (g *Generator) clientIP() string {
	return fmt.Sprintf("10.%d.%d.%d", g.rng.IntN(4), g.rng.IntN(256), g.rng.IntN(256))
}

func (g *Generator) userAgent() string { return userAgents[g.rng.IntN(len(userAgents))] }

func (g *Generator) sampleDur(op operation) time.Duration {
	ns := math.Exp(op.muLog + op.sigmaLog*g.rng.NormFloat64())
	if ns < 1000 {
		ns = 1000 // floor at 1µs
	}
	return time.Duration(ns)
}

func (g *Generator) fillTraceID(b []byte) {
	binary.LittleEndian.PutUint64(b[0:8], g.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], g.rng.Uint64())
}

func (g *Generator) fillSpanID(b []byte) {
	binary.LittleEndian.PutUint64(b[0:8], g.rng.Uint64())
}

func peerHost(op operation) string {
	if op.route != "" {
		return "svc.shop.svc.cluster.local"
	}
	return "localhost"
}

func dbPort(system string) int64 {
	switch system {
	case "postgresql":
		return 5432
	case "mysql":
		return 3306
	case "redis":
		return 6379
	case "mongodb":
		return 27017
	case "cassandra":
		return 9042
	}
	return 0
}

func msgOp(t opType) string {
	if t == opMsgProducer {
		return "publish"
	}
	return "process"
}
