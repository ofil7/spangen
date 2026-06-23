// Package model builds a realistic, bounded-cardinality synthetic microservice
// topology and renders traces into OpenTelemetry pdata. The same pdata feeds
// both sinks (OTLP and direct ClickHouse), guaranteeing the two ingest paths
// carry identical data.
package model

import (
	"fmt"
	mrand "math/rand/v2"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// opType classifies how a span's attributes are rendered (semconv).
type opType int

const (
	opHTTPServer opType = iota
	opHTTPClient
	opDBClient
	opRPCServer
	opRPCClient
	opInternal
	opMsgProducer
	opMsgConsumer
)

// operation is a single kind of work a service performs.
type operation struct {
	name     string // span name, e.g. "GET /api/orders"
	typ      opType
	kind     ptrace.SpanKind
	muLog    float64 // log-normal latency params (nanoseconds domain)
	sigmaLog float64

	// type-specific descriptors (bounded value sets keep cardinality realistic)
	method     string
	route      string
	dbSystem   string
	dbName     string
	dbOp       string
	collection string
	rpcService string
	rpcMethod  string
	destination string

	// peer is the index of a downstream service this op calls, or -1.
	peer int
}

// service is one node in the synthetic topology.
type service struct {
	idx           int
	name          string
	namespace     string
	version       string
	host          string // server.address
	resourceAttrs map[string]string

	entryOps []operation // SERVER/CONSUMER entry points (trace roots)
	childOps []operation // work performed beneath an entry (client/db/internal/rpc)
}

// Catalog is the immutable topology shared by all worker generators.
type Catalog struct {
	Services   []service
	ScopeName  string
	ScopeVer   string
}

var serviceNames = []string{
	"frontend", "api-gateway", "checkout", "cart", "catalog", "payment",
	"shipping", "inventory", "user", "auth", "recommendation", "search",
	"pricing", "promotion", "order", "notification", "email", "fraud",
	"analytics", "loyalty", "wishlist", "review", "media", "session",
}

var httpRoutes = []struct {
	method, route string
}{
	{"GET", "/api/products"}, {"GET", "/api/products/{id}"}, {"POST", "/api/cart"},
	{"GET", "/api/cart"}, {"POST", "/api/checkout"}, {"POST", "/api/orders"},
	{"GET", "/api/orders/{id}"}, {"POST", "/api/payments"}, {"GET", "/api/users/{id}"},
	{"POST", "/api/login"}, {"GET", "/api/search"}, {"POST", "/api/recommendations"},
}

var dbSystems = []struct {
	system, op, coll string
}{
	{"postgresql", "SELECT", "orders"}, {"postgresql", "INSERT", "orders"},
	{"postgresql", "SELECT", "products"}, {"mysql", "SELECT", "users"},
	{"redis", "GET", "session"}, {"redis", "SETEX", "cart"},
	{"mongodb", "find", "catalog"}, {"cassandra", "SELECT", "events"},
}

var environments = []string{"production", "staging"}
var regions = []string{"us-east-1", "us-west-2", "eu-central-1"}

// BuildCatalog deterministically constructs `n` services with bounded operation
// sets. The RNG seed makes the topology reproducible across replicas (the same
// topology everywhere is desirable; only the per-span data differs).
func BuildCatalog(n int, scopeName, scopeVer string, seed uint64) *Catalog {
	rng := mrand.New(mrand.NewPCG(seed, 0xD1B54A32D192ED03))
	cat := &Catalog{ScopeName: scopeName, ScopeVer: scopeVer}
	cat.Services = make([]service, n)

	for i := 0; i < n; i++ {
		name := serviceNames[i%len(serviceNames)]
		if i >= len(serviceNames) {
			name = fmt.Sprintf("%s-%d", name, i/len(serviceNames))
		}
		s := service{
			idx:       i,
			name:      name,
			namespace: "shop",
			version:   fmt.Sprintf("1.%d.%d", rng.IntN(8), rng.IntN(20)),
			host:      fmt.Sprintf("%s.shop.svc.cluster.local", name),
		}
		env := environments[rng.IntN(len(environments))]
		region := regions[rng.IntN(len(regions))]
		s.resourceAttrs = map[string]string{
			"service.name":            name,
			"service.namespace":       s.namespace,
			"service.version":         s.version,
			"service.instance.id":     fmt.Sprintf("%s-%d", name, rng.IntN(3)),
			"deployment.environment":  env,
			"telemetry.sdk.name":      "opentelemetry",
			"telemetry.sdk.language":  "go",
			"telemetry.sdk.version":   "1.31.0",
			"host.name":               fmt.Sprintf("node-%d", rng.IntN(12)),
			"host.arch":               "amd64",
			"os.type":                 "linux",
			"cloud.provider":          "onprem",
			"cloud.region":            region,
			"k8s.namespace.name":      "shop",
			"k8s.deployment.name":     name,
			"k8s.pod.name":            fmt.Sprintf("%s-%x", name, rng.Uint32()&0xffff),
			"k8s.node.name":           fmt.Sprintf("worker-%d", rng.IntN(12)),
		}

		// Entry points: a couple of HTTP server routes (+ sometimes a consumer).
		nEntry := 2 + rng.IntN(3)
		for e := 0; e < nEntry; e++ {
			r := httpRoutes[rng.IntN(len(httpRoutes))]
			s.entryOps = append(s.entryOps, operation{
				name: r.method + " " + r.route, typ: opHTTPServer, kind: ptrace.SpanKindServer,
				method: r.method, route: r.route, peer: -1,
				muLog: 16.0, sigmaLog: 0.6, // ~e^16ns ≈ 9ms median
			})
		}
		if rng.Float64() < 0.4 {
			dst := fmt.Sprintf("%s.events", name)
			s.entryOps = append(s.entryOps, operation{
				name: dst + " process", typ: opMsgConsumer, kind: ptrace.SpanKindConsumer,
				destination: dst, peer: -1, muLog: 15.5, sigmaLog: 0.7,
			})
		}
		cat.Services[i] = s
	}

	// Child operations reference peers; build after all services exist so peer
	// indices are valid. Downstream calls only go to higher indices to avoid
	// cycles (a clean call-graph DAG).
	for i := 0; i < n; i++ {
		s := &cat.Services[i]
		// outbound HTTP/RPC calls to a few downstream services
		nPeers := rng.IntN(3)
		for p := 0; p < nPeers && i+1 < n; p++ {
			peer := i + 1 + rng.IntN(n-i-1)
			if rng.Float64() < 0.5 {
				r := httpRoutes[rng.IntN(len(httpRoutes))]
				s.childOps = append(s.childOps, operation{
					name: r.method + " " + r.route, typ: opHTTPClient, kind: ptrace.SpanKindClient,
					method: r.method, route: r.route, peer: peer, muLog: 15.0, sigmaLog: 0.7,
				})
			} else {
				s.childOps = append(s.childOps, operation{
					name: "grpc." + cat.Services[peer].name + "/Get", typ: opRPCClient, kind: ptrace.SpanKindClient,
					rpcService: cat.Services[peer].name + ".v1.Service", rpcMethod: "Get", peer: peer,
					muLog: 14.5, sigmaLog: 0.7,
				})
			}
		}
		// database calls (leaf)
		nDB := 1 + rng.IntN(2)
		for d := 0; d < nDB; d++ {
			db := dbSystems[rng.IntN(len(dbSystems))]
			s.childOps = append(s.childOps, operation{
				name: db.op + " " + db.coll, typ: opDBClient, kind: ptrace.SpanKindClient,
				dbSystem: db.system, dbOp: db.op, collection: db.coll,
				dbName: s.namespace, peer: -1, muLog: 13.5, sigmaLog: 0.8,
			})
		}
		// internal compute (leaf)
		s.childOps = append(s.childOps, operation{
			name: "compute." + s.name, typ: opInternal, kind: ptrace.SpanKindInternal,
			peer: -1, muLog: 13.0, sigmaLog: 0.9,
		})
	}
	return cat
}
