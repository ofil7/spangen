// Package sink delivers generated traces to a backend. Two implementations
// exist — direct ClickHouse (native protocol) and OTLP/gRPC to a collector —
// both consuming the exact same ptrace.Traces so the ingest paths are
// comparable.
package sink

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Sink sends a batch of traces. Implementations must be safe for concurrent use
// by multiple worker goroutines.
type Sink interface {
	// Send delivers all spans in td. It blocks until the backend acknowledges
	// (or the context/timeout fires).
	Send(ctx context.Context, td ptrace.Traces) error
	// Name identifies the sink for logs/metrics.
	Name() string
	// Close releases connections.
	Close() error
}
