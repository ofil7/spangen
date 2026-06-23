package sink

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip" // registers the gzip compressor in init
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"spangen/internal/config"
)

// otlpSink exports traces to OpenTelemetry collector(s) over OTLP/gRPC. A pool
// of connections provides the concurrency needed to sustain the target rate;
// per-request span count is bounded by the engine (cfg.OTLP.SpansPerRequest) to
// stay under gRPC's 4MB message limit.
type otlpSink struct {
	conns    []*grpc.ClientConn
	clients  []ptraceotlp.GRPCClient
	next     atomic.Uint64
	timeout  time.Duration
	mdCtx    metadata.MD
	callOpts []grpc.CallOption
}

// NewOTLP dials the collector endpoint cfg.OTLP.Connections times.
func NewOTLP(cfg *config.Config) (Sink, error) {
	o := cfg.OTLP

	var creds credentials.TransportCredentials
	if o.Insecure {
		creds = insecure.NewCredentials()
	} else {
		// Trusted air-gapped network: skip CA verification (see clickhouse.go).
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec
	}

	callOpts := []grpc.CallOption{
		// Raise the send limit well above the default 4MB so a slightly oversized
		// batch errors loudly only if truly huge; the engine keeps requests small.
		grpc.MaxCallSendMsgSize(32 << 20),
	}
	if o.Gzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(callOpts...),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	s := &otlpSink{timeout: o.Timeout, callOpts: callOpts}
	if len(o.Headers) > 0 {
		s.mdCtx = metadata.New(o.Headers)
	}

	n := o.Connections
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		conn, err := grpc.NewClient(o.Endpoint, dialOpts...)
		if err != nil {
			s.closeConns()
			return nil, fmt.Errorf("dial otlp %s: %w", o.Endpoint, err)
		}
		s.conns = append(s.conns, conn)
		s.clients = append(s.clients, ptraceotlp.NewGRPCClient(conn))
	}
	return s, nil
}

func (s *otlpSink) Name() string { return "otlp" }

func (s *otlpSink) Close() error { s.closeConns(); return nil }

func (s *otlpSink) closeConns() {
	for _, c := range s.conns {
		_ = c.Close()
	}
}

func (s *otlpSink) Send(ctx context.Context, td ptrace.Traces) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if s.mdCtx != nil {
		ctx = metadata.NewOutgoingContext(ctx, s.mdCtx)
	}

	client := s.clients[0]
	if len(s.clients) > 1 {
		client = s.clients[s.next.Add(1)%uint64(len(s.clients))]
	}

	req := ptraceotlp.NewExportRequestFromTraces(td)
	resp, err := client.Export(ctx, req, s.callOpts...)
	if err != nil {
		return fmt.Errorf("otlp export: %w", err)
	}
	// Surface partial rejections (collector dropped some spans).
	if ps := resp.PartialSuccess(); ps.RejectedSpans() > 0 {
		return fmt.Errorf("otlp partial success: %d spans rejected: %s", ps.RejectedSpans(), ps.ErrorMessage())
	}
	return nil
}
