module spangen

go 1.23

// Direct dependencies. Run `go mod tidy && go mod vendor` on a connected build
// host to resolve the full graph, generate go.sum, and populate vendor/ so the
// image can be built fully offline (air-gapped).
require (
	github.com/ClickHouse/clickhouse-go/v2 v2.30.0
	github.com/prometheus/client_golang v1.20.5
	go.opentelemetry.io/collector/pdata v1.18.0
	golang.org/x/time v0.7.0
	google.golang.org/grpc v1.67.1
)
