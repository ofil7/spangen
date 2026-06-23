// Command spangen is a high-throughput OpenTelemetry trace-span load generator
// for benchmarking ClickHouse. It generates realistic, bounded-cardinality
// traces and delivers them either directly to ClickHouse (native protocol) or
// to an OpenTelemetry Collector via OTLP/gRPC. Run multiple replicas to reach
// aggregate rates (e.g. 10 x 100k = ~1M spans/s).
//
// It makes no calls to the public internet and is intended to run fully
// air-gapped.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"spangen/internal/config"
	"spangen/internal/generate"
	"spangen/internal/metrics"
	"spangen/internal/sink"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := config.Parse()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	met := metrics.New(cfg.Sink, cfg.ReplicaIndex)
	go met.Serve(ctx, cfg.Metrics.Addr)
	log.Printf("metrics on %s/metrics", cfg.Metrics.Addr)

	var snk sink.Sink
	switch cfg.Sink {
	case "clickhouse":
		snk, err = sink.NewClickHouse(cfg, cfg.ReplicaIndex)
	case "otlp":
		snk, err = sink.NewOTLP(cfg)
	}
	if err != nil {
		log.Fatalf("sink %s: %v", cfg.Sink, err)
	}
	defer func() {
		if err := snk.Close(); err != nil {
			log.Printf("sink close: %v", err)
		}
	}()

	eng := generate.New(cfg, snk, met)
	eng.Run(ctx)
}
