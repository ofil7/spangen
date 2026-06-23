// Package metrics exposes the generator's own telemetry so each replica's
// achieved throughput, latency, and error rate can be scraped by Prometheus.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the registered collectors.
type Metrics struct {
	reg *prometheus.Registry

	SpansGenerated prometheus.Counter
	SpansSent      prometheus.Counter
	BatchesOK      prometheus.Counter
	BatchesErr     prometheus.Counter
	SendErrors     prometheus.Counter
	Dropped        prometheus.Counter
	SendDuration   prometheus.Histogram
	Inflight       prometheus.Gauge
	TargetRate     prometheus.Gauge
	AchievedRate   prometheus.Gauge
}

// New registers the collectors. `sink` and `replica` become constant labels so
// scraped series from the 10 replicas are distinguishable.
func New(sink string, replica int) *Metrics {
	reg := prometheus.NewRegistry()
	constLabels := prometheus.Labels{"sink": sink}

	m := &Metrics{reg: reg}
	f := promauto{reg, constLabels}

	m.SpansGenerated = f.counter("spangen_spans_generated_total", "Spans generated.")
	m.SpansSent = f.counter("spangen_spans_sent_total", "Spans successfully sent to the sink.")
	m.BatchesOK = f.counter("spangen_batches_ok_total", "Batches sent successfully.")
	m.BatchesErr = f.counter("spangen_batches_error_total", "Batches that failed to send.")
	m.SendErrors = f.counter("spangen_send_errors_total", "Send errors (including retries).")
	m.Dropped = f.counter("spangen_dropped_spans_total", "Spans dropped after exhausting retries.")
	m.Inflight = f.gauge("spangen_inflight_batches", "Batches currently being sent.")
	m.TargetRate = f.gauge("spangen_target_rate_spans_per_sec", "Current target rate (after ramp).")
	m.AchievedRate = f.gauge("spangen_achieved_rate_spans_per_sec", "Measured spans/sec over the last sample window.")
	m.SendDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "spangen_send_duration_seconds",
		Help:        "Per-batch send latency.",
		ConstLabels: constLabels,
		Buckets:     []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	reg.MustRegister(m.SendDuration)

	// Standard process/go collectors for CPU/mem/GC visibility under load.
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

// Serve runs the /metrics HTTP endpoint until ctx is cancelled.
func (m *Metrics) Serve(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	_ = srv.ListenAndServe()
}

// promauto is a tiny helper to register collectors with shared const labels.
type promauto struct {
	reg    *prometheus.Registry
	labels prometheus.Labels
}

func (p promauto) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: p.labels})
	p.reg.MustRegister(c)
	return c
}

func (p promauto) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: p.labels})
	p.reg.MustRegister(g)
	return g
}
