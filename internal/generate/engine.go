// Package generate runs the worker pool that produces traces and drives them
// into a sink at a paced rate.
package generate

import (
	"context"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"golang.org/x/time/rate"

	"spangen/internal/config"
	"spangen/internal/metrics"
	"spangen/internal/model"
	"spangen/internal/sink"
)

const sendAttempts = 2

// Engine owns the workers, the pacer, and the run lifecycle.
type Engine struct {
	cfg     *config.Config
	snk     sink.Sink
	met     *metrics.Metrics
	catalog *model.Catalog

	limiter *rate.Limiter
	batch   int

	generated atomic.Uint64
	sent      atomic.Uint64
	errors    atomic.Uint64
}

// New builds the engine: the shared topology, the pacer, and validates worker count.
func New(cfg *config.Config, snk sink.Sink, met *metrics.Metrics) *Engine {
	// Topology is identical across replicas (shared seed) — only per-span data
	// differs — so all replicas model the same services.
	cat := model.BuildCatalog(cfg.Gen.Services, "spangen", "1.0.0", cfg.Seed)

	e := &Engine{
		cfg:     cfg,
		snk:     snk,
		met:     met,
		catalog: cat,
		batch:   cfg.EffectiveBatchSize(),
	}

	if cfg.Gen.Rate > 0 {
		burst := e.batch + cfg.Gen.SpansPerTraceMax + 64
		// If ramping, start low; otherwise start at full target.
		start := cfg.Gen.Rate
		if cfg.Gen.Ramp > 0 {
			start = cfg.Gen.Rate / 100
		}
		e.limiter = rate.NewLimiter(rate.Limit(start), burst)
		met.TargetRate.Set(start)
	}
	return e
}

// Run blocks until ctx is cancelled, the duration elapses, or the span cap is hit.
func (e *Engine) Run(ctx context.Context) {
	workers := e.cfg.Gen.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if d := e.cfg.Gen.Duration; d > 0 {
		time.AfterFunc(d, cancel)
		log.Printf("run duration capped at %s", d)
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		seed := seedFor(e.cfg.Seed, e.cfg.ReplicaIndex, w)
		go func(seed uint64) {
			defer wg.Done()
			e.worker(runCtx, cancel, seed)
		}(seed)
	}

	go e.ramp(runCtx, cancel)
	go e.sampleRate(runCtx)
	go e.logStatus(runCtx)

	log.Printf("spangen started: sink=%s mode=%s workers=%d batch=%d target=%.0f spans/s replica=%d",
		e.snk.Name(), e.cfg.CH.Mode, workers, e.batch, e.cfg.Gen.Rate, e.cfg.ReplicaIndex)

	wg.Wait()
	log.Printf("spangen stopped: generated=%d sent=%d errors=%d", e.generated.Load(), e.sent.Load(), e.errors.Load())
}

func (e *Engine) worker(ctx context.Context, cancel context.CancelFunc, seed uint64) {
	g := model.NewGenerator(e.catalog, seed,
		e.cfg.Gen.SpansPerTraceMin, e.cfg.Gen.SpansPerTraceMax,
		e.cfg.Gen.ErrorRate, e.cfg.Gen.EventRate)

	for {
		if ctx.Err() != nil {
			return
		}

		td := ptrace.NewTraces()
		n := g.FillBatch(td, e.batch)

		if limit := e.cfg.Gen.MaxSpans; limit > 0 {
			if e.generated.Add(uint64(n)) >= limit {
				cancel() // last batch still sent below
			}
		} else {
			e.generated.Add(uint64(n))
		}
		e.met.SpansGenerated.Add(float64(n))

		if e.limiter != nil {
			if err := e.limiter.WaitN(ctx, n); err != nil {
				return // context cancelled while waiting
			}
		}

		e.send(ctx, td, n)
	}
}

func (e *Engine) send(ctx context.Context, td ptrace.Traces, n int) {
	e.met.Inflight.Inc()
	defer e.met.Inflight.Dec()

	var err error
	for attempt := 0; attempt < sendAttempts; attempt++ {
		t0 := time.Now()
		err = e.snk.Send(ctx, td)
		e.met.SendDuration.Observe(time.Since(t0).Seconds())
		if err == nil {
			e.met.BatchesOK.Inc()
			e.met.SpansSent.Add(float64(n))
			e.sent.Add(uint64(n))
			return
		}
		e.met.SendErrors.Inc()
		if ctx.Err() != nil {
			break // shutting down; don't retry
		}
		if attempt+1 < sendAttempts {
			time.Sleep(50 * time.Millisecond)
		}
	}
	e.met.BatchesErr.Inc()
	e.met.Dropped.Add(float64(n))
	e.errors.Add(uint64(n))
	if e.errors.Load()%uint64(e.batch*20) < uint64(n) { // log roughly every ~20 batches' worth
		log.Printf("send error (dropped %d spans): %v", n, err)
	}
}

// ramp linearly raises the limiter's rate from its start value to the target
// over cfg.Gen.Ramp, then exits.
func (e *Engine) ramp(ctx context.Context, _ context.CancelFunc) {
	if e.limiter == nil || e.cfg.Gen.Ramp <= 0 {
		if e.limiter != nil {
			e.met.TargetRate.Set(e.cfg.Gen.Rate)
		}
		return
	}
	target := e.cfg.Gen.Rate
	rampDur := e.cfg.Gen.Ramp
	start := time.Now()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			frac := time.Since(start).Seconds() / rampDur.Seconds()
			if frac >= 1 {
				e.limiter.SetLimit(rate.Limit(target))
				e.met.TargetRate.Set(target)
				return
			}
			cur := target * frac
			if cur < target/100 {
				cur = target / 100
			}
			e.limiter.SetLimit(rate.Limit(cur))
			e.met.TargetRate.Set(cur)
		}
	}
}

// sampleRate computes achieved spans/sec over a sliding window for the gauge.
func (e *Engine) sampleRate(ctx context.Context) {
	const window = 2 * time.Second
	tick := time.NewTicker(window)
	defer tick.Stop()
	prev := e.sent.Load()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			cur := e.sent.Load()
			e.met.AchievedRate.Set(float64(cur-prev) / window.Seconds())
			prev = cur
		}
	}
}

func (e *Engine) logStatus(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	prev := e.sent.Load()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			cur := e.sent.Load()
			dt := time.Since(last).Seconds()
			log.Printf("status: sent=%d (%.0f spans/s) generated=%d errors=%d",
				cur, float64(cur-prev)/dt, e.generated.Load(), e.errors.Load())
			prev = cur
			last = time.Now()
		}
	}
}

func seedFor(base uint64, replica, worker int) uint64 {
	return base + uint64(replica)*1000003 + uint64(worker)*2654435761
}
