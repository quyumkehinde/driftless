package queue

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/obs"
)

// Applier performs the work for one claimed job. The apply engine implements
// this; tests substitute stubs.
type Applier interface {
	Apply(ctx context.Context, job Job) error
}

// Metrics holds the queue's prometheus instruments.
type Metrics struct {
	Processed *prometheus.CounterVec
	Dead      prometheus.Counter
	Jobs      *prometheus.GaugeVec
}

// NewMetrics registers the queue metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_jobs_processed_total",
			Help: "Job attempts by outcome.",
		}, []string{"result"}),
		Dead: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driftless_jobs_dead_total",
			Help: "Jobs that exhausted their attempts.",
		}),
		Jobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "driftless_jobs_total",
			Help: "Jobs currently in the queue by status.",
		}, []string{"status"}),
	}
	reg.MustRegister(m.Processed, m.Dead, m.Jobs)
	return m
}

// SampleJobs refreshes the jobs gauge from the queue. Statuses with no
// rows are reset so a drained status reads zero, not its last value.
func (m *Metrics) SampleJobs(ctx context.Context, q *Queue) error {
	counts, err := q.CountByStatus(ctx)
	if err != nil {
		return err
	}
	for _, status := range []string{StatusPending, StatusRunning, StatusDone, StatusDead} {
		m.Jobs.WithLabelValues(status).Set(float64(counts[status]))
	}
	return nil
}

// WorkerPool runs N goroutines that claim and apply jobs until the context
// is cancelled.
type WorkerPool struct {
	queue        *Queue
	applier      Applier
	count        int
	pollInterval time.Duration
	logger       *slog.Logger
	metrics      *Metrics
}

// NewWorkerPool builds a pool. pollInterval is the idle sleep between claim
// attempts when the queue is empty; metrics may be nil.
func NewWorkerPool(q *Queue, applier Applier, count int, pollInterval time.Duration, logger *slog.Logger, metrics *Metrics) *WorkerPool {
	return &WorkerPool{
		queue:        q,
		applier:      applier,
		count:        count,
		pollInterval: pollInterval,
		logger:       obs.WithComponent(logger, "queue"),
		metrics:      metrics,
	}
}

// Run blocks until ctx is cancelled, then waits for in-flight jobs to
// settle.
func (w *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range w.count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	wg.Wait()
}

func (w *WorkerPool) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		job, ok, err := w.queue.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("claim failed", "error", err)
			w.sleep(ctx)
			continue
		}
		if !ok {
			w.sleep(ctx)
			continue
		}
		w.process(ctx, job)
	}
}

func (w *WorkerPool) process(ctx context.Context, job Job) {
	if err := w.applier.Apply(ctx, job); err != nil {
		dead, failErr := w.queue.Fail(ctx, job, err)
		if failErr != nil {
			w.logger.Error("recording failure failed", "job_id", job.ID, "error", failErr)
			return
		}
		if dead {
			w.withMetrics(func(m *Metrics) {
				m.Processed.WithLabelValues("dead").Inc()
				m.Dead.Inc()
			})
			w.logger.Error("job dead after max attempts",
				"job_id", job.ID, "object_type", job.ObjectType, "object_id", job.ObjectID,
				"attempts", job.Attempts, "error", err)
			return
		}
		w.withMetrics(func(m *Metrics) { m.Processed.WithLabelValues("retried").Inc() })
		w.logger.Warn("job attempt failed, will retry",
			"job_id", job.ID, "object_type", job.ObjectType, "object_id", job.ObjectID,
			"attempts", job.Attempts, "error", err)
		return
	}

	requeued, err := w.queue.Complete(ctx, job)
	if err != nil {
		w.logger.Error("completing job failed", "job_id", job.ID, "error", err)
		return
	}
	if requeued {
		w.logger.Debug("job requeued for newer coalesced event", "job_id", job.ID)
		return
	}
	w.withMetrics(func(m *Metrics) { m.Processed.WithLabelValues("done").Inc() })
}

func (w *WorkerPool) withMetrics(f func(*Metrics)) {
	if w.metrics != nil {
		f(w.metrics)
	}
}

// sleep waits one poll interval with jitter, returning early on cancel.
func (w *WorkerPool) sleep(ctx context.Context) {
	jitter := time.Duration(float64(w.pollInterval) * (0.5 + rand.Float64()))
	select {
	case <-ctx.Done():
	case <-time.After(jitter):
	}
}
