package apply

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/crashpoint"
	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// Metrics holds the apply engine's prometheus instruments.
type Metrics struct {
	ApplySeconds *prometheus.HistogramVec
}

// NewMetrics registers the apply metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		ApplySeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "driftless_apply_seconds",
			Help:    "Time to apply one job.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}, []string{"mode"}),
	}
	reg.MustRegister(m.ApplySeconds)
	return m
}

// Engine applies claimed jobs: it fetches the object fresh from Stripe and
// materializes it into the mirror schema under the per-object advisory
// lock. Object types opted into payload mode apply the event's own
// data.object under the ordering guard instead. It implements
// queue.Applier.
type Engine struct {
	pool        *pgxpool.Pool
	client      *stripeapi.Client
	payloadMode map[string]bool
	logger      *slog.Logger
	metrics     *Metrics
}

// NewEngine wires the apply engine. payloadModeTypes lists the object
// types that apply from event payloads; metrics may be nil.
func NewEngine(pool *pgxpool.Pool, client *stripeapi.Client, payloadModeTypes []string, logger *slog.Logger, metrics *Metrics) *Engine {
	payloadMode := make(map[string]bool, len(payloadModeTypes))
	for _, objectType := range payloadModeTypes {
		payloadMode[objectType] = true
	}
	return &Engine{
		pool:        pool,
		client:      client,
		payloadMode: payloadMode,
		logger:      obs.WithComponent(logger, "apply"),
		metrics:     metrics,
	}
}

// Apply materializes one job's object. Everything happens in a single
// transaction holding the advisory lock: fetch, upsert, bookkeeping, and
// marking the poking event processed. A crash rolls all of it back and the
// job re-runs idempotently.
func (e *Engine) Apply(ctx context.Context, job queue.Job) error {
	start := time.Now()
	mode := mirror.SyncSourceFetch
	if e.payloadMode[job.ObjectType] {
		mode = mirror.SyncSourcePayload
	}
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		if err := mirror.LockObject(ctx, tx, job.ObjectType, job.ObjectID); err != nil {
			return err
		}
		if err := e.applyLocked(ctx, tx, job); err != nil {
			return err
		}
		crashpoint.Maybe("apply.before-commit")
		return nil
	})
	// The failure counter runs only after BeginFunc has returned and
	// released the transaction's connection: acquiring a second pooled
	// connection while holding the first deadlocks the whole worker pool
	// once every worker hits a correlated Stripe failure at once.
	var fetchFailure *fetchError
	if errors.As(err, &fetchFailure) {
		if _, bumpErr := db.New(e.pool).BumpFetchFailures(ctx, db.BumpFetchFailuresParams{
			ObjectType: job.ObjectType, ObjectID: job.ObjectID,
		}); bumpErr != nil {
			e.logger.Warn("recording fetch failure failed", "error", bumpErr)
		}
		err = fetchFailure.err
	}
	if e.metrics != nil {
		e.metrics.ApplySeconds.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	}
	return err
}

// fetchError marks a Stripe fetch failure so Apply can record it after the
// transaction's connection is released.
type fetchError struct {
	err error
}

func (e *fetchError) Error() string { return e.err.Error() }
func (e *fetchError) Unwrap() error { return e.err }

func (e *Engine) applyLocked(ctx context.Context, tx pgx.Tx, job queue.Job) error {
	q := db.New(tx)

	var eventPayload []byte
	if job.LatestEventID != nil {
		if event, err := q.GetEventForApply(ctx, *job.LatestEventID); err == nil {
			eventPayload = event.Payload
		}
	}

	// A deleted-object event means the fetch would 404: soft-delete
	// directly. Detected from the event payload's deleted flag, because
	// some deleted-family events (subscription cancellation) leave the
	// object fetchable and those must fetch normally.
	if eventMarksDeleted(eventPayload) {
		return e.finishSoftDelete(ctx, tx, job)
	}

	if e.payloadMode[job.ObjectType] && eventPayload != nil {
		return e.applyPayload(ctx, tx, job, eventPayload)
	}

	raw, err := e.client.GetObject(ctx, stripeapi.PriorityWebhook, job.ObjectType, job.ObjectID)
	var notFound *stripeapi.NotFoundError
	if errors.As(err, &notFound) {
		// The object is gone upstream: the truth is a soft delete.
		return e.finishSoftDelete(ctx, tx, job)
	}
	if err != nil {
		return &fetchError{err: err}
	}
	if stripeapi.IsDeletionStub(raw) {
		// Deletable objects fetch as 200 stubs, not 404s; upserting the
		// stub would clobber the row's history with three fields.
		return e.finishSoftDelete(ctx, tx, job)
	}

	if job.ObjectType == stripeapi.ObjectSubscription {
		if err := e.upsertSubscription(ctx, tx, job.ObjectID, raw); err != nil {
			return err
		}
	} else if err := mirror.UpsertObject(ctx, tx, job.ObjectType, job.ObjectID, raw); err != nil {
		return err
	}
	return e.finishApplied(ctx, tx, job, mirror.SyncSourceFetch)
}

// finishApplied records bookkeeping for a successful upsert.
func (e *Engine) finishApplied(ctx context.Context, tx pgx.Tx, job queue.Job, syncSource string) error {
	if err := e.recordState(ctx, tx, job, syncSource); err != nil {
		return err
	}
	return mirror.NotifyChange(ctx, tx, job.ObjectType, job.ObjectID)
}

// finishSoftDelete soft-deletes the object and records bookkeeping.
func (e *Engine) finishSoftDelete(ctx context.Context, tx pgx.Tx, job queue.Job) error {
	if err := mirror.SoftDeleteObject(ctx, tx, job.ObjectType, job.ObjectID); err != nil {
		return err
	}
	return e.finishApplied(ctx, tx, job, mirror.SyncSourceFetch)
}

func (e *Engine) recordState(ctx context.Context, tx pgx.Tx, job queue.Job, syncSource string) error {
	q := db.New(tx)
	if err := q.UpsertObjectState(ctx, db.UpsertObjectStateParams{
		ObjectType:       job.ObjectType,
		ObjectID:         job.ObjectID,
		LastEventCreated: job.LatestEventCreated,
		LastEventID:      job.LatestEventID,
		SyncSource:       syncSource,
	}); err != nil {
		return err
	}
	if job.LatestEventCreated != nil {
		if err := q.MarkEventsProcessedForObject(ctx, db.MarkEventsProcessedForObjectParams{
			ObjectID: job.ObjectID,
			Created:  *job.LatestEventCreated,
		}); err != nil {
			return err
		}
	}
	return nil
}

// upsertSubscription is the engine's apply-path wrapper.
func (e *Engine) upsertSubscription(ctx context.Context, tx pgx.Tx, subID string, raw []byte) error {
	return mirror.UpsertSubscription(ctx, tx, e.client, stripeapi.PriorityWebhook, subID, raw)
}

// eventMarksDeleted reports whether the event payload's data.object carries
// deleted: true, the marker Stripe sets only on genuinely deleted objects.
func eventMarksDeleted(payload []byte) bool {
	if payload == nil {
		return false
	}
	var envelope struct {
		Data struct {
			Object struct {
				Deleted bool `json:"deleted"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return false
	}
	return envelope.Data.Object.Deleted
}
