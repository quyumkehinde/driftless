// Package queue implements the jobs table operations: coalescing enqueue,
// SKIP LOCKED claiming, retry with backoff, dead-lettering, and the reaper.
package queue

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/store/db"
)

// Job is one row of the jobs table.
type Job = db.DriftlessJob

// The jobs.status values, matching the schema's CHECK constraint.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusDead    = "dead"
)

// KindSyncObject is the only job kind produced today; process_event is
// reserved in the schema for a future direct-event kind.
const KindSyncObject = "sync_object"

// DefaultPriority mirrors the schema default: lower runs sooner.
const DefaultPriority = 100

// backoffSchedule is the delay before retry attempt n+1; attempts past the
// end reuse the last entry. Each delay gets +-20% jitter.
var backoffSchedule = []time.Duration{
	5 * time.Second,
	25 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
}

// Queue wraps the jobs table for one process.
type Queue struct {
	pool              *pgxpool.Pool
	visibilityTimeout time.Duration
	maxAttempts       int32
}

// New builds a Queue. visibilityTimeout bounds how long a claimed job stays
// invisible before the reaper hands it to another worker; maxAttempts is
// the retry budget stamped onto every enqueued job (0 keeps the schema
// default).
func New(pool *pgxpool.Pool, visibilityTimeout time.Duration, maxAttempts int) *Queue {
	return &Queue{pool: pool, visibilityTimeout: visibilityTimeout, maxAttempts: int32(maxAttempts)}
}

// EnqueueParams describes one unit of work keyed by object.
type EnqueueParams struct {
	Kind         string // sync_object (default) or process_event
	ObjectType   string
	ObjectID     string
	EventID      string
	EventCreated time.Time
	Priority     int16 // lower runs sooner; 0 means the default 100
}

// Enqueue inserts a job or coalesces onto the live job for the same object,
// bumping it to the newest event. It runs inside the caller's transaction so
// ingest can commit the event insert and the enqueue atomically.
// inserted is false when the enqueue coalesced.
func (q *Queue) Enqueue(ctx context.Context, tx pgx.Tx, p EnqueueParams) (id int64, inserted bool, err error) {
	if p.Kind == "" {
		p.Kind = KindSyncObject
	}
	if p.Priority == 0 {
		p.Priority = DefaultPriority
	}
	maxAttempts := q.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	params := db.EnqueueJobParams{
		Kind:        p.Kind,
		ObjectType:  p.ObjectType,
		ObjectID:    p.ObjectID,
		Priority:    p.Priority,
		MaxAttempts: maxAttempts,
	}
	if p.EventID != "" {
		params.LatestEventID = &p.EventID
	}
	if !p.EventCreated.IsZero() {
		params.LatestEventCreated = &p.EventCreated
	}
	row, err := db.New(tx).EnqueueJob(ctx, params)
	if err != nil {
		return 0, false, err
	}
	return row.ID, row.Inserted, nil
}

// Claim atomically takes the runnable job with the best (priority, id). ok
// is false when nothing is runnable.
func (q *Queue) Claim(ctx context.Context) (job Job, ok bool, err error) {
	job, err = db.New(q.pool).ClaimJob(ctx, q.visibilityTimeout)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// Complete marks a claimed job done, unless a newer event was coalesced onto
// it while it ran, in which case it goes back to pending and requeued is
// true.
func (q *Queue) Complete(ctx context.Context, job Job) (requeued bool, err error) {
	status, err := db.New(q.pool).CompleteJob(ctx, db.CompleteJobParams{
		ID:                  job.ID,
		ClaimedEventCreated: job.LatestEventCreated,
		ClaimedEventID:      job.LatestEventID,
	})
	if err != nil {
		return false, err
	}
	return status == StatusPending, nil
}

// Fail records a failed attempt: back to pending with backoff, or dead once
// attempts have reached the job's max.
func (q *Queue) Fail(ctx context.Context, job Job, cause error) (dead bool, err error) {
	msg := cause.Error()
	status, err := db.New(q.pool).FailJob(ctx, db.FailJobParams{
		ID:        job.ID,
		Backoff:   backoffFor(job.Attempts),
		LastError: &msg,
	})
	if err != nil {
		return false, err
	}
	return status == StatusDead, nil
}

// Reap resurrects running jobs whose claim expired (a crashed worker) and
// dead-letters any that already used their attempts, reporting the two
// outcomes separately so silent dead-lettering is visible.
func (q *Queue) Reap(ctx context.Context) (resurrected, dead int64, err error) {
	rows, err := db.New(q.pool).ReapExpiredJobs(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, row := range rows {
		if row.Status == StatusDead {
			dead++
		} else {
			resurrected++
		}
	}
	return resurrected, dead, nil
}

// List returns up to limit jobs with the given status, oldest first.
func (q *Queue) List(ctx context.Context, status string, limit int32) ([]Job, error) {
	return db.New(q.pool).ListJobs(ctx, db.ListJobsParams{Status: status, RowLimit: limit})
}

// RetryAllDead requeues every dead job with a fresh attempt budget.
func (q *Queue) RetryAllDead(ctx context.Context) (int64, error) {
	return db.New(q.pool).RetryDeadJobs(ctx)
}

// RetryOne requeues one dead job by id; it reports whether a row changed.
func (q *Queue) RetryOne(ctx context.Context, id int64) (bool, error) {
	n, err := db.New(q.pool).RetryJob(ctx, id)
	return n > 0, err
}

// CountByStatus reports row counts per job status, for status output and
// the jobs gauge.
func (q *Queue) CountByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := db.New(q.pool).CountJobsByStatus(ctx)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64, len(rows))
	for _, r := range rows {
		counts[r.Status] = r.Count
	}
	return counts, nil
}

// backoffFor returns the jittered delay after the given attempt count.
// attempts is 1-based at the time of failure since Claim increments it.
func backoffFor(attempts int32) time.Duration {
	idx := int(attempts) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(backoffSchedule) {
		idx = len(backoffSchedule) - 1
	}
	base := backoffSchedule[idx]
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(base) * jitter)
}
