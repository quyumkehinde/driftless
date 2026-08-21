// Package backfill imports history through the Stripe list APIs into the
// mirror schema: dependency-ordered, resumable at page granularity, and
// guarded against clobbering data that webhooks have already made fresher.
package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/crashpoint"
	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// The backfill_runs.status and backfill_tasks.status values, matching the
// schema CHECK constraints. Runs add cancelled; tasks add failed.
const (
	StatusRunning   = "running"
	StatusDone      = "done"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
	statusPending   = "pending"
)

// pageRetryDelay paces retries when a page fetch keeps failing after the
// client's own retry budget, as during a sustained 429 storm.
const pageRetryDelay = 2 * time.Second

// TypeOrder is the dependency-friendly walk: parents before children so
// foreign joins resolve. Payment methods run after subscriptions because
// their shallow backfill covers exactly the customers with a live
// subscription, which the mirror knows only once subscriptions landed.
var TypeOrder = []stripeapi.ObjectType{
	stripeapi.ObjectProduct,
	stripeapi.ObjectPrice,
	stripeapi.ObjectCustomer,
	stripeapi.ObjectSubscription,
	stripeapi.ObjectPaymentMethod,
	stripeapi.ObjectInvoice,
	stripeapi.ObjectCharge,
	stripeapi.ObjectPaymentIntent,
	stripeapi.ObjectSetupIntent,
	stripeapi.ObjectRefund,
	stripeapi.ObjectDispute,
	stripeapi.ObjectCheckoutSession,
}

// Metrics holds the backfill prometheus instruments.
type Metrics struct {
	Objects *prometheus.CounterVec
}

// NewMetrics registers the backfill metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Objects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_backfill_objects_total",
			Help: "Objects written by backfill, by type.",
		}, []string{"type"}),
	}
	reg.MustRegister(m.Objects)
	return m
}

// Options shape one backfill run.
type Options struct {
	Since       *time.Time             // nil = full history
	Types       []stripeapi.ObjectType // nil = every type, in TypeOrder
	RequestedBy string                 // 'cli' or 'auto-init'
}

// Progress receives a line per page so the CLI can show liveness; nil is
// silent.
type Progress func(objectType stripeapi.ObjectType, pagesDone, objectsDone int64)

// Runner drives backfill runs.
type Runner struct {
	pool     *pgxpool.Pool
	client   *stripeapi.Client
	logger   *slog.Logger
	metrics  *Metrics
	progress Progress
}

// NewRunner wires a backfill runner. metrics and progress may be nil.
func NewRunner(pool *pgxpool.Pool, client *stripeapi.Client, logger *slog.Logger, metrics *Metrics, progress Progress) *Runner {
	return &Runner{
		pool:     pool,
		client:   client,
		logger:   obs.WithComponent(logger, "backfill"),
		metrics:  metrics,
		progress: progress,
	}
}

// Plan returns the types a run with these options would walk, in order.
func Plan(types []stripeapi.ObjectType) ([]stripeapi.ObjectType, error) {
	if len(types) == 0 {
		return TypeOrder, nil
	}
	valid := make(map[stripeapi.ObjectType]bool, len(TypeOrder))
	for _, objectType := range TypeOrder {
		valid[objectType] = true
	}
	var planned []stripeapi.ObjectType
	for _, objectType := range TypeOrder {
		for _, want := range types {
			if want == objectType {
				planned = append(planned, objectType)
			}
		}
	}
	for _, want := range types {
		if !valid[want] {
			return nil, fmt.Errorf("backfill: unknown or unlistable object type %q", want)
		}
	}
	return planned, nil
}

// Start creates a run and executes it to completion. It refuses while
// another run is in progress: overlapping fresh runs are safe but double
// the API draw for no benefit. The pre-check gives the friendly error;
// the partial unique index closes the race window.
func (r *Runner) Start(ctx context.Context, opts Options) (int64, error) {
	planned, err := Plan(opts.Types)
	if err != nil {
		return 0, err
	}
	if running, err := db.New(r.pool).GetRunningBackfillRun(ctx); err == nil {
		return 0, &RunInProgressError{RunID: running}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	run, err := db.New(r.pool).CreateBackfillRun(ctx, db.CreateBackfillRunParams{
		RequestedBy: opts.RequestedBy,
		Since:       opts.Since,
	})
	if err != nil {
		// two Starts raced past the pre-check: the index picked the loser
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" /* unique_violation */ {
			if running, lookupErr := db.New(r.pool).GetRunningBackfillRun(ctx); lookupErr == nil {
				return 0, &RunInProgressError{RunID: running}
			}
		}
		return 0, err
	}
	for _, objectType := range planned {
		if _, err := db.New(r.pool).CreateBackfillTask(ctx, db.CreateBackfillTaskParams{
			RunID: run.ID, ObjectType: string(objectType),
		}); err != nil {
			return run.ID, err
		}
	}
	return run.ID, r.execute(ctx, run.ID, opts.Since)
}

// ErrRunLocked reports that another process is already driving the run.
var ErrRunLocked = errors.New("backfill: run is already being driven by another process")

// RunInProgressError refuses a fresh run while another is in progress; a
// crashed run counts until it is resumed or cancelled.
type RunInProgressError struct {
	RunID int64
}

func (e *RunInProgressError) Error() string {
	return fmt.Sprintf(
		"backfill: run %d is already in progress; resume it with --resume %d, or wait for it to finish",
		e.RunID, e.RunID)
}

// Resume continues a crashed, interrupted, or deliberately cancelled run
// from its cursors. Cancelled runs resume only through this explicit call,
// never through auto_resume.
func (r *Runner) Resume(ctx context.Context, runID int64) error {
	run, err := db.New(r.pool).GetBackfillRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("backfill: run %d: %w", runID, err)
	}
	if run.Status != StatusRunning && run.Status != StatusCancelled {
		return fmt.Errorf("backfill: run %d is %s, not resumable", runID, run.Status)
	}
	if _, err := db.New(r.pool).ReactivateBackfillRun(ctx, runID); err != nil {
		return err
	}
	r.logger.Info("resuming backfill run", "run_id", runID)
	return r.execute(ctx, runID, run.Since)
}

// Cancel records a deliberate stop of a run, keeping its cursors for a
// later explicit resume. It reports whether the run was in fact running.
func (r *Runner) Cancel(ctx context.Context, runID int64) (bool, error) {
	n, err := db.New(r.pool).CancelBackfillRun(ctx, runID)
	return n > 0, err
}

func (r *Runner) execute(ctx context.Context, runID int64, since *time.Time) error {
	// one driver per run: a session advisory lock held for the whole run
	// keeps auto_resume and a manual resume from double-fetching pages
	lockConn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Release()
	lockKey := fmt.Sprintf("backfill_run:%d", runID)
	// A kill -9'd previous driver releases its lock only when Postgres
	// tears its session down, which can lag a fast restart by a moment:
	// retry briefly before concluding another live driver holds the run.
	var locked bool
	for deadline := time.Now().Add(5 * time.Second); ; {
		if err := lockConn.QueryRow(ctx,
			`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, lockKey).Scan(&locked); err != nil {
			return err
		}
		if locked || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	if !locked {
		return fmt.Errorf("run %d: %w", runID, ErrRunLocked)
	}
	defer func() {
		// only needed on the graceful path: a dead session releases its
		// advisory locks automatically, but a pooled session lives on
		_, _ = lockConn.Exec(context.WithoutCancel(ctx),
			`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey)
	}()

	tasks, err := db.New(r.pool).ListRunTasks(ctx, runID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task.Status == StatusDone {
			continue
		}
		if err := r.runTask(ctx, task, since); err != nil {
			// the run stays 'running' so it can be resumed
			msg := err.Error()
			_ = db.New(r.pool).FinishBackfillTask(context.WithoutCancel(ctx), db.FinishBackfillTaskParams{
				ID: task.ID, Status: StatusFailed, LastError: &msg,
			})
			return fmt.Errorf("backfill: task %s: %w", task.ObjectType, err)
		}
	}
	if err := db.New(r.pool).FinishBackfillRun(ctx, db.FinishBackfillRunParams{
		ID: runID, Status: StatusDone,
	}); err != nil {
		return err
	}
	r.logger.Info("backfill run complete", "run_id", runID)
	return nil
}

func (r *Runner) runTask(ctx context.Context, task db.DriftlessBackfillTask, since *time.Time) error {
	if err := db.New(r.pool).StartBackfillTask(ctx, task.ID); err != nil {
		return err
	}
	run, err := db.New(r.pool).GetBackfillRun(ctx, task.RunID)
	if err != nil {
		return err
	}
	// the freshness horizon: an object already updated by an event newer
	// than run start must not be clobbered by a stale list page
	horizon := run.StartedAt
	objectType := stripeapi.ObjectType(task.ObjectType)

	if objectType == stripeapi.ObjectPaymentMethod {
		return r.runPaymentMethods(ctx, task, horizon)
	}
	return r.runListWalk(ctx, objectType, task, since, horizon)
}

// runListWalk pages one collection, upserting each page and advancing the
// cursor in a single transaction per page.
func (r *Runner) runListWalk(ctx context.Context, objectType stripeapi.ObjectType, task db.DriftlessBackfillTask, since *time.Time, horizon time.Time) error {
	path, query := listRequest(objectType, since)
	cursor := task.Cursor
	// resumed tasks continue their tallies; the task row is a snapshot
	// from claim time, so the running totals live here
	pagesDone := int64(task.PagesDone)
	objectsDone := int64(task.ObjectsDone)

	for {
		if cursor != nil && *cursor != "" {
			query.Set("starting_after", *cursor)
		}
		page, err := r.fetchPage(ctx, path, query)
		if err != nil {
			return err
		}
		if len(page.Data) == 0 {
			break
		}
		lastID, count, err := r.commitPage(ctx, task, page.Data, horizon)
		if err != nil {
			return err
		}
		pagesDone++
		objectsDone += count
		if r.progress != nil {
			r.progress(objectType, pagesDone, objectsDone)
		}
		r.note(objectType, count)
		if !page.HasMore {
			break
		}
		cursor = &lastID
	}
	return db.New(r.pool).FinishBackfillTask(ctx, db.FinishBackfillTaskParams{
		ID: task.ID, Status: StatusDone,
	})
}

// runPaymentMethods is the documented shallow policy: list per customer,
// only for customers holding a live subscription in the mirror. Deep
// payment method coverage arrives through events.
func (r *Runner) runPaymentMethods(ctx context.Context, task db.DriftlessBackfillTask, horizon time.Time) error {
	objectType := stripeapi.ObjectType(task.ObjectType)
	// the cursor for this task is the last customer id fully processed
	var afterCustomer string
	if task.Cursor != nil {
		afterCustomer = *task.Cursor
	}
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT customer FROM stripe.subscriptions
		WHERE NOT is_deleted AND status IN ('active','trialing','past_due') AND customer > $1
		ORDER BY customer`, afterCustomer)
	if err != nil {
		return err
	}
	customers := []string{}
	for rows.Next() {
		var customer string
		if err := rows.Scan(&customer); err != nil {
			rows.Close()
			return err
		}
		customers = append(customers, customer)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	pagesDone := int64(task.PagesDone)
	objectsDone := int64(task.ObjectsDone)
	for _, customer := range customers {
		query := url.Values{"limit": {strconv.Itoa(stripeapi.MaxPageLimit)}}
		path := "/v1/customers/" + url.PathEscape(customer) + "/payment_methods"
		for {
			page, err := r.fetchPage(ctx, path, query)
			if err != nil {
				return err
			}
			_, count, err := r.commitPageWithCursor(ctx, task, page.Data, horizon, customer)
			if err != nil {
				return err
			}
			pagesDone++
			objectsDone += count
			if r.progress != nil {
				r.progress(objectType, pagesDone, objectsDone)
			}
			r.note(objectType, count)
			if !page.HasMore || len(page.Data) == 0 {
				break
			}
			var lastID struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(page.Data[len(page.Data)-1], &lastID); err != nil {
				return err
			}
			query.Set("starting_after", lastID.ID)
		}
	}
	return db.New(r.pool).FinishBackfillTask(ctx, db.FinishBackfillTaskParams{
		ID: task.ID, Status: StatusDone,
	})
}

// fetchPage lists one page at backfill priority, retrying past sustained
// but transient pressure instead of failing the task. Terminal errors, a
// revoked key or a rejected cursor, return immediately: retrying those
// forever would pin the run lock and a connection for nothing.
func (r *Runner) fetchPage(ctx context.Context, path string, query url.Values) (*stripeapi.ListPage, error) {
	for {
		page, err := r.client.List(ctx, stripeapi.PriorityBackfill, path, query)
		if err == nil {
			return page, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !retryable(err) {
			return nil, err
		}
		r.logger.Warn("page fetch failed, retrying", "path", path, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pageRetryDelay):
		}
	}
}

// retryable reports whether an error class can heal on its own: rate
// limiting, server errors, and network failures do; other API errors are
// terminal.
func retryable(err error) bool {
	var apiErr *stripeapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status == 429 || apiErr.Status >= 500
	}
	var notFound *stripeapi.NotFoundError
	if errors.As(err, &notFound) {
		return false
	}
	return true // network-class errors
}

// commitPage upserts a page and advances the cursor in one transaction.
func (r *Runner) commitPage(ctx context.Context, task db.DriftlessBackfillTask, items []json.RawMessage, horizon time.Time) (lastID string, count int64, err error) {
	return r.commitPageWithCursor(ctx, task, items, horizon, "")
}

func (r *Runner) commitPageWithCursor(ctx context.Context, task db.DriftlessBackfillTask, items []json.RawMessage, horizon time.Time, cursorOverride string) (lastID string, count int64, err error) {
	objectType := stripeapi.ObjectType(task.ObjectType)
	err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		for _, item := range items {
			var envelope struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(item, &envelope); err != nil || envelope.ID == "" {
				return fmt.Errorf("page item without id: %w", err)
			}
			lastID = envelope.ID

			wrote, err := r.upsertGuarded(ctx, tx, objectType, envelope.ID, item, horizon)
			if err != nil {
				return err
			}
			if wrote {
				count++
			}
		}
		cursor := lastID
		if cursorOverride != "" {
			cursor = cursorOverride
		}
		if err := db.New(tx).AdvanceBackfillCursor(ctx, db.AdvanceBackfillCursorParams{
			ID: task.ID, Cursor: &cursor, ObjectsDone: int32(count),
		}); err != nil {
			return err
		}
		crashpoint.Maybe("backfill.before-page-commit")
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	return lastID, count, nil
}

// upsertGuarded writes one object under its advisory lock unless an event
// newer than the run's start already updated it, in which case the page
// item is a stale snapshot and is skipped.
func (r *Runner) upsertGuarded(ctx context.Context, tx pgx.Tx, objectType stripeapi.ObjectType, id string, item json.RawMessage, horizon time.Time) (bool, error) {
	if err := mirror.LockObject(ctx, tx, objectType, id); err != nil {
		return false, err
	}

	state, err := db.New(tx).GetObjectState(ctx, db.GetObjectStateParams{
		ObjectType: string(objectType), ObjectID: id,
	})
	if err == nil && state.LastEventCreated != nil && state.LastEventCreated.After(horizon) {
		return false, nil // fresher truth already applied
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}

	if objectType == stripeapi.ObjectSubscription {
		if err := mirror.UpsertSubscription(ctx, tx, r.client, stripeapi.PriorityBackfill, id, item); err != nil {
			return false, err
		}
	} else if err := mirror.UpsertObject(ctx, tx, objectType, id, item); err != nil {
		return false, err
	}

	if err := db.New(tx).UpsertObjectState(ctx, db.UpsertObjectStateParams{
		ObjectType:       string(objectType),
		ObjectID:         id,
		LastEventCreated: &horizon,
		SyncSource:       string(mirror.SyncSourceBackfill),
	}); err != nil {
		return false, err
	}
	return true, nil
}

// listRequest builds the collection path and base query for one type.
func listRequest(objectType stripeapi.ObjectType, since *time.Time) (string, url.Values) {
	query := url.Values{"limit": {strconv.Itoa(stripeapi.MaxPageLimit)}}
	if since != nil {
		query.Set("created[gte]", strconv.FormatInt(since.Unix(), 10))
	}
	path, _ := stripeapi.CollectionPath(objectType)
	if objectType == stripeapi.ObjectSubscription {
		// the default listing omits canceled subscriptions; status=all is
		// the difference between a mirror and a lie
		query.Set("status", "all")
	}
	return path, query
}

func (r *Runner) note(objectType stripeapi.ObjectType, count int64) {
	if r.metrics != nil {
		r.metrics.Objects.WithLabelValues(string(objectType)).Add(float64(count))
	}
}
