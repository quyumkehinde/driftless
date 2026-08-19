// Package verify reconciles the mirror against Stripe's current truth: it
// re-reads objects from the API, compares them to the stored rows, reports
// every divergence, and optionally repairs it.
package verify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// Modes recorded on verifications rows.
const (
	ModeQuick = "quick"
	ModeFull  = "full"
)

// Drift kinds: how a mirror row can diverge from Stripe.
const (
	// KindMissing: Stripe has the object, the mirror does not.
	KindMissing = "missing"
	// KindStale: both have it, the stored data differs.
	KindStale = "stale"
	// KindOrphaned: the mirror holds it live, Stripe no longer does.
	KindOrphaned = "orphaned"
)

// DefaultSpotChecks is the quick-mode per-type sample size.
const DefaultSpotChecks = 25

// quickWindow bounds the quick-mode walk: recent objects are compared
// exhaustively, everything older is covered by the random spot-checks.
const quickWindow = 24 * time.Hour

// Types lists what verify covers, in backfill's dependency order. Payment
// methods are excluded: Stripe only lists them per customer, so there is
// no account-wide walk to compare against. Subscription items are checked
// through their parent subscription.
var Types = []string{
	stripeapi.ObjectProduct,
	stripeapi.ObjectPrice,
	stripeapi.ObjectCustomer,
	stripeapi.ObjectSubscription,
	stripeapi.ObjectInvoice,
	stripeapi.ObjectCharge,
	stripeapi.ObjectPaymentIntent,
	stripeapi.ObjectSetupIntent,
	stripeapi.ObjectRefund,
	stripeapi.ObjectDispute,
	stripeapi.ObjectCheckoutSession,
}

// Options shape one verification pass.
type Options struct {
	Full       bool
	Types      []string   // subset of Types; empty = all
	Since      *time.Time // bound the walk to objects created on or after
	Repair     bool
	SpotChecks int // quick-mode sample size per type; 0 = DefaultSpotChecks
}

// Drift is one diverged object.
type Drift struct {
	ObjectType string `json:"object_type"`
	ObjectID   string `json:"object_id"`
	Kind       string `json:"kind"`
	Repaired   bool   `json:"repaired"`
}

// TypeResult sums one object type's pass.
type TypeResult struct {
	ObjectType string `json:"object_type"`
	Checked    int    `json:"checked"`
	Drifted    int    `json:"drifted"`
	Repaired   int    `json:"repaired"`
}

// Report is the full outcome of one verification.
type Report struct {
	Mode     string       `json:"mode"`
	Types    []TypeResult `json:"types"`
	Drifts   []Drift      `json:"drifts"`
	Checked  int          `json:"checked"`
	Drifted  int          `json:"drifted"`
	Repaired int          `json:"repaired"`
}

// Progress receives a line per finished type; nil is silent.
type Progress func(objectType string, checked, drifted int)

// Metrics holds the verify prometheus instruments; long-running processes
// register them, one-shot CLI runs pass nil.
type Metrics struct {
	Drift   *prometheus.GaugeVec
	LastRun *prometheus.GaugeVec
}

// NewMetrics registers the verify metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Drift: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "driftless_verify_drift_total",
			Help: "Drifted objects found by the most recent verification, by type.",
		}, []string{"type"}),
		LastRun: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "driftless_verify_last_run_timestamp",
			Help: "Unix time of the last completed verification, by mode.",
		}, []string{"mode"}),
	}
	reg.MustRegister(m.Drift, m.LastRun)
	return m
}

// Runner drives verification passes.
type Runner struct {
	pool     *pgxpool.Pool
	client   *stripeapi.Client
	logger   *slog.Logger
	progress Progress
	metrics  *Metrics
}

// NewRunner wires a verify runner. progress and metrics may be nil.
func NewRunner(pool *pgxpool.Pool, client *stripeapi.Client, logger *slog.Logger, progress Progress) *Runner {
	return &Runner{
		pool:     pool,
		client:   client,
		logger:   obs.WithComponent(logger, "verify"),
		progress: progress,
	}
}

// WithMetrics attaches instruments updated after every completed run.
func (r *Runner) WithMetrics(m *Metrics) *Runner {
	r.metrics = m
	return r
}

// Plan validates and orders the requested types, so callers can reject
// unverifiable types before a run starts.
func Plan(types []string) ([]string, error) {
	if len(types) == 0 {
		return Types, nil
	}
	valid := make(map[string]bool, len(Types))
	for _, objectType := range Types {
		valid[objectType] = true
	}
	for _, want := range types {
		if !valid[want] {
			return nil, fmt.Errorf("verify: unknown or unverifiable object type %q", want)
		}
	}
	var planned []string
	for _, objectType := range Types {
		for _, want := range types {
			if want == objectType {
				planned = append(planned, objectType)
			}
		}
	}
	return planned, nil
}

// Run executes one verification pass and records it in the verifications
// history. The report is returned even when drift is found; the caller
// owns the exit-code contract.
func (r *Runner) Run(ctx context.Context, opts Options) (*Report, error) {
	planned, err := Plan(opts.Types)
	if err != nil {
		return nil, err
	}
	if opts.SpotChecks <= 0 {
		opts.SpotChecks = DefaultSpotChecks
	}

	mode := ModeQuick
	if opts.Full {
		mode = ModeFull
	}
	var recordedType *string
	if len(opts.Types) == 1 {
		recordedType = &planned[0]
	}
	verificationID, err := db.New(r.pool).CreateVerification(ctx, db.CreateVerificationParams{
		Mode:       mode,
		ObjectType: recordedType,
	})
	if err != nil {
		return nil, err
	}

	report := &Report{Mode: mode}
	for _, objectType := range planned {
		result, drifts, err := r.verifyType(ctx, objectType, opts)
		if err != nil {
			return nil, fmt.Errorf("verify %s: %w", objectType, err)
		}
		report.Types = append(report.Types, result)
		report.Drifts = append(report.Drifts, drifts...)
		report.Checked += result.Checked
		report.Drifted += result.Drifted
		report.Repaired += result.Repaired
		if r.progress != nil {
			r.progress(objectType, result.Checked, result.Drifted)
		}
	}

	detail, err := json.Marshal(report.Drifts)
	if err != nil {
		return nil, err
	}
	if err := db.New(r.pool).FinishVerification(context.WithoutCancel(ctx), db.FinishVerificationParams{
		ID:       verificationID,
		Checked:  int32(report.Checked),
		Drifted:  int32(report.Drifted),
		Repaired: int32(report.Repaired),
		Report:   detail,
	}); err != nil {
		return nil, err
	}
	if r.metrics != nil {
		for _, tr := range report.Types {
			r.metrics.Drift.WithLabelValues(tr.ObjectType).Set(float64(tr.Drifted))
		}
		r.metrics.LastRun.WithLabelValues(mode).SetToCurrentTime()
	}
	return report, nil
}

// verifyType runs one type's pass: an exhaustive walk in full mode, a
// recent-window walk plus random spot-checks in quick mode.
func (r *Runner) verifyType(ctx context.Context, objectType string, opts Options) (TypeResult, []Drift, error) {
	result := TypeResult{ObjectType: objectType}
	var drifts []Drift

	record := func(id, kind string) error {
		drift := Drift{
			ObjectType: objectType,
			ObjectID:   id,
			Kind:       kind,
		}
		result.Drifted++
		if opts.Repair {
			if err := r.repair(ctx, objectType, id); err != nil {
				return fmt.Errorf("repair %s %s: %w", objectType, id, err)
			}
			drift.Repaired = true
			result.Repaired++
		}
		drifts = append(drifts, drift)
		return nil
	}

	from := opts.Since
	if !opts.Full && from == nil {
		windowFrom := time.Now().Add(-quickWindow)
		from = &windowFrom
	}
	checked, err := r.walkCompare(ctx, objectType, from, record)
	if err != nil {
		return result, nil, err
	}
	result.Checked += checked

	if !opts.Full {
		checked, err := r.spotCheck(ctx, objectType, opts.SpotChecks, from, record)
		if err != nil {
			return result, nil, err
		}
		result.Checked += checked
	}
	return result, drifts, nil
}

// walkCompare pages the type's list API, comparing every listed object to
// its mirror row and every live mirror row in the window back to the walk.
func (r *Runner) walkCompare(ctx context.Context, objectType string, from *time.Time, record func(id, kind string) error) (checked int, err error) {
	path, ok := stripeapi.CollectionPath(objectType)
	if !ok {
		return 0, fmt.Errorf("no collection path for %s", objectType)
	}
	query := url.Values{"limit": {strconv.Itoa(stripeapi.MaxPageLimit)}}
	if from != nil {
		query.Set("created[gte]", strconv.FormatInt(from.Unix(), 10))
	}
	if objectType == stripeapi.ObjectSubscription {
		// the default listing omits canceled subscriptions; the mirror
		// keeps them, so the walk must too
		query.Set("status", "all")
	}

	seen := make(map[string]bool)
	cursor := ""
	for {
		if cursor != "" {
			query.Set("starting_after", cursor)
		}
		page, err := r.client.List(ctx, stripeapi.PriorityVerify, path, query)
		if err != nil {
			return checked, err
		}
		if len(page.Data) == 0 {
			break
		}
		for _, raw := range page.Data {
			var envelope struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.ID == "" {
				return checked, fmt.Errorf("listed object without id: %w", err)
			}
			seen[envelope.ID] = true
			checked++
			kind, drifted, err := r.compareRow(ctx, objectType, envelope.ID, raw)
			if err != nil {
				return checked, err
			}
			if drifted {
				if err := record(envelope.ID, kind); err != nil {
					return checked, err
				}
			}
		}
		if !page.HasMore {
			break
		}
		if cursor, err = stripeapi.LastID(page); err != nil {
			return checked, err
		}
	}

	orphans, err := r.orphanedIDs(ctx, objectType, from, seen)
	if err != nil {
		return checked, err
	}
	for _, id := range orphans {
		checked++
		if err := record(id, KindOrphaned); err != nil {
			return checked, err
		}
	}
	return checked, nil
}

// compareRow diffs one listed object against its mirror row.
func (r *Runner) compareRow(ctx context.Context, objectType, id string, upstream []byte) (kind string, drifted bool, err error) {
	table, ok := mirror.Table(objectType)
	if !ok {
		return "", false, fmt.Errorf("no mirror table for %s", objectType)
	}
	var stored []byte
	var isDeleted bool
	err = r.pool.QueryRow(ctx,
		`SELECT data, is_deleted FROM `+table+` WHERE id = $1`, id).Scan(&stored, &isDeleted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return KindMissing, true, nil
	case err != nil:
		return "", false, err
	case isDeleted:
		return KindMissing, true, nil
	case !jsonEqual(stored, upstream):
		return KindStale, true, nil
	}
	return "", false, nil
}

// orphanedIDs returns live mirror ids in the walked window that the walk
// never listed: objects Stripe no longer has.
func (r *Runner) orphanedIDs(ctx context.Context, objectType string, from *time.Time, seen map[string]bool) ([]string, error) {
	table, ok := mirror.Table(objectType)
	if !ok {
		return nil, fmt.Errorf("no mirror table for %s", objectType)
	}
	sql := `SELECT id FROM ` + table + ` WHERE NOT is_deleted`
	var args []any
	if from != nil {
		sql += ` AND created >= $1`
		args = append(args, *from)
	}
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orphans []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if !seen[id] {
			orphans = append(orphans, id)
		}
	}
	return orphans, rows.Err()
}

// spotCheck fetches a random sample of live mirror rows by id and compares
// each against Stripe, covering history the windowed walk skipped.
func (r *Runner) spotCheck(ctx context.Context, objectType string, sample int, walkedFrom *time.Time, record func(id, kind string) error) (checked int, err error) {
	table, ok := mirror.Table(objectType)
	if !ok {
		return 0, fmt.Errorf("no mirror table for %s", objectType)
	}
	sql := `SELECT id FROM ` + table + ` WHERE NOT is_deleted`
	var args []any
	if walkedFrom != nil {
		// the walk already compared the window exhaustively; rows without
		// a created time can only ever be covered here
		sql += ` AND (created < $1 OR created IS NULL)`
		args = append(args, *walkedFrom)
	}
	sql += ` ORDER BY random() LIMIT ` + strconv.Itoa(sample)
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, id := range ids {
		checked++
		upstream, err := r.client.GetObject(ctx, stripeapi.PriorityVerify, objectType, id)
		var notFound *stripeapi.NotFoundError
		switch {
		case errors.As(err, &notFound), err == nil && stripeapi.IsDeletionStub(upstream):
			if err := record(id, KindOrphaned); err != nil {
				return checked, err
			}
			continue
		case err != nil:
			return checked, err
		}
		var stored []byte
		if err := r.pool.QueryRow(ctx,
			`SELECT data FROM `+table+` WHERE id = $1`, id).Scan(&stored); err != nil {
			return checked, err
		}
		if !jsonEqual(stored, upstream) {
			if err := record(id, KindStale); err != nil {
				return checked, err
			}
		}
	}
	return checked, nil
}

// repair re-fetches one object and applies its current truth under the
// per-object lock, exactly the apply path's semantics: a 404 soft-deletes,
// anything else upserts fresh state.
func (r *Runner) repair(ctx context.Context, objectType, id string) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if err := mirror.LockObject(ctx, tx, objectType, id); err != nil {
			return err
		}
		raw, err := r.client.GetObject(ctx, stripeapi.PriorityVerify, objectType, id)
		var notFound *stripeapi.NotFoundError
		switch {
		case errors.As(err, &notFound), err == nil && stripeapi.IsDeletionStub(raw):
			// gone upstream, whether as a 404 or as Stripe's 200 deletion
			// stub; the mirror keeps its history under a soft delete
			if err := mirror.SoftDeleteObject(ctx, tx, objectType, id); err != nil {
				return err
			}
		case err != nil:
			return err
		case objectType == stripeapi.ObjectSubscription:
			if err := mirror.UpsertSubscription(ctx, tx, r.client, stripeapi.PriorityVerify, id, raw); err != nil {
				return err
			}
		default:
			if err := mirror.UpsertObject(ctx, tx, objectType, id, raw); err != nil {
				return err
			}
		}
		if err := db.New(tx).UpsertObjectState(ctx, db.UpsertObjectStateParams{
			ObjectType: objectType,
			ObjectID:   id,
			SyncSource: mirror.SyncSourceRepair,
		}); err != nil {
			return err
		}
		return mirror.NotifyChange(ctx, tx, objectType, id)
	})
}

// jsonEqual compares two JSON documents structurally, so formatting and
// key order differences never read as drift.
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
