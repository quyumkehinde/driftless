// Package sweep finds events Stripe generated that were never received:
// misconfigured endpoints, load balancers eating deliveries, downtime past
// the webhook retry window. It walks the events API on a schedule, records
// what was missed, and enqueues the repairs.
package sweep

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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// grace keeps the window's trailing edge behind now, so in-flight webhook
// deliveries are not misread as gaps.
const grace = 60 * time.Second

// cliffWarnAge is how stale the checkpoint may grow before event-level
// recovery stops being trustworthy: the events API retains roughly thirty
// days, and warning at twenty-five leaves time to act.
const cliffWarnAge = 25 * 24 * time.Hour

// Sweep row statuses; only done rows advance the checkpoint.
const (
	statusDone   = "done"
	statusFailed = "failed"
)

// Metrics holds the sweeper's prometheus instruments.
type Metrics struct {
	Sweeps    *prometheus.CounterVec
	GapEvents prometheus.Counter
	LastSweep prometheus.Gauge
	GapRisk   prometheus.Gauge
}

// NewMetrics registers the sweeper metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Sweeps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_sweeps_total",
			Help: "Sweep passes by result.",
		}, []string{"result"}),
		GapEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driftless_gap_events_total",
			Help: "Events found by sweeps that were never delivered.",
		}),
		LastSweep: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "driftless_last_sweep_timestamp",
			Help: "Unix time of the last successful sweep.",
		}),
		GapRisk: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "driftless_sweep_gap_risk",
			Help: "1 when the checkpoint is close to the event retention cliff.",
		}),
	}
	reg.MustRegister(m.Sweeps, m.GapEvents, m.LastSweep, m.GapRisk)
	return m
}

// Sweeper walks the events API and records what never arrived.
type Sweeper struct {
	pool    *pgxpool.Pool
	client  *stripeapi.Client
	queue   *queue.Queue
	logger  *slog.Logger
	metrics *Metrics

	overlap          time.Duration
	firstRunLookback time.Duration

	// now is replaceable so window and cliff tests control time.
	now func() time.Time
}

// New wires a sweeper. metrics may be nil.
func New(pool *pgxpool.Pool, client *stripeapi.Client, q *queue.Queue, logger *slog.Logger, metrics *Metrics, overlap, firstRunLookback time.Duration) *Sweeper {
	return &Sweeper{
		pool:             pool,
		client:           client,
		queue:            q,
		logger:           obs.WithComponent(logger, "sweep"),
		metrics:          metrics,
		overlap:          overlap,
		firstRunLookback: firstRunLookback,
		now:              time.Now,
	}
}

// Result summarizes one sweep pass.
type Result struct {
	EventsSeen int
	GapsFound  int
}

// RunOnce performs one sweep pass: compute the window, walk the events
// list, record and enqueue anything never seen, and advance the
// checkpoint only on success.
func (s *Sweeper) RunOnce(ctx context.Context) (Result, error) {
	windowFrom, windowTo, err := s.window(ctx)
	if err != nil {
		return Result{}, err
	}
	// The cliff is a property of the window about to be swept: a
	// checkpoint older than the retention cliff means part of this window
	// may already be gone from the events API, and a successful sweep
	// would mask that. Evaluate before walking.
	s.checkCliff(windowFrom)

	sweepRow, err := db.New(s.pool).CreateSweep(ctx, db.CreateSweepParams{
		WindowFrom: windowFrom, WindowTo: windowTo,
	})
	if err != nil {
		return Result{}, err
	}

	result, sweepErr := s.walk(ctx, sweepRow.ID, windowFrom, windowTo)
	status := statusDone
	if sweepErr != nil {
		status = statusFailed
	}
	if err := db.New(s.pool).FinishSweep(context.WithoutCancel(ctx), db.FinishSweepParams{
		ID: sweepRow.ID, Status: status,
		EventsSeen: int32(result.EventsSeen), GapsFound: int32(result.GapsFound),
	}); err != nil && sweepErr == nil {
		sweepErr = err
	}

	if s.metrics != nil {
		s.metrics.Sweeps.WithLabelValues(status).Inc()
		if sweepErr == nil {
			s.metrics.LastSweep.Set(float64(s.now().Unix()))
		}
	}
	if sweepErr != nil {
		return result, fmt.Errorf("sweep %d: %w", sweepRow.ID, sweepErr)
	}

	s.checkDeliveryOutage(ctx, result, windowFrom, windowTo)
	return result, nil
}

// window computes the sweep bounds: from the last successful checkpoint
// minus overlap (first run: the configured lookback), to now minus grace.
func (s *Sweeper) window(ctx context.Context) (from, to time.Time, err error) {
	to = s.now().Add(-grace)
	last, err := db.New(s.pool).GetLastSuccessfulSweep(ctx)
	switch {
	case err == nil:
		from = last.WindowTo.Add(-s.overlap)
	case errors.Is(err, pgx.ErrNoRows):
		from = s.now().Add(-s.firstRunLookback)
	default:
		return time.Time{}, time.Time{}, err
	}
	if from.After(to) {
		from = to
	}
	return from, to, nil
}

// walk pages the events list newest to oldest across the window, storing
// and enqueueing every event never seen before.
func (s *Sweeper) walk(ctx context.Context, sweepID int64, from, to time.Time) (Result, error) {
	var result Result
	query := url.Values{
		"limit":        {strconv.Itoa(stripeapi.MaxPageLimit)},
		"created[gte]": {strconv.FormatInt(from.Unix(), 10)},
		"created[lte]": {strconv.FormatInt(to.Unix(), 10)},
	}
	for _, pattern := range apply.SubscribedEventTypes() {
		query.Add("types[]", pattern)
	}

	var cursor string
	for {
		if cursor != "" {
			query.Set("starting_after", cursor)
		}
		page, err := s.client.List(ctx, stripeapi.PrioritySweep, "/v1/events", query)
		if err != nil {
			return result, err
		}
		if len(page.Data) == 0 {
			return result, nil
		}
		for _, raw := range page.Data {
			result.EventsSeen++
			gap, err := s.recordIfMissing(ctx, sweepID, raw)
			if err != nil {
				return result, err
			}
			if gap {
				result.GapsFound++
				if s.metrics != nil {
					s.metrics.GapEvents.Inc()
				}
			}
		}
		if !page.HasMore {
			return result, nil
		}
		var lastID struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(page.Data[len(page.Data)-1], &lastID); err != nil {
			return result, err
		}
		cursor = lastID.ID
	}
}

// recordIfMissing stores one listed event if it was never seen, records
// the gap, and enqueues its repair, all in one transaction.
func (s *Sweeper) recordIfMissing(ctx context.Context, sweepID int64, raw json.RawMessage) (gap bool, err error) {
	var event struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		APIVersion string `json:"api_version"`
		Created    int64  `json:"created"`
		Livemode   bool   `json:"livemode"`
	}
	if err := json.Unmarshal(raw, &event); err != nil || event.ID == "" {
		return false, fmt.Errorf("listed event without id: %w", err)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		params := db.InsertEventParams{
			EventID:  event.ID,
			Type:     event.Type,
			Created:  time.Unix(event.Created, 0).UTC(),
			Source:   string(mirror.EventSourceSweep),
			Payload:  []byte(raw),
			Livemode: event.Livemode,
		}
		if event.APIVersion != "" {
			params.ApiVersion = &event.APIVersion
		}
		rows, err := db.New(tx).InsertEvent(ctx, params)
		if err != nil {
			return err
		}
		if rows == 0 {
			return nil // already seen: delivered, or found by an overlapping sweep
		}
		gap = true

		if err := db.New(tx).InsertGap(ctx, db.InsertGapParams{
			EventID:      event.ID,
			SweepID:      sweepID,
			EventCreated: time.Unix(event.Created, 0).UTC(),
		}); err != nil {
			return err
		}

		target, known, err := apply.ResolveEvent(event.Type, raw)
		if err != nil {
			s.logger.Warn("gap event has no resolvable object", "event_id", event.ID, "error", err)
			return nil
		}
		if !known {
			return nil // stored, counted as a gap; nothing to apply
		}
		created := time.Unix(event.Created, 0).UTC()
		_, _, err = s.queue.Enqueue(ctx, tx, queue.EnqueueParams{
			ObjectType:   string(target.ObjectType),
			ObjectID:     target.ObjectID,
			EventID:      event.ID,
			EventCreated: created,
		})
		return err
	})
	return gap, err
}

// checkDeliveryOutage raises the loudest possible signal for the
// misconfigured-endpoint class: Stripe is generating events, none are
// arriving as webhooks.
func (s *Sweeper) checkDeliveryOutage(ctx context.Context, result Result, from, to time.Time) {
	if result.GapsFound == 0 {
		return
	}
	arrived, err := db.New(s.pool).CountWebhookEventsBetween(ctx, db.CountWebhookEventsBetweenParams{
		ReceivedAt: from, ReceivedAt_2: to,
	})
	if err != nil {
		s.logger.Error("delivery outage check failed", "error", err)
		return
	}
	if arrived == 0 {
		obs.Critical(s.logger,
			"Stripe is generating events but none are arriving as webhooks; check your webhook endpoint URL",
			"gaps_found", result.GapsFound, "window_from", from, "window_to", to)
	}
}

// checkCliff warns while event-level recovery is still possible: past the
// retention cliff, only a backfill can recover the missed changes. It
// judges the window about to be swept, so a stale checkpoint alarms
// before a successful sweep can freshen it away.
func (s *Sweeper) checkCliff(windowFrom time.Time) {
	age := s.now().Sub(windowFrom)
	atRisk := age > cliffWarnAge
	if s.metrics != nil {
		if atRisk {
			s.metrics.GapRisk.Set(1)
		} else {
			s.metrics.GapRisk.Set(0)
		}
	}
	if atRisk {
		obs.Critical(s.logger,
			"sweep window reaches back further than the event retention cliff; run driftless backfill --since to recover",
			"window_age_days", int(age.Hours()/24))
	}
}
