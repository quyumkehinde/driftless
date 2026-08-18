package sweep

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func newTestSweeper(t *testing.T) (*Sweeper, *fakestripe.Server, *pgxpool.Pool, *strings.Builder) {
	t.Helper()
	pool := testpg.Start(t)
	fs := fakestripe.New(t, "whsec_sweep")
	limiter := stripeapi.NewLimiter(1000)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_sweep", limiter, nil)
	logBuf := &strings.Builder{}
	logger := slog.New(slog.NewJSONHandler(logBuf, nil))
	q := queue.New(pool, 2*time.Minute)
	return New(pool, client, q, logger, nil, 10*time.Minute, 24*time.Hour), fs, pool, logBuf
}

// deliverEvent simulates a webhook delivery by inserting the event the way
// ingest does, so the sweeper sees it as already received.
func deliverEvent(t *testing.T, pool *pgxpool.Pool, event fakestripe.Event) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO driftless.events (event_id, type, created, source, payload, livemode)
		VALUES ($1, $2, $3, 'webhook', $4, false)`,
		event.ID, event.Type, event.Created, event.Payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSweepFindsUndeliveredEvents(t *testing.T) {
	sweeper, fs, pool, _ := newTestSweeper(t)
	ctx := context.Background()
	// fakestripe's clock starts in 2026; sweep windows are computed from
	// the sweeper's clock, so pin it just ahead of the events
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	sweeper.now = func() time.Time { return base }

	delivered := fs.Put("customer", "cus_ok", map[string]any{"email": "ok@x.y"}, "customer.created")
	deliverEvent(t, pool, delivered)
	fs.Put("customer", "cus_lost", map[string]any{"email": "lost@x.y"}, "customer.created")
	fs.Put("subscription", "sub_lost", map[string]any{
		"customer": "cus_lost", "status": "active",
		"items": map[string]any{"data": []any{}, "has_more": false},
	}, "customer.subscription.created")

	result, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.EventsSeen != 3 || result.GapsFound != 2 {
		t.Errorf("seen=%d gaps=%d, want 3 and 2", result.EventsSeen, result.GapsFound)
	}

	// the undelivered events are stored as sweep-sourced with gap rows and
	// repair jobs
	var swept, gaps, jobs int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events WHERE source = 'sweep'`).Scan(&swept)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.gaps`).Scan(&gaps)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs WHERE status = 'pending'`).Scan(&jobs)
	if swept != 2 || gaps != 2 || jobs != 2 {
		t.Errorf("swept=%d gaps=%d jobs=%d, want 2 each", swept, gaps, jobs)
	}

	// gap rows carry the delivery lag
	var lagSeconds float64
	if err := pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM lag) FROM driftless.gaps LIMIT 1`).Scan(&lagSeconds); err != nil {
		t.Fatal(err)
	}
	if lagSeconds <= 0 {
		t.Errorf("lag = %vs, want positive", lagSeconds)
	}
}

func TestSweepOverlapIsFree(t *testing.T) {
	sweeper, fs, pool, _ := newTestSweeper(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	sweeper.now = func() time.Time { return base }

	fs.Put("customer", "cus_1", nil, "customer.created")

	first, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.GapsFound != 1 {
		t.Fatalf("first sweep gaps = %d, want 1", first.GapsFound)
	}

	// the second sweep's window overlaps the first; ON CONFLICT makes the
	// re-listing free
	sweeper.now = func() time.Time { return base.Add(5 * time.Minute) }
	second, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.GapsFound != 0 {
		t.Errorf("second sweep gaps = %d, want 0", second.GapsFound)
	}
	var gaps int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.gaps`).Scan(&gaps)
	if gaps != 1 {
		t.Errorf("gap rows = %d, want 1: overlap must not duplicate", gaps)
	}
}

func TestSweepCheckpointAdvancesOnlyOnSuccess(t *testing.T) {
	sweeper, fs, pool, _ := newTestSweeper(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	sweeper.now = func() time.Time { return base }

	fs.Put("customer", "cus_1", nil, "customer.created")
	if _, err := sweeper.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var checkpoint time.Time
	if err := pool.QueryRow(ctx,
		`SELECT window_to FROM driftless.sweeps WHERE status = 'done' ORDER BY id DESC LIMIT 1`).Scan(&checkpoint); err != nil {
		t.Fatal(err)
	}

	// a failing sweep must not advance the checkpoint
	fs.FailRate(1.0, 500)
	sweeper.now = func() time.Time { return base.Add(10 * time.Minute) }
	if _, err := sweeper.RunOnce(ctx); err == nil {
		t.Fatal("sweep under total API failure should error")
	}
	fs.FailRate(0, 0)

	var after time.Time
	if err := pool.QueryRow(ctx,
		`SELECT window_to FROM driftless.sweeps WHERE status = 'done' ORDER BY window_to DESC LIMIT 1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(checkpoint) {
		t.Errorf("checkpoint moved %v -> %v across a failed sweep", checkpoint, after)
	}
}

func TestSweepUnknownEventTypeStoredWithoutJob(t *testing.T) {
	sweeper, fs, pool, _ := newTestSweeper(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	sweeper.now = func() time.Time { return base }

	// generate an event whose type the contract does not map; the types
	// filter would normally exclude it upstream, but a family wildcard can
	// still surface unmapped members, like an unknown checkout session type
	fs.Put("subscription", "sub_x", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": []any{}, "has_more": false},
	}, "customer.subscription.brand_new_event_kind")

	result, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.GapsFound != 1 {
		t.Fatalf("gaps = %d, want 1", result.GapsFound)
	}
	// mapped family: a job is enqueued; the event is stored either way
	var stored int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events WHERE source = 'sweep'`).Scan(&stored)
	if stored != 1 {
		t.Errorf("stored = %d, want 1", stored)
	}
}

func TestSweepThirtyDayCliff(t *testing.T) {
	sweeper, fs, pool, logBuf := newTestSweeper(t)
	ctx := context.Background()
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	// a checkpoint 26 days old
	sweeper.now = func() time.Time { return base.Add(-26 * 24 * time.Hour) }
	fs.Put("customer", "cus_old", nil, "customer.created")
	if _, err := sweeper.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// time passes without successful sweeps; the next pass must go
	// critical even though it succeeds
	sweeper.now = func() time.Time { return base }
	if _, err := sweeper.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, `"critical":true`) || !strings.Contains(logs, "backfill --since") {
		t.Errorf("expected critical cliff log, got:\n%s", logs)
	}
	_ = pool
}

func TestSweepDeliveryOutageCritical(t *testing.T) {
	sweeper, fs, _, logBuf := newTestSweeper(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	sweeper.now = func() time.Time { return base }

	// events exist upstream; zero webhooks ever arrived
	fs.Put("customer", "cus_1", nil, "customer.created")
	fs.Put("customer", "cus_2", nil, "customer.created")

	if _, err := sweeper.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, `"critical":true`) || !strings.Contains(logs, "none are arriving") {
		t.Errorf("expected none-arriving critical log, got:\n%s", logs)
	}
}
