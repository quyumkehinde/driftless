package backfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func newTestRunner(t *testing.T, progress Progress) (*Runner, *fakestripe.Server, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Start(t)
	fs := fakestripe.New(t, "whsec_backfill")
	limiter := stripeapi.NewLimiter(1000)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_backfill", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRunner(pool, client, logger, nil, progress), fs, pool
}

// seedAccount populates fakestripe with a small account: products, prices,
// customers, subscriptions in several states, invoices.
func seedAccount(t *testing.T, fs *fakestripe.Server, customers int) {
	t.Helper()
	fs.Put("product", "prod_1", map[string]any{"name": "Pro", "active": true}, "product.created")
	fs.Put("price", "price_1", map[string]any{
		"product": "prod_1", "active": true, "currency": "usd", "unit_amount": 4900, "type": "recurring",
	}, "price.created")

	for i := range customers {
		customerID := fmt.Sprintf("cus_%03d", i)
		fs.Put("customer", customerID, map[string]any{"email": fmt.Sprintf("c%03d@x.y", i)}, "customer.created")

		status := []string{"active", "canceled", "trialing"}[i%3]
		subID := fmt.Sprintf("sub_%03d", i)
		item := map[string]any{
			"id": fmt.Sprintf("si_%03d", i), "object": "subscription_item",
			"subscription": subID, "quantity": 1, "price": map[string]any{"id": "price_1"},
		}
		fs.Put("subscription", subID, map[string]any{
			"customer": customerID, "status": status,
			"items": map[string]any{"data": []any{item}, "has_more": false},
		}, "customer.subscription.created")

		fs.Put("payment_method", fmt.Sprintf("pm_%03d", i), map[string]any{
			"customer": customerID, "type": "card",
			"card": map[string]any{"brand": "visa", "last4": "4242"},
		}, "payment_method.attached")

		fs.Put("invoice", fmt.Sprintf("in_%03d", i), map[string]any{
			"customer": customerID, "subscription": subID, "status": "paid",
			"total": 4900, "amount_paid": 4900, "amount_due": 0, "currency": "usd",
		}, "invoice.paid")
	}
}

func TestBackfillIncludesCanceledSubscriptions(t *testing.T) {
	// The default subscriptions listing omits canceled subscriptions, and
	// a backfill that trusts it silently loses cancellation history. That
	// was a real bug in Stripe's own sync tool.
	runner, fs, pool := newTestRunner(t, nil)
	ctx := context.Background()
	seedAccount(t, fs, 9) // 3 active, 3 canceled, 3 trialing

	if _, err := runner.Start(ctx, Options{RequestedBy: "cli"}); err != nil {
		t.Fatal(err)
	}

	var total, canceled int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE status = 'canceled')
		FROM stripe.subscriptions WHERE NOT is_deleted`).Scan(&total, &canceled); err != nil {
		t.Fatal(err)
	}
	if total != 9 || canceled != 3 {
		t.Errorf("subscriptions=%d canceled=%d, want 9 and 3", total, canceled)
	}
}

func TestBackfillMirrorsAccount(t *testing.T) {
	runner, fs, pool := newTestRunner(t, nil)
	ctx := context.Background()
	seedAccount(t, fs, 9)

	runID, err := runner.Start(ctx, Options{RequestedBy: "cli"})
	if err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for table, want := range map[string]int{
		"stripe.products":           1,
		"stripe.prices":             1,
		"stripe.customers":          9,
		"stripe.subscriptions":      9,
		"stripe.subscription_items": 9,
		"stripe.invoices":           9,
		"stripe.payment_methods":    6, // shallow: only live-subscription customers
	} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE NOT is_deleted`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[table] = n
		if n != want {
			t.Errorf("%s = %d rows, want %d", table, n, want)
		}
	}

	// sync_source recorded as backfill
	var backfilled int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM driftless.object_state WHERE sync_source = 'backfill'`).Scan(&backfilled); err != nil {
		t.Fatal(err)
	}
	if backfilled == 0 {
		t.Error("object_state must record backfill as sync_source")
	}

	// the run is done with per-task accounting
	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM driftless.backfill_runs WHERE id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Errorf("run status = %q, want done", status)
	}
	var undone int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM driftless.backfill_tasks WHERE run_id = $1 AND status != 'done'`, runID).Scan(&undone); err != nil {
		t.Fatal(err)
	}
	if undone != 0 {
		t.Errorf("%d tasks not done", undone)
	}
}

func TestBackfillFreshnessGuard(t *testing.T) {
	runner, fs, pool := newTestRunner(t, nil)
	ctx := context.Background()

	// cus_fresh was updated by an event AFTER the backfill run starts:
	// the mirror carries newer truth than the list page will
	fs.Put("customer", "cus_fresh", map[string]any{"email": "stale-page@x.y"}, "customer.created")
	fs.Put("customer", "cus_plain", map[string]any{"email": "plain@x.y"}, "customer.created")

	if _, err := pool.Exec(ctx, `
		INSERT INTO stripe.customers (id, data) VALUES
		('cus_fresh', '{"id":"cus_fresh","object":"customer","email":"fresh-event@x.y"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO driftless.object_state (object_type, object_id, last_synced_at, last_event_created, last_event_id, sync_source)
		VALUES ('customer', 'cus_fresh', now(), now() + interval '1 hour', 'evt_future', 'fetch')`); err != nil {
		t.Fatal(err)
	}

	if _, err := runner.Start(ctx, Options{RequestedBy: "cli", Types: []string{"customer"}}); err != nil {
		t.Fatal(err)
	}

	var freshEmail, plainEmail string
	if err := pool.QueryRow(ctx, `SELECT email FROM stripe.customers WHERE id = 'cus_fresh'`).Scan(&freshEmail); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT email FROM stripe.customers WHERE id = 'cus_plain'`).Scan(&plainEmail); err != nil {
		t.Fatal(err)
	}
	if freshEmail != "fresh-event@x.y" {
		t.Errorf("guarded object clobbered: email = %q", freshEmail)
	}
	if plainEmail != "plain@x.y" {
		t.Errorf("unguarded object not written: email = %q", plainEmail)
	}
}

func TestBackfillResumeAfterInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// interrupt the run via the progress hook once customers start landing
	var runner *Runner
	var fs *fakestripe.Server
	var pool *pgxpool.Pool
	runner, fs, pool = newTestRunner(t, func(objectType string, _, _ int64) {
		if objectType == "customer" {
			cancel()
		}
	})
	seedAccount(t, fs, 9)

	runID, err := runner.Start(ctx, Options{RequestedBy: "cli"})
	if err == nil {
		t.Fatal("interrupted run should return an error")
	}

	// the run is still resumable, and resume completes it
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM driftless.backfill_runs WHERE id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("interrupted run status = %q, want running", status)
	}

	if err := runner.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	var customers, subs int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM stripe.customers`).Scan(&customers)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM stripe.subscriptions`).Scan(&subs)
	if customers != 9 || subs != 9 {
		t.Errorf("after resume: customers=%d subscriptions=%d, want 9 and 9", customers, subs)
	}
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM driftless.backfill_runs WHERE id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Errorf("resumed run status = %q, want done", status)
	}
}

func TestPlan(t *testing.T) {
	all, err := Plan(nil)
	if err != nil || !slices.Equal(all, TypeOrder) {
		t.Errorf("Plan(nil) = %v, %v", all, err)
	}
	subset, err := Plan([]string{"invoice", "customer"})
	if err != nil {
		t.Fatal(err)
	}
	// dependency order preserved regardless of request order
	if !slices.Equal(subset, []string{"customer", "invoice"}) {
		t.Errorf("Plan subset = %v, want customer before invoice", subset)
	}
	if _, err := Plan([]string{"subscription_item"}); err == nil {
		t.Error("subscription_item is not listable; Plan must reject it")
	}
	if _, err := Plan([]string{"plan"}); err == nil {
		t.Error("unknown type must be rejected")
	}
}

func TestBackfillSince(t *testing.T) {
	runner, fs, pool := newTestRunner(t, nil)
	ctx := context.Background()

	fs.Put("customer", "cus_old", map[string]any{"email": "old@x.y", "created": 1000000}, "customer.created")
	fs.Put("customer", "cus_new", map[string]any{"email": "new@x.y", "created": 2000000}, "customer.created")

	since := time.Unix(1500000, 0).UTC()
	if _, err := runner.Start(ctx, Options{RequestedBy: "cli", Since: &since, Types: []string{"customer"}}); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM stripe.customers`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("customers = %d, want only the one created after since", n)
	}
}

func TestRunLockPreventsConcurrentDrivers(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	runner, fs, pool := newTestRunner(t, func(_ string, _, _ int64) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	seedAccount(t, fs, 3)

	done := make(chan error, 1)
	var runID int64
	go func() {
		id, err := runner.Start(context.Background(), Options{RequestedBy: "cli"})
		runID = id
		done <- err
	}()
	<-entered // the first driver holds the run lock mid-page

	// a second driver on the same run must be refused, not double-fetch
	second := NewRunner(pool, runner.client, runner.logger, nil, nil)
	err := second.Resume(context.Background(), 1)
	if !errors.Is(err, ErrRunLocked) {
		t.Errorf("second driver err = %v, want ErrRunLocked", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first driver: %v", err)
	}

	// once released, the lock is free: resuming a done run errors on
	// status, not on the lock
	err = second.Resume(context.Background(), runID)
	if err == nil || errors.Is(err, ErrRunLocked) {
		t.Errorf("after completion err = %v, want a not-resumable status error", err)
	}
}

func TestCancelledRunResumesOnlyExplicitly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, fs, pool := newTestRunner(t, func(objectType string, _, _ int64) {
		if objectType == "customer" {
			cancel()
		}
	})
	seedAccount(t, fs, 6)

	runID, err := runner.Start(ctx, Options{RequestedBy: "cli"})
	if err == nil {
		t.Fatal("interrupted run should error")
	}

	// the CLI marks the deliberate stop
	cancelled, err := runner.Cancel(context.Background(), runID)
	if err != nil || !cancelled {
		t.Fatalf("cancel: %v cancelled=%v", err, cancelled)
	}

	// auto_resume must not see it
	resumable, err := db.New(pool).ListResumableRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range resumable {
		if run.ID == runID {
			t.Error("cancelled run listed as resumable: auto_resume would revive it")
		}
	}

	// an explicit resume revives and completes it
	if err := runner.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM driftless.backfill_runs WHERE id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Errorf("status = %q, want done", status)
	}
}

func TestBackfillTerminalErrorFailsFast(t *testing.T) {
	runner, fs, pool := newTestRunner(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fs.Put("customer", "cus_1", nil, "customer.created")
	// a revoked key answers 401 on every call: terminal, must not loop
	fs.FailRate(1.0, 401)

	start := time.Now()
	_, err := runner.Start(ctx, Options{RequestedBy: "cli", Types: []string{"customer"}})
	if err == nil {
		t.Fatal("terminal API error must fail the run")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("terminal error took %v to surface: retry loop did not classify it", elapsed)
	}
	var failed int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM driftless.backfill_tasks WHERE status = 'failed'`).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if failed == 0 {
		t.Error("task must be recorded failed")
	}
}

func TestBackfillRecordsWatermark(t *testing.T) {
	runner, fs, pool := newTestRunner(t, nil)
	ctx := context.Background()

	fs.Put("customer", "cus_w", nil, "customer.created")
	if _, err := runner.Start(ctx, Options{RequestedBy: "cli", Types: []string{"customer"}}); err != nil {
		t.Fatal(err)
	}

	// without a watermark, payload mode would judge any stale delayed
	// event newer than the backfilled snapshot
	var watermark *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT last_event_created FROM driftless.object_state WHERE object_id = 'cus_w'`).Scan(&watermark); err != nil {
		t.Fatal(err)
	}
	if watermark == nil {
		t.Fatal("backfilled object_state must carry the run horizon as watermark")
	}
}
