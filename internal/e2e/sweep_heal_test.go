package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestSweeperHealsUndeliveredEvents mutates the account while delivering
// no webhooks at all, the misconfigured-endpoint shape. The sweeper alone
// must find every event and converge the mirror.
func TestSweeperHealsUndeliveredEvents(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	item := map[string]any{
		"id": "si_gap", "object": "subscription_item",
		"subscription": "sub_gap", "quantity": 1, "price": map[string]any{"id": "price_gap"},
	}
	fs.Put("customer", "cus_gap1", map[string]any{"email": "old@x.y"}, "customer.created")
	fs.Put("customer", "cus_gap2", nil, "customer.created")
	fs.Put("subscription", "sub_gap", map[string]any{
		"customer": "cus_gap1", "status": "active",
		"items": map[string]any{"data": []any{item}, "has_more": false},
	}, "customer.subscription.created")
	fs.Put("customer", "cus_gap1", map[string]any{"email": "new@x.y"}, "customer.updated")

	// the double's clock is fixed months in the past, so the first-run
	// lookback must reach back that far for the window to cover the events
	startServe(t, binary, connString, fs.URL(), "",
		"DRIFTLESS_SWEEP_INTERVAL=1s",
		"DRIFTLESS_SWEEP_FIRST_RUN_LOOKBACK=17520h",
	)

	// the acceptance bound is two sweep intervals; the slack is for the
	// container and binary startup, not extra sweeps
	waitFor(t, 30*time.Second, "mirror to converge via sweeps alone", func() bool {
		return countRow(t, pool, `SELECT count(*) FROM stripe.customers WHERE NOT is_deleted`) == 2 &&
			countRow(t, pool, `SELECT count(*) FROM stripe.subscriptions WHERE NOT is_deleted`) == 1
	})

	// every event arrived through the sweeper, and each one is a recorded gap
	if n := countRow(t, pool, `SELECT count(*) FROM driftless.events WHERE source = 'sweep'`); n != 4 {
		t.Errorf("sweep-sourced events = %d, want 4", n)
	}
	if n := countRow(t, pool, `SELECT count(*) FROM driftless.events WHERE source = 'webhook'`); n != 0 {
		t.Errorf("webhook-sourced events = %d, want 0", n)
	}
	if n := countRow(t, pool, `SELECT count(*) FROM driftless.gaps`); n != 4 {
		t.Errorf("gap rows = %d, want 4", n)
	}

	// the repair applied fresh state, not the stale event payload
	var email string
	if err := pool.QueryRow(t.Context(),
		`SELECT data->>'email' FROM stripe.customers WHERE id = 'cus_gap1'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "new@x.y" {
		t.Errorf("cus_gap1 email = %q, want the latest state", email)
	}

	// later sweeps advance the checkpoint without re-recording anything
	waitFor(t, 10*time.Second, "a second completed sweep", func() bool {
		return countRow(t, pool, `SELECT count(*) FROM driftless.sweeps WHERE status = 'done'`) >= 2
	})
	if n := countRow(t, pool, `SELECT count(*) FROM driftless.events`); n != 4 {
		t.Errorf("events after second sweep = %d, want still 4", n)
	}
}

// TestThirtyDayCliff plants a checkpoint just short of the events API
// retention limit and requires serve to raise the gap-risk gauge: past
// the cliff, sweeps can no longer prove nothing was missed.
func TestThirtyDayCliff(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	// a successful sweep whose checkpoint is 26 days stale
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.sweeps (window_from, window_to, status, finished_at)
		VALUES (now() - interval '27 days', now() - interval '26 days', 'done', now() - interval '26 days')`); err != nil {
		t.Fatal(err)
	}

	proc := startServe(t, binary, connString, fs.URL(), "",
		"DRIFTLESS_SWEEP_INTERVAL=1s",
	)

	waitFor(t, 15*time.Second, "gap risk gauge to raise", func() bool {
		resp, err := http.Get(proc.MetricsURL + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		return strings.Contains(string(body), "driftless_sweep_gap_risk 1")
	})
}
