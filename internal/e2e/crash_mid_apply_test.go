package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestCrashMidApplyWithRedelivery kills the process inside the apply
// transaction, then restarts with Stripe-style redeliveries racing in.
// The rollback plus the reaper plus idempotent re-apply must leave exactly
// one correct subscription row; duplicate rows here were a real incident
// class in node receivers.
func TestCrashMidApplyWithRedelivery(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	event := fs.Put("subscription", "sub_crash", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": []any{map[string]any{
			"id": "si_c1", "object": "subscription_item", "subscription": "sub_crash",
			"quantity": 1, "price": map[string]any{"id": "price_c"},
		}}, "has_more": false},
	}, "customer.subscription.created")

	// short visibility timeout so the reaper notices the dead claim fast
	shortClaim := []string{"DRIFTLESS_WORKERS_VISIBILITY_TIMEOUT=2s"}

	proc := startServe(t, binary, connString, fs.URL(), "apply.before-commit", shortClaim...)
	statuses := fs.Deliver(t, proc.IngestURL, event.ID)
	if statuses[0] != http.StatusOK {
		t.Fatalf("ingest status = %d", statuses[0])
	}
	// the worker claims the job and dies inside the apply transaction
	proc.WaitExit(t)

	mirrored := countRow(t, pool, `SELECT count(*) FROM stripe.subscriptions`)
	if mirrored != 0 {
		t.Fatalf("mirror rows after crash = %d, want 0: the apply transaction must roll back", mirrored)
	}

	// restart clean; Stripe-style duplicate redeliveries race in
	clean := startServe(t, binary, connString, fs.URL(), "", shortClaim...)
	for _, status := range fs.DeliverConcurrent(t, clean.IngestURL, event.ID, 3) {
		if status != http.StatusOK {
			t.Fatalf("redelivery status = %d", status)
		}
	}

	waitFor(t, 30*time.Second, "subscription to mirror after recovery", func() bool {
		return countRow(t, pool, `SELECT count(*) FROM stripe.subscriptions WHERE id = 'sub_crash' AND status = 'active'`) == 1
	})
	waitForDrain(t, pool)

	// exactly one of everything, no duplicate side effects
	events := countRow(t, pool, `SELECT count(*) FROM driftless.events`)
	subs := countRow(t, pool, `SELECT count(*) FROM stripe.subscriptions`)
	items := countRow(t, pool, `SELECT count(*) FROM stripe.subscription_items WHERE NOT is_deleted`)
	if events != 1 || subs != 1 || items != 1 {
		t.Errorf("events=%d subscriptions=%d items=%d, want exactly 1 of each", events, subs, items)
	}
}
