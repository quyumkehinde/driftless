package verify

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// startVerify wires a runner against a migrated database and a seeded
// fakestripe, with the mirror synced to the double's state.
func startVerify(t *testing.T) (*Runner, *pgxpool.Pool, *fakestripe.Server) {
	t.Helper()
	pool := testpg.Start(t)
	fs := fakestripe.New(t, "whsec_verify_test")
	limiter := stripeapi.NewLimiter(50)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_verify", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRunner(pool, client, logger, nil), pool, fs
}

// seed puts an object in the double and mirrors it, using the repair path
// as the writer so both sides agree byte for byte.
func seed(t *testing.T, r *Runner, fs *fakestripe.Server, objectType stripeapi.ObjectType, id string, obj map[string]any, eventType string) {
	t.Helper()
	fs.Put(objectType, id, obj, eventType)
	if err := r.repair(t.Context(), objectType, id); err != nil {
		t.Fatalf("seed %s %s: %v", objectType, id, err)
	}
}

func seedAccount(t *testing.T, r *Runner, fs *fakestripe.Server) {
	t.Helper()
	seed(t, r, fs, "product", "prod_1", map[string]any{"name": "Pro", "active": true}, "product.created")
	seed(t, r, fs, "price", "price_1", map[string]any{"product": "prod_1", "currency": "usd", "unit_amount": 4900}, "price.created")
	seed(t, r, fs, "customer", "cus_1", map[string]any{"email": "a@x.y"}, "customer.created")
	seed(t, r, fs, "customer", "cus_2", map[string]any{"email": "b@x.y"}, "customer.created")
	item := map[string]any{
		"id": "si_1", "object": "subscription_item",
		"subscription": "sub_1", "quantity": 1, "price": map[string]any{"id": "price_1"},
	}
	seed(t, r, fs, "subscription", "sub_1", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": []any{item}, "has_more": false},
	}, "customer.subscription.created")
	seed(t, r, fs, "invoice", "in_1", map[string]any{"customer": "cus_1", "status": "paid", "total": 4900}, "invoice.paid")
}

func TestVerifyCleanMirrorFindsNothing(t *testing.T) {
	r, pool, fs := startVerify(t)
	seedAccount(t, r, fs)

	report, err := r.Run(t.Context(), Options{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Drifted != 0 || report.Repaired != 0 {
		t.Errorf("clean mirror: drifted=%d repaired=%d, want 0", report.Drifted, report.Repaired)
	}
	if report.Checked != 6 {
		t.Errorf("checked = %d, want 6", report.Checked)
	}

	var mode Mode
	var drifted int
	if err := pool.QueryRow(t.Context(),
		`SELECT mode, drifted FROM driftless.verifications ORDER BY id DESC LIMIT 1`).Scan(&mode, &drifted); err != nil {
		t.Fatal(err)
	}
	if mode != ModeFull || drifted != 0 {
		t.Errorf("verification row mode=%q drifted=%d, want full and 0", mode, drifted)
	}
}

func TestVerifyDetectsEveryDriftKind(t *testing.T) {
	r, _, fs := startVerify(t)
	seedAccount(t, r, fs)

	// stale: upstream changed, no event delivered
	fs.Put("customer", "cus_1", map[string]any{"email": "changed@x.y"}, "customer.updated")
	// missing: upstream object the mirror never saw
	fs.Put("customer", "cus_3", map[string]any{"email": "new@x.y"}, "customer.created")
	// orphaned: upstream deleted, mirror still live
	fs.Delete("customer", "cus_2", "customer.deleted")

	report, err := r.Run(t.Context(), Options{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]DriftKind{}
	for _, d := range report.Drifts {
		kinds[d.ObjectID] = d.Kind
	}
	want := map[string]DriftKind{"cus_1": KindStale, "cus_3": KindMissing, "cus_2": KindOrphaned}
	if len(kinds) != len(want) {
		t.Fatalf("drifts = %v, want exactly %v", kinds, want)
	}
	for id, kind := range want {
		if kinds[id] != kind {
			t.Errorf("%s drift kind = %q, want %q", id, kinds[id], kind)
		}
	}
}

func TestVerifyRepairZeroesDrift(t *testing.T) {
	r, pool, fs := startVerify(t)
	seedAccount(t, r, fs)

	fs.Put("customer", "cus_1", map[string]any{"email": "changed@x.y"}, "customer.updated")
	fs.Put("customer", "cus_3", map[string]any{"email": "new@x.y"}, "customer.created")
	fs.Delete("customer", "cus_2", "customer.deleted")

	report, err := r.Run(t.Context(), Options{Full: true, Repair: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Drifted != 3 || report.Repaired != 3 {
		t.Fatalf("drifted=%d repaired=%d, want 3 and 3", report.Drifted, report.Repaired)
	}

	// the repaired mirror matches upstream exactly
	clean, err := r.Run(t.Context(), Options{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if clean.Drifted != 0 {
		t.Errorf("after repair: drifted=%d, want 0; drifts=%v", clean.Drifted, clean.Drifts)
	}

	var email string
	if err := pool.QueryRow(t.Context(),
		`SELECT data->>'email' FROM stripe.customers WHERE id = 'cus_1'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "changed@x.y" {
		t.Errorf("repaired email = %q", email)
	}
	var isDeleted bool
	if err := pool.QueryRow(t.Context(),
		`SELECT is_deleted FROM stripe.customers WHERE id = 'cus_2'`).Scan(&isDeleted); err != nil {
		t.Fatal(err)
	}
	if !isDeleted {
		t.Error("orphan repair must soft-delete the mirror row")
	}
	var syncSource string
	if err := pool.QueryRow(t.Context(),
		`SELECT sync_source FROM driftless.object_state WHERE object_type = 'customer' AND object_id = 'cus_1'`).Scan(&syncSource); err != nil {
		t.Fatal(err)
	}
	if syncSource != "repair" {
		t.Errorf("sync_source = %q, want repair", syncSource)
	}
}

func TestQuickSpotChecksCatchOldDrift(t *testing.T) {
	r, _, fs := startVerify(t)
	seedAccount(t, r, fs)

	// the double's clock sits months in the past, so every object falls
	// outside the quick walk window and only spot-checks can see it
	fs.Put("customer", "cus_1", map[string]any{"email": "changed@x.y"}, "customer.updated")
	fs.Delete("customer", "cus_2", "customer.deleted")

	// a sample at least as large as any table makes the check exhaustive
	report, err := r.Run(t.Context(), Options{SpotChecks: 100})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]DriftKind{}
	for _, d := range report.Drifts {
		kinds[d.ObjectID] = d.Kind
	}
	if kinds["cus_1"] != KindStale || kinds["cus_2"] != KindOrphaned {
		t.Errorf("quick drifts = %v, want cus_1 stale and cus_2 orphaned", kinds)
	}
	if report.Mode != ModeQuick {
		t.Errorf("mode = %q, want quick", report.Mode)
	}
}

func TestQuickSinceWidensTheWalk(t *testing.T) {
	r, _, fs := startVerify(t)
	seedAccount(t, r, fs)

	// missing drift is invisible to spot-checks (nothing to sample) and to
	// the default quick window (created months ago); an explicit since
	// pulls it into the walk
	fs.Put("customer", "cus_3", map[string]any{"email": "new@x.y"}, "customer.created")

	since := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	report, err := r.Run(t.Context(), Options{Since: &since})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range report.Drifts {
		if d.ObjectID == "cus_3" && d.Kind == KindMissing {
			found = true
		}
	}
	if !found {
		t.Errorf("quick with since must find the missing object; drifts = %v", report.Drifts)
	}
}

func TestVerifyIgnoresVolatileStripeFields(t *testing.T) {
	r, _, fs := startVerify(t)
	invoice := map[string]any{
		"customer": "cus_1", "status": "paid", "total": 4900,
		"webhooks_delivered_at": nil,
		"invoice_pdf":           "https://pay.stripe.com/invoice/one/pdf",
		"hosted_invoice_url":    "https://invoice.stripe.com/i/one",
	}
	charge := map[string]any{
		"customer": "cus_1", "status": "succeeded", "amount": 4900,
		"receipt_url": "https://pay.stripe.com/receipts/one?s=ap",
	}
	seed(t, r, fs, "invoice", "in_1", invoice, "invoice.paid")
	seed(t, r, fs, "charge", "ch_1", charge, "charge.succeeded")

	// Stripe regenerates these server-side after every event; the mirror
	// can never converge on them, so they must not read as drift.
	fs.Put("invoice", "in_1", map[string]any{
		"customer": "cus_1", "status": "paid", "total": 4900,
		"webhooks_delivered_at": 1787332358,
		"invoice_pdf":           "https://pay.stripe.com/invoice/two/pdf",
		"hosted_invoice_url":    "https://invoice.stripe.com/i/two",
	}, "")
	fs.Put("charge", "ch_1", map[string]any{
		"customer": "cus_1", "status": "succeeded", "amount": 4900,
		"receipt_url": "https://pay.stripe.com/receipts/two?s=ap",
	}, "")

	report, err := r.Run(t.Context(), Options{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Drifted != 0 {
		t.Errorf("volatile-only changes: drifted=%d, want 0; drifts=%v", report.Drifted, report.Drifts)
	}

	// a real field change underneath the volatile noise still drifts
	fs.Put("charge", "ch_1", map[string]any{
		"customer": "cus_1", "status": "succeeded", "amount": 9900,
		"receipt_url": "https://pay.stripe.com/receipts/three?s=ap",
	}, "")
	report, err = r.Run(t.Context(), Options{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Drifted != 1 || report.Drifts[0].Kind != KindStale || report.Drifts[0].ObjectID != "ch_1" {
		t.Errorf("real change: drifted=%d drifts=%v, want ch_1 stale", report.Drifted, report.Drifts)
	}
}

func TestPlanRejectsUnverifiableTypes(t *testing.T) {
	if _, err := Plan([]stripeapi.ObjectType{"payment_method"}); err == nil {
		t.Error("payment_method must be rejected: Stripe has no account-wide listing to compare")
	}
	if _, err := Plan([]stripeapi.ObjectType{"subscription_item"}); err == nil {
		t.Error("subscription_item must be rejected: verified through its parent subscription")
	}
	planned, err := Plan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != len(Types) {
		t.Errorf("default plan covers %d types, want %d", len(planned), len(Types))
	}
}
