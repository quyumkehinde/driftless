package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// seedLargeAccount fills fakestripe with a realistic account shape:
// customers with mixed-state subscriptions, items, invoices, and charges.
func seedLargeAccount(t *testing.T, fs *fakestripe.Server, customers int) (objects int) {
	t.Helper()
	fs.Put("product", "prod_main", map[string]any{"name": "Pro", "active": true}, "product.created")
	fs.Put("price", "price_main", map[string]any{
		"product": "prod_main", "active": true, "currency": "usd", "unit_amount": 4900, "type": "recurring",
	}, "price.created")
	objects = 2

	for i := range customers {
		customerID := fmt.Sprintf("cus_%05d", i)
		subID := fmt.Sprintf("sub_%05d", i)
		status := []string{"active", "canceled", "trialing", "past_due"}[i%4]
		item := map[string]any{
			"id": fmt.Sprintf("si_%05d", i), "object": "subscription_item",
			"subscription": subID, "quantity": 1, "price": map[string]any{"id": "price_main"},
		}
		fs.Put("customer", customerID, map[string]any{"email": fmt.Sprintf("c%05d@x.y", i)}, "customer.created")
		fs.Put("subscription", subID, map[string]any{
			"customer": customerID, "status": status,
			"items": map[string]any{"data": []any{item}, "has_more": false},
		}, "customer.subscription.created")
		fs.Put("invoice", fmt.Sprintf("in_%05d", i), map[string]any{
			"customer": customerID, "subscription": subID, "status": "paid",
			"total": 4900, "amount_paid": 4900, "amount_due": 0, "currency": "usd",
		}, "invoice.paid")
		fs.Put("charge", fmt.Sprintf("ch_%05d", i), map[string]any{
			"customer": customerID, "status": "succeeded", "amount": 4900,
			"amount_refunded": 0, "currency": "usd", "paid": true, "refunded": false,
		}, "charge.succeeded")
		objects += 5 // customer, subscription, item, invoice, charge
	}
	return objects
}

// verifyMirrorMatchesStore compares every seeded object byte-for-byte
// against fakestripe's store, the modulo-bookkeeping byte-exactness the
// milestone acceptance demands.
func verifyMirrorMatchesStore(t *testing.T, pool *pgxpool.Pool, fs *fakestripe.Server, customers int) {
	t.Helper()
	ctx := context.Background()

	for table, want := range map[string]int{
		"stripe.customers":          customers,
		"stripe.subscriptions":      customers,
		"stripe.subscription_items": customers,
		"stripe.invoices":           customers,
		"stripe.charges":            customers,
	} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE NOT is_deleted`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("%s = %d rows, want %d", table, n, want)
		}
	}

	// spot-check byte parity on a deterministic sample across types
	rng := rand.New(rand.NewPCG(11, 0))
	for range min(customers, 50) {
		i := rng.IntN(customers)
		for objectType, id := range map[string]string{
			"customer":     fmt.Sprintf("cus_%05d", i),
			"subscription": fmt.Sprintf("sub_%05d", i),
			"invoice":      fmt.Sprintf("in_%05d", i),
			"charge":       fmt.Sprintf("ch_%05d", i),
		} {
			table := map[string]string{
				"customer": "stripe.customers", "subscription": "stripe.subscriptions",
				"invoice": "stripe.invoices", "charge": "stripe.charges",
			}[objectType]
			var data []byte
			if err := pool.QueryRow(ctx, `SELECT data FROM `+table+` WHERE id = $1`, id).Scan(&data); err != nil {
				t.Fatalf("%s %s: %v", table, id, err)
			}
			want, _ := fs.Object(objectType, id)
			wantJSON, _ := json.Marshal(want)
			var got, expected map[string]any
			_ = json.Unmarshal(data, &got)
			_ = json.Unmarshal(wantJSON, &expected)
			if !reflect.DeepEqual(got, expected) {
				t.Errorf("%s %s diverged from store", objectType, id)
			}
		}
	}
}

// TestBackfillCrashResume kills the backfill process without warning,
// several times, and requires the resumed run to converge to the same
// byte-exact mirror an uninterrupted run produces. Set
// DRIFTLESS_ACCEPTANCE=1 for the full 100k-object shape.
func TestBackfillCrashResume(t *testing.T) {
	customers, kills, completeWithin := 60, 3, 3*time.Minute
	if os.Getenv("DRIFTLESS_ACCEPTANCE") == "1" {
		// 100k+ objects at 5 per customer; the shallow payment-methods
		// task alone is ~15k per-customer calls at the 100 rps ceiling
		customers, kills, completeWithin = 20000, 3, 12*time.Minute
	}

	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)
	ctx := context.Background()
	objects := seedLargeAccount(t, fs, customers)
	t.Logf("seeded %d objects", objects)

	// first attempt plus kill -9s at random moments, resuming each time
	rng := rand.New(rand.NewPCG(13, 0))
	proc := startCLI(t, binary, connString, fs.URL(), "backfill", "--full")

	// the run row must exist before there is anything to kill
	var runID int64
	waitFor(t, 15*time.Second, "backfill run row", func() bool {
		return pool.QueryRow(ctx,
			`SELECT id FROM driftless.backfill_runs ORDER BY id LIMIT 1`).Scan(&runID) == nil
	})

	runStatus := func() string {
		var status string
		_ = pool.QueryRow(ctx,
			`SELECT status FROM driftless.backfill_runs WHERE id = $1`, runID).Scan(&status)
		return status
	}

	for kill := range kills {
		time.Sleep(time.Duration(200+rng.IntN(600)) * time.Millisecond)
		if runStatus() == "done" {
			t.Logf("run finished before kill %d; %d kills landed", kill+1, kill)
			break
		}
		proc.Kill9(t)
		t.Logf("kill %d done, resuming run %d", kill+1, runID)
		proc = startCLI(t, binary, connString, fs.URL(),
			"backfill", "--resume", strconv.FormatInt(runID, 10))
	}

	// success is judged by database state, not process exit: the final
	// driver must converge the run to done
	waitFor(t, completeWithin, "backfill run to complete", func() bool {
		return runStatus() == "done"
	})

	// exactly one run ever existed
	var runs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM driftless.backfill_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Errorf("runs = %d, want exactly 1", runs)
	}

	verifyMirrorMatchesStore(t, pool, fs, customers)
}
