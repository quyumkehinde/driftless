package e2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// mirrorMatches compares a mirror row's data against fakestripe's own
// store, normalizing both through JSON.
func mirrorMatches(t *testing.T, pool *pgxpool.Pool, fs *fakestripe.Server, table string, objectType stripeapi.ObjectType, id string) {
	t.Helper()
	var data []byte
	err := pool.QueryRow(context.Background(),
		`SELECT data FROM `+table+` WHERE id = $1 AND NOT is_deleted`, id).Scan(&data)
	if err != nil {
		t.Errorf("%s %s: %v", table, id, err)
		return
	}
	want, ok := fs.Object(objectType, id)
	if !ok {
		t.Errorf("%s %s missing upstream", objectType, id)
		return
	}
	wantJSON, _ := json.Marshal(want)
	var got, expected map[string]any
	_ = json.Unmarshal(data, &got)
	_ = json.Unmarshal(wantJSON, &expected)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("%s %s diverged:\n got %v\nwant %v", table, id, got, expected)
	}
}

// TestLifeOfASubscription is the milestone acceptance: the full journey
// from customer to cancellation, delivered shuffled and duplicated, must
// converge every affected table to fakestripe's own store.
func TestLifeOfASubscription(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)
	ctx := context.Background()

	// the journey, in causal order
	fs.Put("customer", "cus_life", map[string]any{"email": "life@x.y", "currency": "usd"}, "customer.created")
	fs.Put("product", "prod_life", map[string]any{"name": "Pro Plan", "active": true}, "product.created")
	fs.Put("price", "price_life", map[string]any{
		"product": "prod_life", "active": true, "currency": "usd", "unit_amount": 4900, "type": "recurring",
		"recurring": map[string]any{"interval": "month"},
	}, "price.created")
	fs.Put("checkout_session", "cs_life", map[string]any{
		"customer": "cus_life", "status": "complete", "mode": "subscription", "amount_total": 4900, "currency": "usd",
	}, "checkout.session.completed")
	item := map[string]any{
		"id": "si_life", "object": "subscription_item", "subscription": "sub_life",
		"quantity": 1, "price": map[string]any{"id": "price_life"},
	}
	fs.Put("subscription", "sub_life", map[string]any{
		"customer": "cus_life", "status": "active",
		"items": map[string]any{"data": []any{item}, "has_more": false},
	}, "customer.subscription.created")
	fs.Put("invoice", "in_life", map[string]any{
		"customer": "cus_life", "subscription": "sub_life", "status": "paid",
		"total": 4900, "amount_paid": 4900, "amount_due": 0, "currency": "usd",
	}, "invoice.paid")
	fs.Put("payment_intent", "pi_life", map[string]any{
		"customer": "cus_life", "status": "succeeded", "amount": 4900, "currency": "usd", "latest_charge": "ch_life",
	}, "payment_intent.succeeded")
	fs.Put("charge", "ch_life", map[string]any{
		"customer": "cus_life", "payment_intent": "pi_life", "status": "succeeded",
		"amount": 4900, "amount_refunded": 0, "currency": "usd", "paid": true, "refunded": false,
	}, "charge.succeeded")
	// cancellation: the subscription's final state
	fs.Put("subscription", "sub_life", map[string]any{
		"customer": "cus_life", "status": "canceled", "canceled_at": 1735700000,
		"items": map[string]any{"data": []any{item}, "has_more": false},
	}, "customer.subscription.deleted")

	proc := startServe(t, binary, connString, fs.URL(), "")

	// deliver everything shuffled, then duplicate a few, Stripe-style
	fs.DeliverAll(t, proc.IngestURL, true)
	events := fs.Events()
	for _, i := range []int{0, 4, 8} {
		fs.Deliver(t, proc.IngestURL, events[i].ID, fakestripe.Duplicate(2))
	}

	waitFor(t, 30*time.Second, "all events processed", func() bool {
		var unprocessed int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM driftless.events WHERE processed_at IS NULL`).Scan(&unprocessed)
		return err == nil && unprocessed == 0
	})
	waitForDrain(t, pool)

	// every affected table converged to upstream truth
	for _, obj := range []struct {
		table      string
		objectType stripeapi.ObjectType
		id         string
	}{
		{"stripe.customers", "customer", "cus_life"},
		{"stripe.products", "product", "prod_life"},
		{"stripe.prices", "price", "price_life"},
		{"stripe.checkout_sessions", "checkout_session", "cs_life"},
		{"stripe.subscriptions", "subscription", "sub_life"},
		{"stripe.invoices", "invoice", "in_life"},
		{"stripe.payment_intents", "payment_intent", "pi_life"},
		{"stripe.charges", "charge", "ch_life"},
	} {
		mirrorMatches(t, pool, fs, obj.table, obj.objectType, obj.id)
	}

	// typed columns worth spot-checking: money and the canceled status
	var status string
	var amountPaid int64
	if err := pool.QueryRow(ctx,
		`SELECT s.status, i.amount_paid FROM stripe.subscriptions s
		 JOIN stripe.invoices i ON i.subscription = s.id WHERE s.id = 'sub_life'`).
		Scan(&status, &amountPaid); err != nil {
		t.Fatal(err)
	}
	if status != "canceled" || amountPaid != 4900 {
		t.Errorf("status=%q amount_paid=%d, want canceled and 4900", status, amountPaid)
	}

	// one row per object, no duplicates anywhere
	for _, table := range []string{
		"stripe.customers", "stripe.subscriptions", "stripe.invoices", "stripe.charges",
	} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%s rows = %d, want 1", table, n)
		}
	}
}
