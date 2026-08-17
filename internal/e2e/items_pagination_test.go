package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestSubscriptionItemsPagination pins the truncation class: a
// subscription with fifteen items embeds only ten (has_more), and every
// item must still land.
func TestSubscriptionItemsPagination(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)
	ctx := context.Background()

	const itemCount = 15
	embedded := make([]map[string]any, 0, 10)
	for i := range itemCount {
		item := map[string]any{
			"id": fmt.Sprintf("si_%02d", i), "object": "subscription_item",
			"subscription": "sub_15", "quantity": 1,
			"price": map[string]any{"id": fmt.Sprintf("price_%02d", i)},
		}
		fs.Put("subscription_item", item["id"].(string), item, "noop.item.stored")
		if i < 10 {
			embedded = append(embedded, item)
		}
	}
	event := fs.Put("subscription", "sub_15", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": embedded, "has_more": true},
	}, "customer.subscription.created")

	proc := startServe(t, binary, connString, fs.URL(), "")
	fs.Deliver(t, proc.IngestURL, event.ID)

	waitFor(t, 15*time.Second, "subscription to mirror", func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM stripe.subscriptions WHERE id = 'sub_15'`).Scan(&n)
		return n == 1
	})
	waitForDrain(t, pool)

	var items int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM stripe.subscription_items WHERE subscription = 'sub_15' AND NOT is_deleted`).
		Scan(&items); err != nil {
		t.Fatal(err)
	}
	if items != itemCount {
		t.Errorf("items = %d, want all %d", items, itemCount)
	}
}
