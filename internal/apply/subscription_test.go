package apply

import (
	"context"
	"fmt"
	"testing"
)

func TestApplySubscriptionExplodesItems(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	items := make([]map[string]any, 3)
	for i := range items {
		items[i] = map[string]any{
			"id": fmt.Sprintf("si_%d", i), "object": "subscription_item",
			"subscription": "sub_e", "quantity": i + 1,
			"price": map[string]any{"id": fmt.Sprintf("price_%d", i)},
		}
	}
	event := fs.Put("subscription", "sub_e", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": items, "has_more": false},
	}, "customer.subscription.created")
	insertEvent(t, pool, event)

	if err := engine.Apply(ctx, jobFor("subscription", "sub_e", &event)); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM stripe.subscription_items WHERE subscription = 'sub_e' AND NOT is_deleted`).
		Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("items = %d, want 3", count)
	}
	var price string
	var quantity int64
	if err := pool.QueryRow(ctx,
		`SELECT price, quantity FROM stripe.subscription_items WHERE id = 'si_1'`).Scan(&price, &quantity); err != nil {
		t.Fatal(err)
	}
	if price != "price_1" || quantity != 2 {
		t.Errorf("si_1: price=%q quantity=%d", price, quantity)
	}

	// removing an item from the subscription soft-deletes its row
	event2 := fs.Put("subscription", "sub_e", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": items[:2], "has_more": false},
	}, "customer.subscription.updated")
	insertEvent(t, pool, event2)
	if err := engine.Apply(ctx, jobFor("subscription", "sub_e", &event2)); err != nil {
		t.Fatal(err)
	}

	var live, dead int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE NOT is_deleted), count(*) FILTER (WHERE is_deleted)
		FROM stripe.subscription_items WHERE subscription = 'sub_e'`).Scan(&live, &dead); err != nil {
		t.Fatal(err)
	}
	if live != 2 || dead != 1 {
		t.Errorf("live=%d dead=%d, want 2 live and 1 soft-deleted", live, dead)
	}
}

func TestApplySubscriptionPagesThroughItems(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	// fifteen items: the embedded list truncates at ten with has_more, so
	// a correct apply must page the collection; writing only the embedded
	// ten is the entitlement-truncation bug class
	const itemCount = 15
	embedded := make([]map[string]any, 0, 10)
	for i := range itemCount {
		item := map[string]any{
			"id": fmt.Sprintf("si_p%02d", i), "object": "subscription_item",
			"subscription": "sub_p", "quantity": 1,
			"price": map[string]any{"id": fmt.Sprintf("price_p%02d", i)},
		}
		// the collection endpoint serves all of them
		fs.Put("subscription_item", item["id"].(string), item, "noop.item.stored")
		if i < 10 {
			embedded = append(embedded, item)
		}
	}
	event := fs.Put("subscription", "sub_p", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": embedded, "has_more": true},
	}, "customer.subscription.created")
	insertEvent(t, pool, event)

	if err := engine.Apply(ctx, jobFor("subscription", "sub_p", &event)); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM stripe.subscription_items WHERE subscription = 'sub_p' AND NOT is_deleted`).
		Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != itemCount {
		t.Errorf("items mirrored = %d, want all %d: embedded page truncates", count, itemCount)
	}
}
