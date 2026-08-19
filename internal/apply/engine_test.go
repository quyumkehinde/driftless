package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// newTestEngine wires an engine against fakestripe and a real database.
func newTestEngine(t *testing.T) (*Engine, *fakestripe.Server, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Start(t)
	fs := fakestripe.New(t, "whsec_apply")
	limiter := stripeapi.NewLimiter(1000)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_apply", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(pool, client, nil, logger, nil), fs, pool
}

// newPayloadEngine is newTestEngine with payload mode for the given types.
func newPayloadEngine(t *testing.T, payloadTypes ...string) (*Engine, *fakestripe.Server, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Start(t)
	fs := fakestripe.New(t, "whsec_apply")
	limiter := stripeapi.NewLimiter(1000)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_apply", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(pool, client, payloadTypes, logger, nil), fs, pool
}

// jobFor builds the claimed-job shape the worker would hand to Apply.
func jobFor(objectType, objectID string, event *fakestripe.Event) queue.Job {
	job := queue.Job{ObjectType: objectType, ObjectID: objectID}
	if event != nil {
		job.LatestEventID = &event.ID
		created := event.Created
		job.LatestEventCreated = &created
	}
	return job
}

// jsonEqual compares stored JSON with a native object by normalizing both
// through JSON, so number representations cannot cause false mismatches.
func jsonEqual(t *testing.T, stored []byte, want map[string]any) bool {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var a, b map[string]any
	if err := json.Unmarshal(stored, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantJSON, &b); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(a, b)
}

// insertEvent records the poking event the way ingest would have.
func insertEvent(t *testing.T, pool *pgxpool.Pool, event fakestripe.Event) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO driftless.events (event_id, type, created, source, payload, livemode)
		VALUES ($1, $2, $3, 'webhook', $4, false)`,
		event.ID, event.Type, event.Created, event.Payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestApplyFetchesAndMirrors(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_1", map[string]any{
		"email": "mirror@x.y", "created": 1735689600, "livemode": false,
	}, "customer.created")
	insertEvent(t, pool, event)

	if err := engine.Apply(ctx, jobFor("customer", "cus_1", &event)); err != nil {
		t.Fatal(err)
	}

	// the mirror row matches fakestripe's own store
	var data []byte
	var email string
	var isDeleted bool
	err := pool.QueryRow(ctx,
		`SELECT data, email, is_deleted FROM stripe.customers WHERE id = 'cus_1'`).
		Scan(&data, &email, &isDeleted)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := fs.Object("customer", "cus_1")
	if !jsonEqual(t, data, want) {
		t.Errorf("mirrored data = %s, want %v", data, want)
	}
	if email != "mirror@x.y" || isDeleted {
		t.Errorf("email=%q is_deleted=%v", email, isDeleted)
	}

	// bookkeeping: object_state and processed_at
	var syncSource string
	var lastEventID string
	err = pool.QueryRow(ctx,
		`SELECT sync_source, last_event_id FROM driftless.object_state WHERE object_type='customer' AND object_id='cus_1'`).
		Scan(&syncSource, &lastEventID)
	if err != nil {
		t.Fatal(err)
	}
	if syncSource != "fetch" || lastEventID != event.ID {
		t.Errorf("object_state: sync_source=%q last_event_id=%q", syncSource, lastEventID)
	}
	var processed *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT processed_at FROM driftless.events WHERE event_id = $1`, event.ID).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if processed == nil {
		t.Error("event not marked processed")
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_i", map[string]any{"email": "i@x.y"}, "customer.created")
	insertEvent(t, pool, event)
	job := jobFor("customer", "cus_i", &event)

	if err := engine.Apply(ctx, job); err != nil {
		t.Fatal(err)
	}
	var firstData []byte
	var firstUpdated time.Time
	if err := pool.QueryRow(ctx,
		`SELECT data, updated_at FROM stripe.customers WHERE id = 'cus_i'`).Scan(&firstData, &firstUpdated); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := engine.Apply(ctx, job); err != nil {
		t.Fatal(err)
	}
	var secondData []byte
	var secondUpdated time.Time
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT data, updated_at, (SELECT count(*) FROM stripe.customers) FROM stripe.customers WHERE id = 'cus_i'`).
		Scan(&secondData, &secondUpdated, &count); err != nil {
		t.Fatal(err)
	}
	if string(firstData) != string(secondData) {
		t.Error("re-apply changed data")
	}
	if !secondUpdated.After(firstUpdated) {
		t.Error("re-apply should refresh updated_at")
	}
	if count != 1 {
		t.Errorf("rows = %d, want 1", count)
	}
}

func TestApply404SoftDeletes(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	event := fs.Put("product", "prod_x", map[string]any{"name": "Gone"}, "product.created")
	insertEvent(t, pool, event)
	if err := engine.Apply(ctx, jobFor("product", "prod_x", &event)); err != nil {
		t.Fatal(err)
	}

	// object vanishes upstream without a deleted event reaching us
	fs.Delete("product", "prod_x", "product.deleted")
	if err := engine.Apply(ctx, jobFor("product", "prod_x", nil)); err != nil {
		t.Fatal(err)
	}

	var isDeleted bool
	var name string
	err := pool.QueryRow(ctx,
		`SELECT is_deleted, name FROM stripe.products WHERE id = 'prod_x'`).Scan(&isDeleted, &name)
	if err != nil {
		t.Fatal(err)
	}
	if !isDeleted || name != "Gone" {
		t.Errorf("is_deleted=%v name=%q: soft delete must keep last data", isDeleted, name)
	}
}

func TestApplyDeletedEventSkipsFetch(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	created := fs.Put("customer", "cus_del", map[string]any{"email": "d@x.y"}, "customer.created")
	insertEvent(t, pool, created)
	if err := engine.Apply(ctx, jobFor("customer", "cus_del", &created)); err != nil {
		t.Fatal(err)
	}

	// craft a deleted event while the object is STILL fetchable upstream:
	// if the row ends up soft-deleted, the shortcut was taken, no fetch
	tombstone := fakestripe.Event{
		ID: "evt_crafted_deleted", Type: "customer.deleted",
		Created: time.Now().UTC().Truncate(time.Second),
		Payload: []byte(`{"id":"evt_crafted_deleted","type":"customer.deleted","created":1735700000,"livemode":false,"data":{"object":{"id":"cus_del","object":"customer","deleted":true}}}`),
	}
	insertEvent(t, pool, tombstone)
	if err := engine.Apply(ctx, jobFor("customer", "cus_del", &tombstone)); err != nil {
		t.Fatal(err)
	}

	var isDeleted bool
	if err := pool.QueryRow(ctx,
		`SELECT is_deleted FROM stripe.customers WHERE id = 'cus_del'`).Scan(&isDeleted); err != nil {
		t.Fatal(err)
	}
	if !isDeleted {
		t.Error("deleted event must soft-delete without fetching")
	}
}

func TestApplyCancellationEventStillFetches(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	// subscription "deleted" means canceled: the object stays fetchable
	// and its fetched status is the truth
	event := fs.Put("subscription", "sub_c", map[string]any{
		"customer": "cus_1", "status": "canceled",
		"items": map[string]any{"data": []any{}, "has_more": false},
	}, "customer.subscription.deleted")
	insertEvent(t, pool, event)

	if err := engine.Apply(ctx, jobFor("subscription", "sub_c", &event)); err != nil {
		t.Fatal(err)
	}

	var status string
	var isDeleted bool
	err := pool.QueryRow(ctx,
		`SELECT status, is_deleted FROM stripe.subscriptions WHERE id = 'sub_c'`).Scan(&status, &isDeleted)
	if err != nil {
		t.Fatal(err)
	}
	if status != "canceled" || isDeleted {
		t.Errorf("status=%q is_deleted=%v: canceled subscriptions stay visible", status, isDeleted)
	}
}

func TestApplyNotifiesListeners(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN driftless_changes`); err != nil {
		t.Fatal(err)
	}

	event := fs.Put("customer", "cus_n", map[string]any{"email": "n@x.y"}, "customer.created")
	insertEvent(t, pool, event)
	if err := engine.Apply(ctx, jobFor("customer", "cus_n", &event)); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	notification, err := conn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("no notification: %v", err)
	}
	if notification.Payload != `{"id":"cus_n","type":"customer"}` {
		t.Errorf("payload = %s", notification.Payload)
	}
}

func TestApplyConcurrentSameObject(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_race", map[string]any{"email": "race@x.y"}, "customer.created")
	insertEvent(t, pool, event)
	job := jobFor("customer", "cus_race", &event)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := engine.Apply(ctx, job); err != nil {
				t.Errorf("concurrent apply: %v", err)
			}
		}()
	}
	wg.Wait()

	var count int
	var data []byte
	if err := pool.QueryRow(ctx,
		`SELECT count(*), min(data::text)::jsonb FROM stripe.customers WHERE id = 'cus_race'`).
		Scan(&count, &data); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("rows = %d, want 1", count)
	}
	want, _ := fs.Object("customer", "cus_race")
	if !jsonEqual(t, data, want) {
		t.Errorf("final state diverged from upstream: %s vs %v", data, want)
	}
}

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

	const itemCount = 15
	embedded := make([]map[string]any, 0, 10)
	for i := range itemCount {
		item := map[string]any{
			"id": fmt.Sprintf("si_p%02d", i), "object": "subscription_item",
			"subscription": "sub_p", "quantity": 1,
			"price": map[string]any{"id": fmt.Sprintf("price_p%02d", i)},
		}
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

func TestApplyFetchFailureReleasesConnection(t *testing.T) {
	// Regression: the fetch-failure counter used to run inside the apply
	// transaction while acquiring a second pooled connection; with every
	// connection already held by workers, that deadlocked the pool. A
	// one-connection pool makes the old behavior hang and the fix pass.
	_, connString := testpg.StartWithURL(t)
	poolCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	fs := fakestripe.New(t, "whsec_apply")
	limiter := stripeapi.NewLimiter(1000)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_apply", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewEngine(pool, client, nil, logger, nil)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO driftless.object_state (object_type, object_id, last_synced_at, sync_source)
		VALUES ('customer', 'cus_fail', now(), 'fetch')`); err != nil {
		t.Fatal(err)
	}

	// a terminal API failure on the fetch
	fs.FailRate(1.0, 401)

	applyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = engine.Apply(applyCtx, queue.Job{ObjectType: "customer", ObjectID: "cus_fail"})
	if err == nil {
		t.Fatal("apply must surface the fetch failure")
	}
	if applyCtx.Err() != nil {
		t.Fatal("apply timed out: the failure counter is deadlocking the pool again")
	}

	fs.FailRate(0, 0)
	var failures int
	if err := pool.QueryRow(ctx,
		`SELECT fetch_failures FROM driftless.object_state WHERE object_id = 'cus_fail'`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Errorf("fetch_failures = %d, want 1", failures)
	}
}

func TestApplySoftDeleteCascadesToItems(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	item := map[string]any{
		"id": "si_casc", "object": "subscription_item", "subscription": "sub_casc",
		"quantity": 1, "price": map[string]any{"id": "price_1"},
	}
	event := fs.Put("subscription", "sub_casc", map[string]any{
		"customer": "cus_1", "status": "active",
		"items": map[string]any{"data": []any{item}, "has_more": false},
	}, "customer.subscription.created")
	insertEvent(t, pool, event)
	if err := engine.Apply(ctx, jobFor("subscription", "sub_casc", &event)); err != nil {
		t.Fatal(err)
	}

	// the subscription becomes unfetchable upstream: the 404 soft delete
	// must not leave its items as live phantom entitlements
	fs.Force404("sub_casc")
	if err := engine.Apply(ctx, jobFor("subscription", "sub_casc", nil)); err != nil {
		t.Fatal(err)
	}

	var subDeleted, itemDeleted bool
	if err := pool.QueryRow(ctx,
		`SELECT is_deleted FROM stripe.subscriptions WHERE id = 'sub_casc'`).Scan(&subDeleted); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT is_deleted FROM stripe.subscription_items WHERE id = 'si_casc'`).Scan(&itemDeleted); err != nil {
		t.Fatal(err)
	}
	if !subDeleted || !itemDeleted {
		t.Errorf("sub_deleted=%v item_deleted=%v, want both", subDeleted, itemDeleted)
	}
}

func TestApplyFetchedDeletionStubSoftDeletes(t *testing.T) {
	engine, fs, pool := newTestEngine(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_stub", map[string]any{"email": "keep@x.y"}, "customer.created")
	insertEvent(t, pool, event)
	if err := engine.Apply(ctx, jobFor("customer", "cus_stub", &event)); err != nil {
		t.Fatal(err)
	}

	// deleted upstream, but the job in hand carries no deleted marker:
	// the fetch returns Stripe's 200 stub, which must soft-delete, never
	// overwrite the row with the three-field stub
	fs.Delete("customer", "cus_stub", "customer.deleted")
	if err := engine.Apply(ctx, jobFor("customer", "cus_stub", nil)); err != nil {
		t.Fatal(err)
	}

	var isDeleted bool
	var email string
	if err := pool.QueryRow(ctx,
		`SELECT is_deleted, data->>'email' FROM stripe.customers WHERE id = 'cus_stub'`).Scan(&isDeleted, &email); err != nil {
		t.Fatal(err)
	}
	if !isDeleted {
		t.Error("a fetched deletion stub must soft-delete the row")
	}
	if email != "keep@x.y" {
		t.Errorf("email = %q: the stub must not clobber the row's history", email)
	}
}
