package fakestripe

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/ingest"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

const driverSecret = "whsec_driver_test"

// startIngest wires a real ingest server backed by a real database, the
// same shape serve will use.
func startIngest(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Start(t)
	q := queue.New(pool, 2*time.Minute)
	verifier := ingest.NewVerifier(driverSecret, "", 300*time.Second)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(ingest.NewServer(pool, q, verifier, logger, nil).Handler())
	t.Cleanup(srv.Close)
	return srv, pool
}

func TestDeliverToRealIngest(t *testing.T) {
	fs := New(t, driverSecret)
	target, pool := startIngest(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_1", map[string]any{"email": "a@b.c"}, "customer.created")

	statuses := fs.Deliver(t, target.URL, event.ID)
	if len(statuses) != 1 || statuses[0] != http.StatusOK {
		t.Fatalf("statuses = %v", statuses)
	}

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events WHERE event_id = $1`, event.ID).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs WHERE object_id = 'cus_1'`).Scan(&jobCount)
	if eventCount != 1 || jobCount != 1 {
		t.Errorf("events=%d jobs=%d", eventCount, jobCount)
	}
}

func TestDeliverDuplicate(t *testing.T) {
	fs := New(t, driverSecret)
	target, pool := startIngest(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_dup", nil, "customer.created")
	statuses := fs.Deliver(t, target.URL, event.ID, Duplicate(3))
	for _, status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("statuses = %v: every duplicate gets 200", statuses)
		}
	}

	var eventCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	if eventCount != 1 {
		t.Errorf("events = %d, want 1 after triple delivery", eventCount)
	}
}

func TestDeliverWrongSignature(t *testing.T) {
	fs := New(t, driverSecret)
	target, pool := startIngest(t)
	ctx := context.Background()

	event := fs.Put("customer", "cus_bad", nil, "customer.created")
	statuses := fs.Deliver(t, target.URL, event.ID, WrongSignature())
	if statuses[0] != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", statuses[0])
	}
	var eventCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	if eventCount != 0 {
		t.Errorf("events = %d, want 0", eventCount)
	}
}

func TestDeliverAllOutOfOrder(t *testing.T) {
	fs := New(t, driverSecret)
	target, pool := startIngest(t)
	ctx := context.Background()

	fs.Put("customer", "cus_1", nil, "customer.created")
	fs.Put("customer", "cus_1", map[string]any{"email": "v2@x.y"}, "customer.updated")
	fs.Put("subscription", "sub_1", nil, "customer.subscription.created")
	fs.Put("subscription", "sub_1", map[string]any{"status": "active"}, "customer.subscription.updated")

	fs.DeliverAll(t, target.URL, true)

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs WHERE status = 'pending'`).Scan(&jobCount)
	if eventCount != 4 {
		t.Errorf("events = %d, want 4", eventCount)
	}
	// coalescing: two objects, two live jobs regardless of delivery order
	if jobCount != 2 {
		t.Errorf("jobs = %d, want 2", jobCount)
	}

	// the coalesced jobs must point at each object's newest event
	rows, err := pool.Query(ctx,
		`SELECT object_id, latest_event_id FROM driftless.jobs ORDER BY object_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	events := fs.Events()
	newest := map[string]string{}
	for _, e := range events {
		switch e.Type {
		case "customer.updated":
			newest["cus_1"] = e.ID
		case "customer.subscription.updated":
			newest["sub_1"] = e.ID
		}
	}
	for rows.Next() {
		var objectID string
		var latest *string
		if err := rows.Scan(&objectID, &latest); err != nil {
			t.Fatal(err)
		}
		if latest == nil || *latest != newest[objectID] {
			t.Errorf("%s: latest_event_id = %v, want %s", objectID, latest, newest[objectID])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
