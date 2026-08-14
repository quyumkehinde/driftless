package e2e

import (
	"context"
	"net/http"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestCrashBetweenInsertAndAck replays a documented incident class: a
// handler that acknowledges webhooks before its database write is durable
// loses events forever, because Stripe stops retrying acknowledged
// deliveries. One public report attributes an eight-month revenue leak to
// exactly this bug.
//
// The process is killed at exact instruction boundaries via the crashpoint
// hook. In both cases the invariant is the same: either the event was
// durably recorded before any 200 was sent, or no 200 was sent and Stripe
// retries. Exactly one event row and one job exist after the retry.
func TestCrashBetweenInsertAndAck(t *testing.T) {
	binary := buildBinary(t)

	for _, tc := range []struct {
		name string
		// where the process dies
		point string
		// whether the event row was committed before the crash
		wantRowAfterCrash bool
	}{
		{"crash between commit and ack", "ingest.after-commit", true},
		{"crash inside the transaction", "ingest.before-commit", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, connString := testpg.StartWithURL(t)
			ctx := context.Background()

			fs := fakestripe.New(t, e2eSecret)
			event := fs.Put("customer", "cus_leak", map[string]any{"email": "leak@x.y"}, "customer.created")

			// first delivery: the process dies at the crashpoint, so no
			// acknowledgment can have been sent
			proc := startServe(t, binary, connString, tc.point)
			status, err := fs.TryDeliver(proc.IngestURL, event.ID)
			if err == nil && status == http.StatusOK {
				t.Fatal("delivery was acknowledged although the process died")
			}
			proc.WaitExit(t)

			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			wantCount := 0
			if tc.wantRowAfterCrash {
				wantCount = 1
			}
			if count != wantCount {
				t.Fatalf("events after crash = %d, want %d", count, wantCount)
			}

			// Stripe saw no 2xx, so it retries against the restarted process
			clean := startServe(t, binary, connString, "")
			statuses := fs.Deliver(t, clean.IngestURL, event.ID)
			if statuses[0] != http.StatusOK {
				t.Fatalf("retry status = %d, want 200", statuses[0])
			}

			var eventCount, jobCount int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs`).Scan(&jobCount)
			if eventCount != 1 || jobCount != 1 {
				t.Errorf("after retry: events=%d jobs=%d, want exactly 1 and 1", eventCount, jobCount)
			}
		})
	}
}
