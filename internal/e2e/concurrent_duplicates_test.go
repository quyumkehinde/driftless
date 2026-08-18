package e2e

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/ingest"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

type countingApplier struct {
	fetches atomic.Int64
	fs      *fakestripe.Server
}

func (a *countingApplier) Apply(_ context.Context, job queue.Job) error {
	a.fetches.Add(1)
	// the fetch itself, as the worker will do once the apply engine lands
	resp, err := http.Get(a.fs.URL() + "/v1/customers/" + job.ObjectID)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// TestConcurrentDuplicateDeliveries replays a commonly reported incident
// class: identical webhook deliveries race into a handler that has no
// prior state for the event, and naive receivers process it once per
// delivery. Driftless must end with one event row, one job, one fetch.
func TestConcurrentDuplicateDeliveries(t *testing.T) {
	pool := testpg.Start(t)
	ctx := context.Background()

	fs := fakestripe.New(t, e2eSecret)
	event := fs.Put("customer", "cus_race", map[string]any{"email": "race@x.y"}, "customer.created")

	q := queue.New(pool, 2*time.Minute, 8)
	verifier := ingest.NewVerifier(e2eSecret, "", 300*time.Second)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(ingest.NewServer(pool, q, verifier, logger, nil).Handler())
	t.Cleanup(srv.Close)

	// three identical deliveries, concurrently
	statuses := fs.DeliverConcurrent(t, srv.URL, event.ID, 3)
	for _, status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("statuses = %v: every delivery must be acknowledged", statuses)
		}
	}

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs`).Scan(&jobCount)
	if eventCount != 1 {
		t.Errorf("event rows = %d, want 1", eventCount)
	}
	if jobCount != 1 {
		t.Errorf("job rows = %d, want 1", jobCount)
	}

	// run workers: the one job must cost exactly one fetch
	applier := &countingApplier{fs: fs}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wp := queue.NewWorkerPool(q, applier, 4, 20*time.Millisecond, logger, nil)
	done := make(chan struct{})
	go func() { wp.Run(workCtx); close(done) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		counts, err := q.CountByStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts["done"] == 1 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	<-done

	if fetches := applier.fetches.Load(); fetches != 1 {
		t.Errorf("fetches = %d, want exactly 1", fetches)
	}
}
