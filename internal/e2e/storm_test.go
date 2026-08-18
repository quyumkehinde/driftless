package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/backfill"
	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/ingest"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// Test429Storm runs a backfill while half of all API responses are 429s,
// with live webhooks arriving through the same shared limiter. The rate
// must adapt instead of dying: the backfill completes with no failed
// tasks, no job goes dead, and webhook-priority applies stay prompt
// because the priority tiers shield them from backfill pressure.
func Test429Storm(t *testing.T) {
	pool := testpg.Start(t)
	fs := fakestripe.New(t, e2eSecret)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedLargeAccount(t, fs, 40)

	// one process, one limiter: exactly the serve wiring
	limiter := stripeapi.NewLimiter(30)
	t.Cleanup(limiter.Stop)
	client := stripeapi.New(fs.URL(), "rk_test_storm", limiter, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	q := queue.New(pool, 2*time.Minute)
	engine := apply.NewEngine(pool, client, nil, logger, nil)
	workers := queue.NewWorkerPool(q, engine, 4, 20*time.Millisecond, logger, nil)
	workersDone := make(chan struct{})
	go func() { workers.Run(ctx); close(workersDone) }()

	verifier := ingest.NewVerifier(e2eSecret, "", 300*time.Second)
	ingestSrv := httptest.NewServer(ingest.NewServer(pool, q, verifier, logger, nil).Handler())
	t.Cleanup(ingestSrv.Close)

	// the storm: half of all API calls answer 429 with Retry-After
	fs.FailRate(0.5, http.StatusTooManyRequests)
	t.Cleanup(func() { fs.FailRate(0, 0) })

	runner := backfill.NewRunner(pool, client, logger, nil, nil)
	backfillDone := make(chan error, 1)
	go func() {
		_, err := runner.Start(ctx, backfill.Options{RequestedBy: "cli"})
		backfillDone <- err
	}()

	// live webhooks land mid-storm; their applies ride webhook priority
	var latencies []time.Duration
	for i := range 5 {
		time.Sleep(500 * time.Millisecond)
		id := fmt.Sprintf("cus_storm_%d", i)
		event := fs.Put("customer", id, map[string]any{"email": "storm@x.y"}, "customer.created")
		start := time.Now()
		fs.Deliver(t, ingestSrv.URL, event.ID)
		waitFor(t, 30*time.Second, "webhook apply during storm", func() bool {
			var n int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM stripe.customers WHERE id = $1 AND NOT is_deleted`, id).Scan(&n)
			return n == 1
		})
		latencies = append(latencies, time.Since(start))
	}

	select {
	case err := <-backfillDone:
		if err != nil {
			t.Fatalf("backfill under storm: %v", err)
		}
	case <-time.After(4 * time.Minute):
		t.Fatal("backfill did not finish under the storm")
	}

	// zero dead work anywhere
	var deadJobs, failedTasks int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs WHERE status = 'dead'`).Scan(&deadJobs)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.backfill_tasks WHERE status = 'failed'`).Scan(&failedTasks)
	if deadJobs != 0 || failedTasks != 0 {
		t.Errorf("dead jobs=%d failed tasks=%d, want 0 and 0", deadJobs, failedTasks)
	}

	// AIMD actually engaged
	if got := limiter.EffectiveRPS(); got >= 30 {
		t.Errorf("effective rps = %v, want reduced below the configured 30", got)
	}

	// webhook applies stayed prompt despite the storm; generous bound
	// because each fetch may eat 429 retries, but priority must keep it
	// far under backfill-scale delays
	for i, latency := range latencies {
		if latency > 15*time.Second {
			t.Errorf("webhook %d applied after %v: webhook priority starved", i, latency)
		}
	}

	cancel()
	<-workersDone
}
