package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/testpg"
)

type applierFunc func(context.Context, Job) error

func (f applierFunc) Apply(ctx context.Context, job Job) error { return f(ctx, job) }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWorkerPoolProcessesJobs(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const jobCount = 40
	for i := range jobCount {
		enqueue(t, pool, q, EnqueueParams{
			ObjectType: "customer", ObjectID: fmt.Sprintf("cus_%d", i),
			EventID: fmt.Sprintf("evt_%d", i), EventCreated: time.Now().UTC(),
		})
	}

	var mu sync.Mutex
	applied := make(map[string]int)
	applier := applierFunc(func(_ context.Context, job Job) error {
		mu.Lock()
		applied[job.ObjectID]++
		mu.Unlock()
		if job.ObjectID == "cus_0" {
			return errors.New("transient failure")
		}
		return nil
	})

	wp := NewWorkerPool(q, applier, 8, 20*time.Millisecond, discardLogger(), nil)
	done := make(chan struct{})
	go func() { wp.Run(ctx); close(done) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		counts, err := q.CountByStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts["done"] == jobCount-1 && counts["pending"] == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done

	counts, err := q.CountByStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts["done"] != jobCount-1 {
		t.Errorf("done = %d, want %d", counts["done"], jobCount-1)
	}
	// the failing job is pending with backoff, not lost
	if counts["pending"] != 1 {
		t.Errorf("pending = %d, want 1 (the failing job)", counts["pending"])
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < jobCount; i++ {
		if n := applied[fmt.Sprintf("cus_%d", i)]; n != 1 {
			t.Errorf("cus_%d applied %d times, want 1", i, n)
		}
	}
}
