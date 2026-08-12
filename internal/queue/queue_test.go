package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/testpg"
)

// enqueue is a test shorthand: one job in its own transaction.
func enqueue(t *testing.T, pool *pgxpool.Pool, q *Queue, p EnqueueParams) (int64, bool) {
	t.Helper()
	ctx := context.Background()
	var id int64
	var inserted bool
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var err error
		id, inserted, err = q.Enqueue(ctx, tx, p)
		return err
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id, inserted
}

func TestEnqueueCoalesces(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	firstID, inserted := enqueue(t, pool, q, EnqueueParams{
		ObjectType: "subscription", ObjectID: "sub_1",
		EventID: "evt_1", EventCreated: base,
	})
	if !inserted {
		t.Fatal("first enqueue should insert")
	}

	// 19 more pokes for the same object, newest last
	for i := 1; i < 20; i++ {
		id, inserted := enqueue(t, pool, q, EnqueueParams{
			ObjectType: "subscription", ObjectID: "sub_1",
			EventID: fmt.Sprintf("evt_%d", i+1), EventCreated: base.Add(time.Duration(i) * time.Second),
		})
		if inserted {
			t.Fatalf("enqueue %d should coalesce, not insert", i+1)
		}
		if id != firstID {
			t.Fatalf("coalesced onto job %d, want %d", id, firstID)
		}
	}

	// a different object still gets its own row
	otherID, inserted := enqueue(t, pool, q, EnqueueParams{
		ObjectType: "subscription", ObjectID: "sub_2", EventID: "evt_x", EventCreated: base,
	})
	if !inserted || otherID == firstID {
		t.Fatalf("different object: id=%d inserted=%v", otherID, inserted)
	}

	// the coalesced job carries the newest event
	jobs, err := q.List(ctx, "pending", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("pending jobs = %d, want 2", len(jobs))
	}
	first := jobs[0]
	if first.LatestEventID == nil || *first.LatestEventID != "evt_20" {
		t.Errorf("latest_event_id = %v, want evt_20", first.LatestEventID)
	}

	// an old duplicate delivery must not move the job backwards
	enqueue(t, pool, q, EnqueueParams{
		ObjectType: "subscription", ObjectID: "sub_1", EventID: "evt_1", EventCreated: base,
	})
	jobs, err = q.List(ctx, "pending", 10)
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].LatestEventID == nil || *jobs[0].LatestEventID != "evt_20" {
		t.Errorf("stale enqueue moved latest_event_id to %v", jobs[0].LatestEventID)
	}
}

func TestCoalescingBoundsFetches(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	enqueue(t, pool, q, EnqueueParams{
		ObjectType: "subscription", ObjectID: "sub_burst", EventID: "evt_1", EventCreated: base,
	})

	// claim the job, simulating a worker mid-fetch
	job, ok, err := q.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	fetches := 1

	// a burst of 19 more events lands while the fetch is in flight
	for i := 1; i < 20; i++ {
		_, inserted := enqueue(t, pool, q, EnqueueParams{
			ObjectType: "subscription", ObjectID: "sub_burst",
			EventID: fmt.Sprintf("evt_%d", i+1), EventCreated: base.Add(time.Duration(i) * time.Second),
		})
		if inserted {
			t.Fatalf("burst enqueue %d should coalesce onto the running job", i+1)
		}
	}

	// the finished fetch is stale: the job must requeue, not complete
	requeued, err := q.Complete(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if !requeued {
		t.Fatal("job with coalesced newer events must requeue")
	}

	// second fetch covers the whole burst
	job, ok, err = q.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("re-claim: ok=%v err=%v", ok, err)
	}
	fetches++
	requeued, err = q.Complete(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if requeued {
		t.Fatal("no newer events arrived; job must complete")
	}

	if fetches > 2 {
		t.Fatalf("20 events cost %d fetches, want at most 2", fetches)
	}
	counts, err := q.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts["done"] != 1 || counts["pending"] != 0 {
		t.Errorf("counts = %v, want exactly one done", counts)
	}
}

func TestNoDoubleClaimUnderConcurrency(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	const jobCount = 200
	const workers = 50

	for i := range jobCount {
		enqueue(t, pool, q, EnqueueParams{
			ObjectType: "customer", ObjectID: fmt.Sprintf("cus_%03d", i),
			EventID: fmt.Sprintf("evt_%03d", i), EventCreated: time.Now().UTC(),
		})
	}

	var mu sync.Mutex
	claims := make(map[int64]int)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, ok, err := q.Claim(ctx)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				claims[job.ID]++
				mu.Unlock()
				if _, err := q.Complete(ctx, job); err != nil {
					t.Errorf("complete: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(claims) != jobCount {
		t.Errorf("claimed %d distinct jobs, want %d", len(claims), jobCount)
	}
	for id, n := range claims {
		if n != 1 {
			t.Errorf("job %d claimed %d times", id, n)
		}
	}
	counts, err := q.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts["done"] != jobCount {
		t.Errorf("done = %d, want %d", counts["done"], jobCount)
	}
}

func TestFailBackoffAndDeadLetter(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	enqueue(t, pool, q, EnqueueParams{
		ObjectType: "invoice", ObjectID: "in_1", EventID: "evt_1", EventCreated: time.Now().UTC(),
	})

	// max_attempts defaults to 8: fail through the whole budget
	for attempt := 1; attempt <= 8; attempt++ {
		// make the job runnable immediately regardless of backoff
		if _, err := pool.Exec(ctx, `UPDATE driftless.jobs SET run_after = now()`); err != nil {
			t.Fatal(err)
		}
		job, ok, err := q.Claim(ctx)
		if err != nil || !ok {
			t.Fatalf("attempt %d: claim ok=%v err=%v", attempt, ok, err)
		}
		if job.Attempts != int32(attempt) {
			t.Errorf("attempt %d: attempts = %d", attempt, job.Attempts)
		}
		dead, err := q.Fail(ctx, job, errors.New("stripe exploded"))
		if err != nil {
			t.Fatal(err)
		}
		if wantDead := attempt == 8; dead != wantDead {
			t.Errorf("attempt %d: dead = %v, want %v", attempt, dead, wantDead)
		}

		if attempt < 8 {
			// backoff schedule: 5s, 25s, 2m, 10m, 30m (then 30m), +-20%
			jobs, err := q.List(ctx, "pending", 1)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("attempt %d: list err=%v n=%d", attempt, err, len(jobs))
			}
			delay := time.Until(jobs[0].RunAfter)
			idx := attempt - 1
			if idx >= len(backoffSchedule) {
				idx = len(backoffSchedule) - 1
			}
			base := backoffSchedule[idx]
			lo := time.Duration(float64(base)*0.8) - 5*time.Second
			hi := time.Duration(float64(base)*1.2) + 5*time.Second
			if delay < lo || delay > hi {
				t.Errorf("attempt %d: backoff %v outside [%v, %v]", attempt, delay, lo, hi)
			}
		}
	}

	// dead job: visible, then retryable with a fresh budget
	deadJobs, err := q.List(ctx, "dead", 10)
	if err != nil || len(deadJobs) != 1 {
		t.Fatalf("dead jobs err=%v n=%d", err, len(deadJobs))
	}
	if deadJobs[0].LastError == nil || *deadJobs[0].LastError != "stripe exploded" {
		t.Errorf("last_error = %v", deadJobs[0].LastError)
	}

	n, err := q.RetryAllDead(ctx)
	if err != nil || n != 1 {
		t.Fatalf("retry dead: n=%d err=%v", n, err)
	}
	jobs, err := q.List(ctx, "pending", 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("after retry: err=%v n=%d", err, len(jobs))
	}
	if jobs[0].Attempts != 0 {
		t.Errorf("retried job attempts = %d, want 0", jobs[0].Attempts)
	}
}

func TestReaperResurrectsExpiredClaims(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	enqueue(t, pool, q, EnqueueParams{
		ObjectType: "charge", ObjectID: "ch_1", EventID: "evt_1", EventCreated: time.Now().UTC(),
	})

	// worker claims and then "crashes": no complete, no fail
	job, ok, err := q.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}

	// claim still fresh: reaper must not touch it
	n, err := q.Reap(ctx)
	if err != nil || n != 0 {
		t.Fatalf("reap fresh claim: n=%d err=%v", n, err)
	}

	// force the visibility timeout to expire
	if _, err := pool.Exec(ctx,
		`UPDATE driftless.jobs SET claimed_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	n, err = q.Reap(ctx)
	if err != nil || n != 1 {
		t.Fatalf("reap expired claim: n=%d err=%v", n, err)
	}

	// the job is claimable again
	again, ok, err := q.Claim(ctx)
	if err != nil || !ok {
		t.Fatalf("re-claim ok=%v err=%v", ok, err)
	}
	if again.ID != job.ID {
		t.Errorf("re-claimed %d, want %d", again.ID, job.ID)
	}
	if again.Attempts != 2 {
		t.Errorf("attempts after crash resurrection = %d, want 2", again.Attempts)
	}
}

func TestReaperDeadLettersCrashLoops(t *testing.T) {
	pool := testpg.Start(t)
	q := New(pool, 2*time.Minute)
	ctx := context.Background()

	enqueue(t, pool, q, EnqueueParams{
		ObjectType: "charge", ObjectID: "ch_loop", EventID: "evt_1", EventCreated: time.Now().UTC(),
	})

	// crash-loop through the whole attempt budget
	for attempt := 1; attempt <= 8; attempt++ {
		job, ok, err := q.Claim(ctx)
		if err != nil || !ok {
			t.Fatalf("attempt %d: claim ok=%v err=%v", attempt, ok, err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE driftless.jobs SET claimed_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Reap(ctx); err != nil {
			t.Fatal(err)
		}
	}

	counts, err := q.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts["dead"] != 1 {
		t.Errorf("counts = %v, want the crash-looping job dead", counts)
	}
}
