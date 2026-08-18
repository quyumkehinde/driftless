package stripeapi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLimiterGrantsAllUnderConcurrency(t *testing.T) {
	l := NewLimiter(100)
	defer l.Stop()
	ctx := context.Background()

	const n = 50
	start := time.Now()
	var wg sync.WaitGroup
	var granted sync.Map
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Acquire(ctx, PriorityWebhook); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			granted.Store(i, true)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	count := 0
	granted.Range(func(_, _ any) bool { count++; return true })
	if count != n {
		t.Errorf("granted %d, want %d", count, n)
	}
	// 50 tokens at 100 rps from an empty bucket needs roughly half a
	// second; a large violation means token accounting is broken
	if elapsed < 300*time.Millisecond {
		t.Errorf("granted too fast: %v means tokens were minted from nothing", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("granted too slow: %v", elapsed)
	}
}

func TestLimiterPriorityOrder(t *testing.T) {
	l := NewLimiter(5)
	defer l.Stop()
	ctx := context.Background()

	var mu sync.Mutex
	var order []Priority
	var wg sync.WaitGroup

	acquire := func(p Priority) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Acquire(ctx, p); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			order = append(order, p)
			mu.Unlock()
		}()
	}

	// low priority queues first, high priority arrives later; grants must
	// still drain webhook before verify
	for range 5 {
		acquire(PriorityVerify)
	}
	time.Sleep(50 * time.Millisecond)
	for range 5 {
		acquire(PriorityWebhook)
	}
	wg.Wait()

	lastWebhook, firstVerify := -1, len(order)
	for i, p := range order {
		if p == PriorityWebhook && i > lastWebhook {
			lastWebhook = i
		}
		if p == PriorityVerify && i < firstVerify {
			firstVerify = i
		}
	}
	// the first token may land before the webhooks are queued; after that
	// every webhook must precede every remaining verify
	if firstVerify == 0 {
		order = order[1:]
		lastWebhook, firstVerify = -1, len(order)
		for i, p := range order {
			if p == PriorityWebhook && i > lastWebhook {
				lastWebhook = i
			}
			if p == PriorityVerify && i < firstVerify {
				firstVerify = i
			}
		}
	}
	if lastWebhook > firstVerify {
		t.Errorf("webhook starved: grant order %v", order)
	}
}

func TestLimiterAIMD(t *testing.T) {
	l := NewLimiter(100)
	defer l.Stop()

	// the refill goroutine reads the clock concurrently, so advancing it
	// must be synchronized
	var clockMu sync.Mutex
	clock := time.Now()
	advance := func(d time.Duration) {
		clockMu.Lock()
		clock = clock.Add(d)
		clockMu.Unlock()
	}
	l.mu.Lock()
	l.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	l.mu.Unlock()

	l.On429(0)
	if got := l.EffectiveRPS(); got != 50 {
		t.Fatalf("after one 429: rate = %v, want 50", got)
	}
	l.On429(0)
	if got := l.EffectiveRPS(); got != 25 {
		t.Fatalf("after two 429s: rate = %v, want 25", got)
	}

	// recovery: 10% per minute, multiplicative
	advance(time.Minute)
	if got := l.EffectiveRPS(); got < 27.4 || got > 27.6 {
		t.Errorf("after 1 minute: rate = %v, want about 27.5", got)
	}
	// long quiet period: fully recovered, capped at the configured ceiling
	advance(30 * time.Minute)
	if got := l.EffectiveRPS(); got != 100 {
		t.Errorf("after long recovery: rate = %v, want the 100 ceiling", got)
	}
}

func TestLimiterRetryAfterDelaysGrants(t *testing.T) {
	l := NewLimiter(100)
	defer l.Stop()
	ctx := context.Background()

	// warm the bucket, then a 429 with Retry-After must hold grants back
	if err := l.Acquire(ctx, PriorityWebhook); err != nil {
		t.Fatal(err)
	}
	l.On429(300 * time.Millisecond)

	start := time.Now()
	if err := l.Acquire(ctx, PriorityWebhook); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("grant after %v, want at least the 300ms Retry-After", elapsed)
	}
}

func TestLimiterAcquireHonorsContext(t *testing.T) {
	l := NewLimiter(1)
	defer l.Stop()

	// drain the bucket so the next acquire must wait
	if err := l.Acquire(context.Background(), PriorityWebhook); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := l.Acquire(ctx, PriorityBackfill)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Error("cancelled acquire took too long to return")
	}
}

func TestLimiterStop(t *testing.T) {
	l := NewLimiter(1)
	if err := l.Acquire(context.Background(), PriorityWebhook); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan error, 1)
	go func() { blocked <- l.Acquire(context.Background(), PriorityWebhook) }()
	time.Sleep(20 * time.Millisecond)
	l.Stop()

	select {
	case err := <-blocked:
		if !errors.Is(err, ErrLimiterClosed) {
			t.Errorf("blocked acquire = %v, want ErrLimiterClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked acquire did not return after Stop")
	}
}
