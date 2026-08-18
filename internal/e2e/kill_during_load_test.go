package e2e

import (
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestKillDuringLoad is the milestone acceptance scenario: sustained
// synthetic webhook load with kill -9 mid-stream, and zero lost or
// double-recorded events afterward.
//
// The CI shape is scaled down. Set DRIFTLESS_ACCEPTANCE=1 for the full
// run: 500 rps for 10s, 3 kills, and the p99 ack latency assertion.
func TestKillDuringLoad(t *testing.T) {
	rps, duration, kills, assertLatency := 100, 3*time.Second, 1, false
	if os.Getenv("DRIFTLESS_ACCEPTANCE") == "1" {
		rps, duration, kills, assertLatency = 500, 10*time.Second, 3, true
	}

	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	// current serve target, replaced on every restart
	var mu sync.Mutex
	proc := startServe(t, binary, connString, fs.URL(), "")
	target := func() string {
		mu.Lock()
		defer mu.Unlock()
		return proc.IngestURL
	}

	// generators: mutate fakestripe, then deliver with retries until acked,
	// the way Stripe treats non-2xx responses
	const workers = 20
	interval := time.Duration(int64(time.Second) * int64(workers) / int64(rps))
	deadline := time.Now().Add(duration)

	var latencies sync.Map
	var generated, acked atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				objectID := fmt.Sprintf("cus_w%d_%d", w, i)
				event := fs.Put("customer", objectID, nil, "customer.created")
				generated.Add(1)

				for attempt := 0; ; attempt++ {
					start := time.Now()
					status, err := fs.TryDeliver(target(), event.ID)
					if err == nil && status == http.StatusOK {
						latencies.Store(event.ID, time.Since(start))
						acked.Add(1)
						break
					}
					if attempt > 400 {
						t.Errorf("event %s: no ack after %d attempts", event.ID, attempt)
						return
					}
					time.Sleep(25 * time.Millisecond)
				}
				time.Sleep(interval)
			}
		}()
	}

	// killer, on the test goroutine so startServe and Kill9 may t.Fatal:
	// evenly spaced kill -9 plus restart while the generators run
	step := duration / time.Duration(kills+1)
	for i := range kills {
		time.Sleep(step)
		if !time.Now().Before(deadline) {
			t.Errorf("kill %d would land after the load window; %d kills overlapped load", i+1, i)
			break
		}
		mu.Lock()
		proc.Kill9(t)
		proc = startServe(t, binary, connString, fs.URL(), "")
		mu.Unlock()
	}
	wg.Wait()

	// invariants: every generated event was acknowledged, and exists
	// exactly once with exactly one job
	if generated.Load() != acked.Load() {
		t.Errorf("generated %d events, acked %d", generated.Load(), acked.Load())
	}
	eventCount := countRow(t, pool, `SELECT count(*) FROM driftless.events`)
	jobCount := countRow(t, pool, `SELECT count(*) FROM driftless.jobs`)
	extraJobs := countRow(t, pool,
		`SELECT count(*) FROM (SELECT object_id FROM driftless.jobs GROUP BY object_id HAVING count(*) > 1) d`)
	if int64(eventCount) != generated.Load() {
		t.Errorf("event rows = %d, want %d: no loss, no double-recording", eventCount, generated.Load())
	}
	if int64(jobCount) != generated.Load() || extraJobs != 0 {
		t.Errorf("job rows = %d (%d objects with extras), want %d distinct", jobCount, extraJobs, generated.Load())
	}

	if assertLatency {
		var all []time.Duration
		latencies.Range(func(_, v any) bool {
			all = append(all, v.(time.Duration))
			return true
		})
		slices.Sort(all)
		p99 := all[len(all)*99/100]
		t.Logf("acked %d events, p99 ack = %v", len(all), p99)
		if p99 > 50*time.Millisecond {
			t.Errorf("p99 ack = %v, want < 50ms", p99)
		}
	}
}
