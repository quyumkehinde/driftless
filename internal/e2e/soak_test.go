package e2e

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestSoak is the long-run acceptance: sustained webhook load with
// periodic kill -9s, a full verification every cycle, and zero drift at
// the end. It only runs when explicitly asked:
//
//	DRIFTLESS_SOAK=1 DRIFTLESS_SOAK_HOURS=24 \
//	  go test -race -run TestSoak -timeout 26h ./internal/e2e/
func TestSoak(t *testing.T) {
	if os.Getenv("DRIFTLESS_SOAK") != "1" {
		t.Skip("set DRIFTLESS_SOAK=1 to run the soak")
	}
	hours := 24.0
	if raw := os.Getenv("DRIFTLESS_SOAK_HOURS"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("DRIFTLESS_SOAK_HOURS=%q: %v", raw, err)
		}
		hours = parsed
	}
	deadline := time.Now().Add(time.Duration(hours * float64(time.Hour)))

	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	var mu sync.Mutex
	proc := startServe(t, binary, connString, fs.URL(), "")
	target := func() string {
		mu.Lock()
		defer mu.Unlock()
		return proc.IngestURL
	}

	var generated, kills, cycles atomic.Int64

	for cycle := 1; time.Now().Before(deadline); cycle++ {
		cycleEnd := time.Now().Add(50 * time.Minute)
		if cycleEnd.After(deadline) {
			cycleEnd = deadline
		}

		// load: steady mutations delivered with retries, kill -9 roughly
		// every ten minutes
		nextKill := time.Now().Add(10 * time.Minute)
		for i := 0; time.Now().Before(cycleEnd); i++ {
			objectID := fmt.Sprintf("cus_soak_%d_%d", cycle, i)
			event := fs.Put("customer", objectID, map[string]any{
				"email": fmt.Sprintf("s%d@x.y", i),
			}, "customer.created")
			generated.Add(1)

			for attempt := 0; ; attempt++ {
				status, err := fs.TryDeliver(target(), event.ID)
				if err == nil && status == http.StatusOK {
					break
				}
				if attempt > 2000 {
					t.Fatalf("cycle %d: event %s never acknowledged", cycle, event.ID)
				}
				time.Sleep(50 * time.Millisecond)
			}

			if time.Now().After(nextKill) {
				mu.Lock()
				proc.Kill9(t)
				proc = startServe(t, binary, connString, fs.URL(), "")
				mu.Unlock()
				kills.Add(1)
				nextKill = time.Now().Add(10 * time.Minute)
			}
			time.Sleep(200 * time.Millisecond)
		}

		// settle, then the cycle's proof: a full verification of the
		// whole mirror against the double
		waitForDrain(t, pool)
		verify := startCLI(t, binary, connString, fs.URL(), "verify", "--full", "--format", "json")
		if code := verify.WaitCode(); code != 0 {
			t.Fatalf("cycle %d: verify exit = %d\n%s", cycle, code, verify.Output())
		}
		cycles.Add(1)
		t.Logf("cycle %d clean: %d events generated, %d kills so far",
			cycle, generated.Load(), kills.Load())
	}

	events := countRow(t, pool, `SELECT count(*) FROM driftless.events`)
	if int64(events) != generated.Load() {
		t.Errorf("event rows = %d, generated %d: loss or double-recording", events, generated.Load())
	}
	dead := countRow(t, pool, `SELECT count(*) FROM driftless.jobs WHERE status = 'dead'`)
	if dead != 0 {
		t.Errorf("dead jobs = %d, want 0", dead)
	}
	t.Logf("soak done: %d cycles, %d events, %d kills, zero drift",
		cycles.Load(), generated.Load(), kills.Load())
}
