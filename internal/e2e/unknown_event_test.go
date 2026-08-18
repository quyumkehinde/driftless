package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestUnknownEventTypeStored pins the unknown-type policy end to end: an
// event type outside the contract is stored and counted, never silently
// dropped, and the process stays healthy.
func TestUnknownEventTypeStored(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	event := fs.Put("plan", "plan_1", map[string]any{"amount": 999}, "plan.created")
	proc := startServe(t, binary, connString, fs.URL(), "")

	statuses := fs.Deliver(t, proc.IngestURL, event.ID)
	if statuses[0] != http.StatusOK {
		t.Fatalf("status = %d: unknown types are acknowledged, not rejected", statuses[0])
	}

	// stored, no job
	eventCount := countRow(t, pool, `SELECT count(*) FROM driftless.events WHERE event_id = $1`, event.ID)
	jobCount := countRow(t, pool, `SELECT count(*) FROM driftless.jobs`)
	if eventCount != 1 || jobCount != 0 {
		t.Errorf("events=%d jobs=%d, want 1 and 0", eventCount, jobCount)
	}

	// counted in the unhandled metric
	waitFor(t, 10*time.Second, "unhandled metric", func() bool {
		resp, err := http.Get(proc.MetricsURL + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		return strings.Contains(string(body),
			`driftless_events_unhandled_total{type="plan.created"} 1`)
	})

	// the process is still serving
	resp, err := http.Get(proc.IngestURL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d after unknown event", resp.StatusCode)
	}
}
