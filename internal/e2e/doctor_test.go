package e2e

import (
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestDoctorHealthyEnvironment runs doctor against a working setup and
// requires a clean bill plus the restricted-key acknowledgment.
func TestDoctorHealthyEnvironment(t *testing.T) {
	binary := buildBinary(t)
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	proc := startCLI(t, binary, connString, fs.URL(), "doctor")
	if code := proc.WaitCode(); code != 0 {
		t.Fatalf("doctor exit = %d, want 0\n%s", code, proc.Output())
	}
	out := proc.Output()
	for _, want := range []string{
		"database", "connected",
		"migrations", "schema current",
		"stripe api", "key works",
		"account guard", "not initialized",
		"key scope", "restricted key",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor output missing %q:\n%s", want, out)
		}
	}
}

// TestDoctorDetectsWrongWebhookURL plants the classic misconfiguration
// signature: sweep-recovered events, zero webhook arrivals.
func TestDoctorDetectsWrongWebhookURL(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.events (event_id, type, created, source, payload, livemode)
		VALUES ('evt_swept', 'customer.created', now(), 'sweep', '{}', false)`); err != nil {
		t.Fatal(err)
	}

	proc := startCLI(t, binary, connString, fs.URL(), "doctor")
	if code := proc.WaitCode(); code != 0 {
		t.Fatalf("doctor exit = %d, want 0: wrong URL is a warning, not a failure\n%s", code, proc.Output())
	}
	if !strings.Contains(proc.Output(), "no webhook has ever arrived") {
		t.Errorf("doctor missed the wrong-URL signature:\n%s", proc.Output())
	}
}

// TestDoctorFailsOnAccountMismatch points a key at a database that
// mirrors a different account.
func TestDoctorFailsOnAccountMismatch(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.meta (stripe_account_id, livemode, schema_version)
		VALUES ('acct_someone_else', false, '1')`); err != nil {
		t.Fatal(err)
	}

	proc := startCLI(t, binary, connString, fs.URL(), "doctor")
	if code := proc.WaitCode(); code != 1 {
		t.Fatalf("doctor exit = %d, want 1 on account mismatch\n%s", code, proc.Output())
	}
	if !strings.Contains(proc.Output(), "acct_someone_else") {
		t.Errorf("doctor mismatch line must name the recorded account:\n%s", proc.Output())
	}
}

// TestEventsShowDumpsStoredEvent round-trips one delivered webhook
// through the CLI dump.
func TestEventsShowDumpsStoredEvent(t *testing.T) {
	binary := buildBinary(t)
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	proc := startServe(t, binary, connString, fs.URL(), "")
	event := fs.Put("customer", "cus_show", map[string]any{"email": "show@x.y"}, "customer.created")
	if status, err := fs.TryDeliver(proc.IngestURL, event.ID); err != nil || status != 200 {
		t.Fatalf("deliver: status=%d err=%v", status, err)
	}

	show := startCLI(t, binary, connString, fs.URL(), "events", "show", event.ID)
	if code := show.WaitCode(); code != 0 {
		t.Fatalf("events show exit = %d\n%s", code, show.Output())
	}
	out := show.Output()
	for _, want := range []string{event.ID, "customer.created", "source        webhook", `"email": "show@x.y"`} {
		if !strings.Contains(out, want) {
			t.Errorf("events show missing %q:\n%s", want, out)
		}
	}

	missing := startCLI(t, binary, connString, fs.URL(), "events", "show", "evt_nope")
	if code := missing.WaitCode(); code != 1 {
		t.Errorf("events show for unknown id exit = %d, want 1", code)
	}
}
