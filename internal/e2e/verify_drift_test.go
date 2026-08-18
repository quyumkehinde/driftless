package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// verifyReport is the CLI's json output shape, decoded for assertions.
type verifyReport struct {
	Mode   string `json:"mode"`
	Drifts []struct {
		ObjectType string `json:"object_type"`
		ObjectID   string `json:"object_id"`
		Kind       string `json:"kind"`
		Repaired   bool   `json:"repaired"`
	} `json:"drifts"`
	Checked  int `json:"checked"`
	Drifted  int `json:"drifted"`
	Repaired int `json:"repaired"`
}

func runVerify(t *testing.T, binary, connString, apiBaseURL string, args ...string) (int, verifyReport) {
	t.Helper()
	proc := startCLI(t, binary, connString, apiBaseURL,
		append([]string{"verify", "--format", "json"}, args...)...)
	code := proc.WaitCode()
	// the report is the first json value; the exit-3 error line follows it
	var report verifyReport
	if err := json.NewDecoder(strings.NewReader(proc.Output())).Decode(&report); err != nil {
		t.Fatalf("verify output is not a json report (exit %d): %v\n%s", code, err, proc.Output())
	}
	return code, report
}

// TestVerifyDetectsAndRepairsDrift is the milestone acceptance scenario:
// mutate a slice of the account without delivering events, then require
// full verify to report exactly the injected drift set, repair to zero it,
// and a final verify to exit clean.
func TestVerifyDetectsAndRepairsDrift(t *testing.T) {
	const customers = 40
	binary := buildBinary(t)
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)
	seedLargeAccount(t, fs, customers)

	if ok := startCLI(t, binary, connString, fs.URL(), "backfill", "--full").Wait(); !ok {
		t.Fatal("backfill failed")
	}

	// a synced mirror verifies clean: exit 0 is the healthy CI signal
	code, report := runVerify(t, binary, connString, fs.URL(), "--full")
	if code != 0 || report.Drifted != 0 {
		t.Fatalf("clean mirror: exit=%d drifted=%d, want 0 and 0; drifts=%v", code, report.Drifted, report.Drifts)
	}

	// inject drift into about 3% of the account without delivering a
	// single event: updates, an upstream delete, and a never-seen object
	injected := map[string]string{}
	for i := range 3 {
		id := fmt.Sprintf("cus_%05d", i)
		fs.Put("customer", id, map[string]any{"email": "drifted@x.y"}, "customer.updated")
		injected["customer:"+id] = "stale"
	}
	fs.Put("invoice", "in_00001", map[string]any{
		"customer": "cus_00001", "subscription": "sub_00001", "status": "void",
		"total": 4900, "amount_paid": 0, "amount_due": 4900, "currency": "usd",
	}, "invoice.voided")
	injected["invoice:in_00001"] = "stale"
	fs.Delete("customer", "cus_00039", "customer.deleted")
	injected["customer:cus_00039"] = "orphaned"
	fs.Put("customer", "cus_fresh", map[string]any{"email": "fresh@x.y"}, "customer.created")
	injected["customer:cus_fresh"] = "missing"

	// full verify reports exactly the injected set, and exits 3 for CI
	code, report = runVerify(t, binary, connString, fs.URL(), "--full")
	if code != 3 {
		t.Errorf("drifted mirror: exit = %d, want 3", code)
	}
	found := map[string]string{}
	for _, d := range report.Drifts {
		found[d.ObjectType+":"+d.ObjectID] = d.Kind
	}
	if len(found) != len(injected) {
		t.Errorf("drift set = %v, want exactly %v", found, injected)
	}
	for key, kind := range injected {
		if found[key] != kind {
			t.Errorf("%s reported %q, want %q", key, found[key], kind)
		}
	}

	// quick verify with a wide window catches the same drift cheaply
	code, report = runVerify(t, binary, connString, fs.URL(), "--quick", "--since", "2020-01-01")
	if code != 3 || report.Mode != "quick" {
		t.Errorf("quick: exit=%d mode=%q, want 3 and quick", code, report.Mode)
	}

	// repair fixes everything it reports, still exiting 3 so the run that
	// found drift is never mistaken for a clean one
	code, report = runVerify(t, binary, connString, fs.URL(), "--full", "--repair")
	if code != 3 || report.Repaired != report.Drifted || report.Drifted != len(injected) {
		t.Errorf("repair: exit=%d drifted=%d repaired=%d, want 3 and %d of each",
			code, report.Drifted, report.Repaired, len(injected))
	}

	// the repaired mirror is clean
	code, report = runVerify(t, binary, connString, fs.URL(), "--full")
	if code != 0 || report.Drifted != 0 {
		t.Errorf("after repair: exit=%d drifted=%d, want 0 and 0; drifts=%v", code, report.Drifted, report.Drifts)
	}
}
