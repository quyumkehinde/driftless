package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestImportThenVerifyRepair is the zero-downtime migration story: copy a
// sync-engine database's reconstructed rows in, then let verify --repair
// re-fetch true state until a final verify runs clean.
func TestImportThenVerifyRepair(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	// Stripe's truth: full objects with fields the sync-engine copy lacks
	const customers = 10
	for i := range customers {
		fs.Put("customer", fmt.Sprintf("cus_mig%02d", i), map[string]any{
			"email": fmt.Sprintf("m%02d@x.y", i), "name": fmt.Sprintf("M %02d", i),
			"currency": "usd", "balance": 0,
		}, "customer.created")
	}

	// the sync-engine leftovers: typed subset of the same customers
	if _, err := pool.Exec(t.Context(), `
		CREATE SCHEMA sync_engine;
		CREATE TABLE sync_engine.customers (
			id text PRIMARY KEY, email text, created integer,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		t.Fatal(err)
	}
	for i := range customers {
		if _, err := pool.Exec(t.Context(), `
			INSERT INTO sync_engine.customers (id, email, created)
			VALUES ($1, $2, $3)`,
			fmt.Sprintf("cus_mig%02d", i), fmt.Sprintf("m%02d@x.y", i), 1700000000+i); err != nil {
			t.Fatal(err)
		}
	}

	imp := startCLI(t, binary, connString, fs.URL(), "import", "--from-sync-engine", "--schema", "sync_engine")
	if code := imp.WaitCode(); code != 0 {
		t.Fatalf("import exit = %d\n%s", code, imp.Output())
	}
	if out := imp.Output(); !strings.Contains(out, "imported 10 row(s)") || !strings.Contains(out, "verify --repair") {
		t.Errorf("import output must count rows and point at verify --repair:\n%s", out)
	}

	// reconstructed rows are incomplete by design: verify sees them all
	// as stale and repair re-fetches the truth
	code, report := runVerify(t, binary, connString, fs.URL(), "--full", "--repair")
	if code != 3 || report.Drifted != customers || report.Repaired != customers {
		t.Fatalf("repair pass: exit=%d drifted=%d repaired=%d, want 3 and %d of each\n",
			code, report.Drifted, report.Repaired, customers)
	}

	code, report = runVerify(t, binary, connString, fs.URL(), "--full")
	if code != 0 || report.Drifted != 0 {
		t.Errorf("after repair: exit=%d drifted=%d, want a clean mirror; drifts=%v", code, report.Drifted, report.Drifts)
	}
}
