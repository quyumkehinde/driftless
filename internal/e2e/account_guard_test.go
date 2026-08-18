package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestAccountGuard walks the guard's whole lifecycle: first serve records
// the account, a mismatched key refuses to start, --force-account
// overwrites, and backfill honors the same guard.
func TestAccountGuard(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	// first serve records the account in driftless.meta
	startServe(t, binary, connString, fs.URL(), "")
	waitFor(t, 10*time.Second, "meta row recorded", func() bool {
		var recorded string
		err := pool.QueryRow(t.Context(),
			`SELECT stripe_account_id FROM driftless.meta`).Scan(&recorded)
		return err == nil && recorded == fakestripe.AccountID
	})

	// a database recorded against someone else's account refuses to serve
	if _, err := pool.Exec(t.Context(),
		`UPDATE driftless.meta SET stripe_account_id = 'acct_someone_else'`); err != nil {
		t.Fatal(err)
	}
	secretEnv := []string{"DRIFTLESS_STRIPE_WEBHOOK_SECRET=" + e2eSecret}
	refused := startCLIWithEnv(t, binary, connString, fs.URL(), secretEnv, "serve")
	if code := refused.WaitCode(); code != 1 {
		t.Fatalf("mismatched serve exit = %d, want 1\n%s", code, refused.Output())
	}
	if out := refused.Output(); !strings.Contains(out, "acct_someone_else") || !strings.Contains(out, "--force-account") {
		t.Errorf("refusal must name the recorded account and the override flag:\n%s", out)
	}

	// backfill refuses against the same mismatch
	backfillProc := startCLI(t, binary, connString, fs.URL(), "backfill", "--full")
	if code := backfillProc.WaitCode(); code != 1 {
		t.Errorf("mismatched backfill exit = %d, want 1\n%s", code, backfillProc.Output())
	}

	// the explicit override rewrites the record and serves
	forced := startCLIWithEnv(t, binary, connString, fs.URL(), secretEnv, "serve", "--force-account")
	waitFor(t, 10*time.Second, "forced serve to rewrite meta", func() bool {
		var recorded string
		err := pool.QueryRow(t.Context(),
			`SELECT stripe_account_id FROM driftless.meta`).Scan(&recorded)
		return err == nil && recorded == fakestripe.AccountID
	})
	forced.Kill9(t)
}
