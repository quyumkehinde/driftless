package cli

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// runCLIWithInput is runCLI with stdin driving interactive prompts.
func runCLIWithInput(t *testing.T, connString, input string, args ...string) (string, error) {
	t.Helper()
	t.Setenv("DRIFTLESS_DATABASE_URL", connString)
	t.Setenv("DRIFTLESS_STRIPE_API_KEY", "rk_test_cli")

	root := NewRootCmd()
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(strings.NewReader(input))
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestInitDeclinedBackfill(t *testing.T) {
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, "whsec_init")
	t.Setenv("DRIFTLESS_STRIPE_API_BASE_URL", fs.URL())

	out, err := runCLIWithInput(t, connString, "n\n", "init")
	if err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	for _, want := range []string{
		"database: reachable",
		"key works, account " + fakestripe.AccountID,
		"migrations: up to date",
		"skipped",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestInitAcceptedBackfill(t *testing.T) {
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, "whsec_init")
	t.Setenv("DRIFTLESS_STRIPE_API_BASE_URL", fs.URL())
	fs.Put("customer", "cus_init", map[string]any{"email": "init@x.y"}, "customer.created")

	out, err := runCLIWithInput(t, connString, "y\n", "init")
	if err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	if !strings.Contains(out, "driftless is ready") {
		t.Errorf("output missing completion:\n%s", out)
	}

	var email, requestedBy string
	if err := pool.QueryRow(context.Background(),
		`SELECT email FROM stripe.customers WHERE id = 'cus_init'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(),
		`SELECT requested_by FROM driftless.backfill_runs`).Scan(&requestedBy); err != nil {
		t.Fatal(err)
	}
	if email != "init@x.y" || requestedBy != "auto-init" {
		t.Errorf("email=%q requested_by=%q", email, requestedBy)
	}
}

func TestInitBadStripeKey(t *testing.T) {
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, "whsec_init")
	t.Setenv("DRIFTLESS_STRIPE_API_BASE_URL", fs.URL())
	fs.FailNext(1, http.StatusUnauthorized)

	_, err := runCLIWithInput(t, connString, "n\n", "init")
	if err == nil || !strings.Contains(err.Error(), "api key check failed") {
		t.Errorf("err = %v, want key check failure", err)
	}
}
