package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func TestBackfillDryRun(t *testing.T) {
	_, connString := testpg.StartWithURL(t)

	out, err := runCLI(t, connString, "backfill", "--dry-run", "--since", "2026-01-01")
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !strings.Contains(out, "dry run") || !strings.Contains(out, "customer") {
		t.Errorf("dry run output missing plan:\n%s", out)
	}
	if !strings.Contains(out, "created >= 2026-01-01") {
		t.Errorf("dry run output missing since filter:\n%s", out)
	}
	// order visible: products before customers before invoices
	if strings.Index(out, "product") > strings.Index(out, "customer") {
		t.Errorf("plan order wrong:\n%s", out)
	}
}

func TestBackfillCommandRuns(t *testing.T) {
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, "whsec_cli")
	t.Setenv("DRIFTLESS_STRIPE_API_BASE_URL", fs.URL())

	fs.Put("customer", "cus_cli", map[string]any{"email": "cli@x.y"}, "customer.created")

	out, err := runCLI(t, connString, "backfill", "--type", "customers")
	if err != nil {
		t.Fatalf("backfill: %v\n%s", err, out)
	}
	if !strings.Contains(out, "complete") {
		t.Errorf("output missing completion:\n%s", out)
	}

	var email string
	if err := pool.QueryRow(context.Background(),
		`SELECT email FROM stripe.customers WHERE id = 'cus_cli'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "cli@x.y" {
		t.Errorf("email = %q", email)
	}
}

func TestBackfillFlagValidation(t *testing.T) {
	_, connString := testpg.StartWithURL(t)

	for name, args := range map[string][]string{
		"resume with type": {"backfill", "--resume", "3", "--type", "customers"},
		"full with since":  {"backfill", "--full", "--since", "2026-01-01"},
		"bad since":        {"backfill", "--since", "January"},
		"unknown type":     {"backfill", "--type", "plans"},
		"unlistable type":  {"backfill", "--type", "subscription_items"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := runCLI(t, connString, args...)
			if err == nil || exitCode(err) != 2 {
				t.Errorf("err=%v exit=%d, want usage error", err, exitCode(err))
			}
		})
	}
}

func TestBackfillOptionsNormalization(t *testing.T) {
	opts, err := backfillOptions(false, "2026-02-03", []string{"customers", "invoice"}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Since == nil || !opts.Since.Equal(time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("since = %v", opts.Since)
	}
	if len(opts.Types) != 2 || opts.Types[0] != "customer" || opts.Types[1] != "invoice" {
		t.Errorf("types = %v: plurals must normalize", opts.Types)
	}
}
