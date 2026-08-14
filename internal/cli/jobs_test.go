package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// runCLI executes the root command against a test database via env config.
func runCLI(t *testing.T, connString string, args ...string) (string, error) {
	t.Helper()
	t.Setenv("DRIFTLESS_DATABASE_URL", connString)
	t.Setenv("DRIFTLESS_STRIPE_API_KEY", "rk_test_cli")

	root := NewRootCmd()
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestJobsListAndRetry(t *testing.T) {
	pool, connString := testpg.StartWithURL(t)
	q := queue.New(pool, 2*time.Minute)
	ctx := context.Background()

	// seed one job and kill it
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, _, err := q.Enqueue(ctx, tx, queue.EnqueueParams{
			ObjectType: "customer", ObjectID: "cus_dead",
			EventID: "evt_1", EventCreated: time.Now().UTC(),
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE driftless.jobs SET status='dead', attempts=8, last_error='stripe exploded'`); err != nil {
		t.Fatal(err)
	}

	out, err := runCLI(t, connString, "jobs", "list", "--status", "dead")
	if err != nil {
		t.Fatalf("jobs list: %v", err)
	}
	if !strings.Contains(out, "customer:cus_dead") || !strings.Contains(out, "stripe exploded") {
		t.Errorf("list output missing job details:\n%s", out)
	}

	out, err = runCLI(t, connString, "jobs", "retry", "--all-dead")
	if err != nil {
		t.Fatalf("jobs retry: %v", err)
	}
	if !strings.Contains(out, "requeued 1 dead job(s)") {
		t.Errorf("retry output: %s", out)
	}

	var status string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT status, attempts FROM driftless.jobs`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 0 {
		t.Errorf("after retry: status=%s attempts=%d, want pending/0", status, attempts)
	}

	out, err = runCLI(t, connString, "jobs", "list", "--status", "dead")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "no dead jobs") {
		t.Errorf("expected empty dead list, got:\n%s", out)
	}
}

func TestJobsRetryUsageErrors(t *testing.T) {
	_, connString := testpg.StartWithURL(t)

	// neither --all-dead nor an id
	_, err := runCLI(t, connString, "jobs", "retry")
	if err == nil || exitCode(err) != 2 {
		t.Errorf("bare retry: err=%v exit=%d, want usage error", err, exitCode(err))
	}

	// both at once
	_, err = runCLI(t, connString, "jobs", "retry", "--all-dead", "42")
	if err == nil || exitCode(err) != 2 {
		t.Errorf("conflicting retry args: err=%v exit=%d, want usage error", err, exitCode(err))
	}

	// well-formed but nonexistent id: runtime error, not usage
	_, err = runCLI(t, connString, "jobs", "retry", "42")
	if err == nil || exitCode(err) != 1 {
		t.Errorf("missing job: err=%v exit=%d, want runtime error", err, exitCode(err))
	}
}
