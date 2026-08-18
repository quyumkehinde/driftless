package e2e

import (
	"strings"
	"testing"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestStatusSummarizesTheMirror exercises the one-screen summary against
// an account that has really been backfilled and verified.
func TestStatusSummarizesTheMirror(t *testing.T) {
	binary := buildBinary(t)
	_, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)
	seedLargeAccount(t, fs, 5)

	if ok := startCLI(t, binary, connString, fs.URL(), "backfill", "--full").Wait(); !ok {
		t.Fatal("backfill failed")
	}
	if code := startCLI(t, binary, connString, fs.URL(), "verify", "--full").WaitCode(); code != 0 {
		t.Fatalf("verify exit = %d, want 0", code)
	}

	proc := startCLI(t, binary, connString, fs.URL(), "status")
	if code := proc.WaitCode(); code != 0 {
		t.Fatalf("status exit = %d, want 0\n%s", code, proc.Output())
	}
	out := proc.Output()

	for _, want := range []string{
		"queue       pending=0 running=0 dead=0",
		"backfill    run 1 done",
		"tasks=12/12",
		"verify      last full",
		"drifted=0",
		"unhandled   none",
		"sweeps      none yet",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q:\n%s", want, out)
		}
	}
}
