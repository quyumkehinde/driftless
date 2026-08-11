package cli

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"plain error", errors.New("boom"), 1},
		{"usage error", &usageError{err: errors.New("unknown flag: --nope")}, 2},
		{"wrapped usage error", fmt.Errorf("context: %w", &usageError{err: errors.New("bad flag")}), 2},
		{"migrations pending", &MigrationsPendingError{Pending: 3}, 4},
		{"unknown command", errors.New(`unknown command "frob" for "driftless"`), 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCode(tt.err); got != tt.want {
				t.Errorf("exitCode(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestVersionCommand(t *testing.T) {
	root := NewRootCmd()
	var out strings.Builder
	root.SetOut(&out)
	root.SetArgs([]string{"version"})
	if err := root.Execute(); err != nil {
		t.Fatalf("version command failed: %v", err)
	}
	got := out.String()
	if !strings.HasPrefix(got, "driftless ") {
		t.Errorf("version output %q does not start with 'driftless '", got)
	}
	if !strings.Contains(got, "go1.") {
		t.Errorf("version output %q does not contain the go version", got)
	}
}

func TestUnknownFlagIsUsageError(t *testing.T) {
	root := NewRootCmd()
	root.SetOut(&strings.Builder{})
	root.SetErr(&strings.Builder{})
	root.SetArgs([]string{"version", "--nope"})
	err := root.Execute()
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
	if got := exitCode(err); got != 2 {
		t.Errorf("exitCode for unknown flag = %d, want 2", got)
	}
}
