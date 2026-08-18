package cli

import (
	"strings"
	"testing"
)

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
