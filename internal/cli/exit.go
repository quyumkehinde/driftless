package cli

import (
	"errors"
	"fmt"
	"strings"
)

// ExitCoder is implemented by errors that map to a specific process exit code.
type ExitCoder interface {
	ExitCode() int
}

// usageError wraps flag/argument parse failures: exit code 2.
type usageError struct {
	err error
}

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }
func (e *usageError) ExitCode() int { return 2 }

// MigrationsPendingError is returned by commands that refuse to run while
// schema migrations are pending: exit code 4.
type MigrationsPendingError struct {
	Pending int
}

func (e *MigrationsPendingError) Error() string {
	return fmt.Sprintf("%d migration(s) pending; run 'driftless migrate up'", e.Pending)
}

// ExitCode returns the exit code for pending migrations.
func (e *MigrationsPendingError) ExitCode() int { return 4 }

// DriftError is returned when verify finds diverged objects: exit code 3,
// the code CI pipelines gate on. Repaired drift still exits 3; the next
// clean verify proves the repair.
type DriftError struct {
	Drifted  int
	Repaired int
}

func (e *DriftError) Error() string {
	if e.Repaired > 0 {
		return fmt.Sprintf("verify: %d object(s) drifted, %d repaired", e.Drifted, e.Repaired)
	}
	return fmt.Sprintf("verify: %d object(s) drifted", e.Drifted)
}

// ExitCode returns the exit code for found drift.
func (e *DriftError) ExitCode() int { return 3 }

// exitCode maps an error returned by cobra to the documented exit code.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ec ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	// cobra reports unknown subcommands with an untyped error; treat as usage.
	if strings.HasPrefix(err.Error(), "unknown command ") {
		return 2
	}
	return 1
}
