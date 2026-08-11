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
// schema migrations are pending: exit code 4. Defined here for serve/verify
// (later milestones) so the exit-code contract lives in one place.
type MigrationsPendingError struct {
	Pending int
}

func (e *MigrationsPendingError) Error() string {
	return fmt.Sprintf("%d migration(s) pending; run 'driftless migrate up'", e.Pending)
}

// ExitCode returns the exit code for pending migrations.
func (e *MigrationsPendingError) ExitCode() int { return 4 }

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
