//go:build crashpoint

package crashpoint

import (
	"fmt"
	"os"
)

// Maybe exits the process hard, skipping all defers the way kill -9 would,
// when the DRIFTLESS_CRASHPOINT environment variable names this point.
func Maybe(name string) {
	if os.Getenv("DRIFTLESS_CRASHPOINT") == name {
		fmt.Fprintf(os.Stderr, "crashpoint %s: exiting\n", name)
		os.Exit(137)
	}
}
