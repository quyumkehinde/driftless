// Command gendocs regenerates the CLI reference in docs/cli from the
// command definitions, so the published reference can never drift from
// the binary's own help text. CI fails when the output is stale.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra/doc"

	"github.com/quyumkehinde/driftless/internal/cli"
)

func main() {
	dir := filepath.Join("docs", "cli")
	if err := regenerate(dir); err != nil {
		fmt.Fprintln(os.Stderr, "gendocs:", err)
		os.Exit(1)
	}
}

func regenerate(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	root := cli.NewRootCmd()
	root.DisableAutoGenTag = true
	return doc.GenMarkdownTree(root, dir)
}
