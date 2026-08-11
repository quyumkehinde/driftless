// Command driftless is the entrypoint for the driftless CLI.
package main

import (
	"os"

	"github.com/quyumkehinde/driftless/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
