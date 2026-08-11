package main

import (
	"os"

	"github.com/quyumkehinde/driftless/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
