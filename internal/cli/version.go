package cli

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// Stamped by goreleaser via -ldflags "-X .../internal/cli.version=..." etc.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the driftless version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Println(versionString())
		},
	}
}

// versionString renders "driftless <version> (<commit>, <date>, <go>)",
// falling back to build info for non-goreleaser builds (go install / go build).
func versionString() string {
	v, c, d := version, commit, date
	if v == "dev" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
				v = bi.Main.Version
			}
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					if c == "none" && len(s.Value) >= 12 {
						c = s.Value[:12]
					}
				case "vcs.time":
					if d == "unknown" {
						d = s.Value
					}
				}
			}
		}
	}
	return fmt.Sprintf("driftless %s (%s, %s, %s)", v, c, d, runtime.Version())
}
