// Package cli implements the driftless command tree.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootFlags holds the global flag values shared by all subcommands.
type rootFlags struct {
	configPath string
	logLevel   string
	logFormat  string
}

// NewRootCmd builds the driftless root command and its subcommand tree.
func NewRootCmd() *cobra.Command {
	flags := &rootFlags{}

	root := &cobra.Command{
		Use:   "driftless",
		Short: "Keep your Postgres in sync with Stripe",
		Long: "Driftless receives Stripe webhooks, detects delivery gaps, backfills history,\n" +
			"and materializes everything into a stripe schema in your own Postgres.",
		// main prints errors exactly once; suppress cobra's own printing.
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&flags.configPath, "config", "",
		"path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)")
	root.PersistentFlags().StringVar(&flags.logLevel, "log-level", "",
		"log level: debug|info|warn|error (overrides config)")
	root.PersistentFlags().StringVar(&flags.logFormat, "log-format", "",
		"log format: json|text (overrides config)")

	// Flag parse failures are usage errors: exit code 2.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &usageError{err: err}
	})

	root.AddCommand(newVersionCmd())
	root.AddCommand(newConfigCmd(flags))
	root.AddCommand(newMigrateCmd(flags))
	root.AddCommand(newJobsCmd(flags))
	root.AddCommand(newServeCmd(flags))
	root.AddCommand(newBackfillCmd(flags))
	root.AddCommand(newInitCmd(flags))
	root.AddCommand(newVerifyCmd(flags))
	root.AddCommand(newStatusCmd(flags))

	return root
}

// Execute runs the CLI and returns the process exit code:
// 0 success, 1 runtime error, 2 invalid config/flags, 4 migrations pending.
func Execute() int {
	root := NewRootCmd()
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "driftless: %v\n", err)
		return exitCode(err)
	}
	return 0
}
