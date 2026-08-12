package cli

import (
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/quyumkehinde/driftless/internal/config"
)

func newConfigCmd(flags *rootFlags) *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect configuration",
	}
	configCmd.AddCommand(newConfigPrintCmd(flags))
	return configCmd
}

func newConfigPrintCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "print",
		Short: "Print the effective configuration with secrets redacted",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, explicit := config.ResolvePath(flags.configPath)
			cfg, err := config.Load(path, explicit)
			if err != nil {
				return err
			}
			applyLogFlags(cfg, flags)
			warnings, verr := cfg.Validate(config.ScopeDefault)
			for _, w := range warnings {
				cmd.PrintErrln("warning:", w)
			}
			out, err := yaml.Marshal(cfg.Redacted())
			if err != nil {
				return err
			}
			cmd.Print(string(out))
			// The redacted config is printed even when invalid, so the
			// operator can see what the process would have used.
			return verr
		},
	}
}

// applyLogFlags lets the global flags override the configured log settings.
func applyLogFlags(cfg *config.Config, flags *rootFlags) {
	if flags.logLevel != "" {
		cfg.Log.Level = flags.logLevel
	}
	if flags.logFormat != "" {
		cfg.Log.Format = flags.logFormat
	}
}
