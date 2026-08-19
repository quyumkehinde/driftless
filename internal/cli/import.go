package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/importer"
)

func newImportCmd(flags *rootFlags) *cobra.Command {
	var (
		fromSyncEngine bool
		schema         string
	)
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Migrate rows from a stripe/sync-engine database into the mirror",
		Long: "Import copies a stripe/sync-engine schema into the mirror, one table per\n" +
			"transaction, reconstructing each object from its typed columns. Existing\n" +
			"mirror rows are never overwritten. Imported objects are marked\n" +
			"import-sourced; run verify --repair afterward to re-fetch true state for\n" +
			"every one of them.\n" +
			"\n" +
			"The sync-engine schema must be renamed away from stripe first, since the\n" +
			"mirror itself lives there:\n" +
			"  ALTER SCHEMA stripe RENAME TO sync_engine",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !fromSyncEngine {
				return &usageError{err: fmt.Errorf("pass --from-sync-engine; it is the only supported source")}
			}
			if schema == "stripe" {
				return &usageError{err: fmt.Errorf(
					"the mirror itself lives in schema \"stripe\"; restore the sync-engine dump under another name (for example sync_engine) and pass it with --schema")}
			}

			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := refuseIfMigrationsPending(cmd.Context(), cfg); err != nil {
				return err
			}
			logger, err := buildLogger(cmd, cfg)
			if err != nil {
				return err
			}

			result, err := importer.New(pool, logger).Run(cmd.Context(), schema)
			if err != nil {
				return err
			}
			cmd.Printf("imported %d row(s) across %d table(s); %d already present and left untouched\n",
				result.Imported, result.Tables, result.Skipped)
			cmd.Println("imported data is reconstructed from typed columns; run 'driftless verify --repair' to re-fetch and true up every object")
			return nil
		},
	}
	cmd.Flags().BoolVar(&fromSyncEngine, "from-sync-engine", false, "import from a stripe/sync-engine schema layout")
	cmd.Flags().StringVar(&schema, "schema", "sync_engine", "schema holding the sync-engine tables")
	return cmd
}
