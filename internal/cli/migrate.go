package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/store/migrations"
)

func newMigrateCmd(flags *rootFlags) *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Manage database schema migrations",
	}
	migrateCmd.AddCommand(newMigrateUpCmd(flags), newMigrateStatusCmd(flags))
	return migrateCmd
}

func newMigrateUpCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply pending migrations",
		Long: "Migrations are embedded in the binary and applied forward-only. Up is\n" +
			"safe to run before swapping binaries during an upgrade.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := openDB(cmd, flags)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			applied, err := migrations.Up(cmd.Context(), db)
			if err != nil {
				return fmt.Errorf("migrate up: %w", err)
			}
			if len(applied) == 0 {
				cmd.Println("no pending migrations")
				return nil
			}
			for _, name := range applied {
				cmd.Println("applied", name)
			}
			return nil
		},
	}
}

func newMigrateStatusCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show applied and pending migrations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := openDB(cmd, flags)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			statuses, err := migrations.StatusList(cmd.Context(), db)
			if err != nil {
				return fmt.Errorf("migrate status: %w", err)
			}
			for _, s := range statuses {
				state := "pending"
				if !s.AppliedAt.IsZero() {
					state = "applied " + s.AppliedAt.UTC().Format(time.RFC3339)
				}
				cmd.Printf("%04d  %-40s  %s\n", s.Version, s.Source, state)
			}
			return nil
		},
	}
}
