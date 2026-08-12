package cli

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// registers the pgx database/sql driver
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
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
		Args:  cobra.NoArgs,
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

// openDB loads and validates the config, then opens and pings the database.
func openDB(cmd *cobra.Command, flags *rootFlags) (*sql.DB, error) {
	path, explicit := config.ResolvePath(flags.configPath)
	cfg, err := config.Load(path, explicit)
	if err != nil {
		return nil, err
	}
	applyLogFlags(cfg, flags)
	warnings, err := cfg.Validate(config.ScopeDefault)
	for _, w := range warnings {
		cmd.PrintErrln("warning:", w)
	}
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return db, nil
}
