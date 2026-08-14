// Connection plumbing shared by every command that touches the database:
// load and validate config, open a connection, ping, hand it down. Domain
// packages receive connections; only this layer creates them.

package cli

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	// registers the pgx database/sql driver for the migration runner
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/queue"
)

const connectTimeout = 10 * time.Second

// loadConfig resolves, loads, and validates the effective config, printing
// any warnings to the command's stderr.
func loadConfig(cmd *cobra.Command, flags *rootFlags, scope config.Scope) (*config.Config, error) {
	path, explicit := config.ResolvePath(flags.configPath)
	cfg, err := config.Load(path, explicit)
	if err != nil {
		return nil, err
	}
	applyLogFlags(cfg, flags)
	warnings, err := cfg.Validate(scope)
	for _, w := range warnings {
		cmd.PrintErrln("warning:", w)
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// openDB opens a database/sql connection, for goose which requires one.
func openDB(cmd *cobra.Command, flags *rootFlags) (*sql.DB, error) {
	cfg, err := loadConfig(cmd, flags, config.ScopeDefault)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), connectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return db, nil
}

// openPool opens the pgx pool the domain packages run on.
func openPool(cmd *cobra.Command, flags *rootFlags, scope config.Scope) (*config.Config, *pgxpool.Pool, error) {
	cfg, err := loadConfig(cmd, flags, scope)
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.New(cmd.Context(), cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), connectTimeout)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("connect to database: %w", err)
	}
	return cfg, pool, nil
}

// openQueue is openPool plus the queue wrapper most job commands want.
func openQueue(cmd *cobra.Command, flags *rootFlags) (*pgxpool.Pool, *queue.Queue, error) {
	cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
	if err != nil {
		return nil, nil, err
	}
	return pool, queue.New(pool, cfg.Workers.VisibilityTimeout.Std()), nil
}
