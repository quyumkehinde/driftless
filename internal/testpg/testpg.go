// Package testpg starts disposable migrated Postgres containers for
// integration tests. It is imported only from test files.
package testpg

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	// registers the pgx database/sql driver that the migration runner needs
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/quyumkehinde/driftless/internal/store/migrations"
)

// Start runs a postgres:17 container, applies all migrations, and returns a
// connection pool. Everything is cleaned up with the test. Tests that use
// containers must skip under -short.
func Start(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in short mode")
	}
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("driftless"),
		tcpostgres.WithUsername("driftless"),
		tcpostgres.WithPassword("driftless"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	mdb, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.Up(ctx, mdb); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := mdb.Close(); err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
