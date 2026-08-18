// Package testpg provides disposable migrated Postgres databases for
// integration tests. It is imported only from test files.
package testpg

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	// registers the pgx database/sql driver that the migration runner needs
	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/quyumkehinde/driftless/internal/store/migrations"
)

// templateDB is migrated once per process; every test database is cloned
// from it, which is orders of magnitude cheaper than a container per test.
const templateDB = "driftless_template"

var (
	setupOnce sync.Once
	setupErr  error
	baseURL   *url.URL // connection URL for the container's postgres database
	dbSeq     atomic.Int64
	createMu  sync.Mutex
)

// Start returns a pool to a fresh migrated database. Everything is cleaned
// up with the test; the container itself is shared by the whole package run
// and reaped by testcontainers when the process exits. Tests that use
// containers must skip under -short.
func Start(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := StartWithURL(t)
	return pool
}

// StartWithURL is Start plus the connection string, for tests that hand the
// database to a subprocess or the CLI.
func StartWithURL(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in short mode")
	}
	ctx := context.Background()

	setupOnce.Do(func() { setupErr = startContainer(ctx) })
	if setupErr != nil {
		t.Fatalf("shared postgres container: %v", setupErr)
	}

	name := fmt.Sprintf("driftless_test_%d", dbSeq.Add(1))
	if err := cloneTemplate(ctx, name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() { dropDatabase(t, name) })

	connStr := databaseURL(name)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, connStr
}

// startContainer runs the one postgres:17 container and migrates the
// template database inside it.
func startContainer(ctx context.Context) error {
	ctr, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase(templateDB),
		tcpostgres.WithUsername("driftless"),
		tcpostgres.WithPassword("driftless"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return err
	}
	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	if baseURL, err = url.Parse(connStr); err != nil {
		return err
	}

	mdb, err := sql.Open("pgx", databaseURL(templateDB))
	if err != nil {
		return err
	}
	if _, err := migrations.Up(ctx, mdb); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return mdb.Close()
}

// cloneTemplate copies the migrated template into a new database. Creates
// are serialized: Postgres refuses to copy a template that another create
// is reading.
func cloneTemplate(ctx context.Context, name string) error {
	createMu.Lock()
	defer createMu.Unlock()
	return withAdmin(func(admin *sql.DB) error {
		_, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, templateDB))
		return err
	})
}

// dropDatabase removes a test's database, kicking out any connection a
// killed subprocess left behind.
func dropDatabase(t *testing.T, name string) {
	err := withAdmin(func(admin *sql.DB) error {
		_, err := admin.ExecContext(context.Background(),
			fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", name))
		return err
	})
	if err != nil {
		t.Logf("drop database %s: %v", name, err)
	}
}

// withAdmin runs f on a short-lived connection to the maintenance database.
func withAdmin(f func(*sql.DB) error) error {
	admin, err := sql.Open("pgx", databaseURL("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	return f(admin)
}

// databaseURL rewrites the container's connection URL to point at name.
func databaseURL(name string) string {
	u := *baseURL
	u.Path = "/" + name
	return u.String()
}
