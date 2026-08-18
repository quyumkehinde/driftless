package migrations

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgres runs a disposable postgres:17 container and returns an open
// connection to it.
func startPostgres(t *testing.T) *sql.DB {
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
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrations(t *testing.T) {
	db := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pending, err := Pending(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if pending == 0 {
		t.Fatal("fresh database should have pending migrations")
	}

	applied, err := Up(ctx, db)
	if err != nil {
		t.Fatalf("Up on fresh database: %v", err)
	}
	if len(applied) != pending {
		t.Errorf("applied %d migrations, want %d", len(applied), pending)
	}

	t.Run("all tables exist", func(t *testing.T) {
		rows, err := db.QueryContext(ctx,
			`SELECT table_name FROM information_schema.tables WHERE table_schema = 'driftless' ORDER BY table_name`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			tables = append(tables, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"backfill_runs", "backfill_tasks", "events", "gaps", "jobs",
			"meta", "object_state", "sweeps", "verifications",
		}
		if !slices.Equal(tables, want) {
			t.Errorf("tables = %v, want %v", tables, want)
		}
	})

	t.Run("events primary key", func(t *testing.T) {
		var col string
		err := db.QueryRowContext(ctx, `
			SELECT a.attname
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = 'driftless.events'::regclass AND i.indisprimary`).Scan(&col)
		if err != nil {
			t.Fatal(err)
		}
		if col != "event_id" {
			t.Errorf("events primary key = %q, want event_id", col)
		}
	})

	t.Run("jobs partial indexes", func(t *testing.T) {
		for _, idx := range []string{"jobs_live_object", "jobs_claim"} {
			var pred string
			err := db.QueryRowContext(ctx,
				`SELECT COALESCE(pg_get_expr(indpred, indrelid), '')
				 FROM pg_index WHERE indexrelid = ('driftless.' || $1)::regclass`, idx).Scan(&pred)
			if err != nil {
				t.Fatalf("index %s: %v", idx, err)
			}
			if pred == "" {
				t.Errorf("index %s should be partial", idx)
			}
		}
	})

	t.Run("meta single row guard", func(t *testing.T) {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO driftless.meta (id) VALUES (true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO driftless.meta (id) VALUES (false)`); err == nil {
			t.Error("meta must reject id = false")
		}
	})

	t.Run("version table stays in public", func(t *testing.T) {
		// The container's database user is named driftless, so once the
		// driftless schema exists, "$user",public search_path resolution
		// would move an unqualified goose_db_version into it and goose
		// would lose its history.
		rows, err := db.QueryContext(ctx,
			`SELECT table_schema FROM information_schema.tables WHERE table_name = 'goose_db_version'`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var schemas []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			schemas = append(schemas, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(schemas, []string{"public"}) {
			t.Errorf("goose_db_version in schemas %v, want only public", schemas)
		}
	})

	t.Run("up is idempotent", func(t *testing.T) {
		again, err := Up(ctx, db)
		if err != nil {
			t.Fatalf("second Up: %v", err)
		}
		if len(again) != 0 {
			t.Errorf("second Up applied %v, want nothing", again)
		}
	})

	t.Run("pending flips to zero", func(t *testing.T) {
		pending, err := Pending(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if pending != 0 {
			t.Errorf("pending = %d after Up, want 0", pending)
		}
	})

	t.Run("status reports 0001 applied", func(t *testing.T) {
		statuses, err := StatusList(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(statuses) == 0 {
			t.Fatal("no migration statuses")
		}
		first := statuses[0]
		if first.Version != 1 || first.AppliedAt.IsZero() {
			t.Errorf("first migration = %+v, want version 1 applied", first)
		}
	})
}
