// Package migrations embeds the SQL schema migrations and runs them with
// goose. Migrations are forward-only: there are no Down sections and old
// migrations are never rewritten.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

//go:embed *.sql
var embedded embed.FS

// VersionTable is where goose records applied versions. It is explicitly
// schema-qualified; see newProvider for why the bare name is a trap.
const VersionTable = "public.goose_db_version"

func newProvider(db *sql.DB) (*goose.Provider, error) {
	// The version table name must be schema-qualified: with the default
	// search_path of "$user",public, a database user named driftless would
	// resolve the bare name into the driftless schema once migration 0001
	// creates it, and goose would lose its history.
	store, err := database.NewStore(database.DialectPostgres, VersionTable)
	if err != nil {
		return nil, err
	}
	return goose.NewProvider("", db, embedded, goose.WithStore(store))
}

// Up applies all pending migrations and returns the names of those applied.
func Up(ctx context.Context, db *sql.DB) ([]string, error) {
	p, err := newProvider(db)
	if err != nil {
		return nil, err
	}
	results, err := p.Up(ctx)
	if err != nil {
		return nil, err
	}
	applied := make([]string, 0, len(results))
	for _, r := range results {
		applied = append(applied, r.Source.Path)
	}
	return applied, nil
}

// Status describes one migration: its file and when it was applied
// (zero time when still pending).
type Status struct {
	Version   int64
	Source    string
	AppliedAt time.Time
}

// StatusList reports every known migration in version order.
func StatusList(ctx context.Context, db *sql.DB) ([]Status, error) {
	p, err := newProvider(db)
	if err != nil {
		return nil, err
	}
	statuses, err := p.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Status, 0, len(statuses))
	for _, s := range statuses {
		st := Status{
			Version: s.Source.Version,
			Source:  s.Source.Path,
		}
		if s.State == goose.StateApplied {
			st.AppliedAt = s.AppliedAt
		}
		out = append(out, st)
	}
	return out, nil
}

// LatestVersion returns the highest embedded migration version, so a
// readiness check can compare it against the version table without goose.
func LatestVersion() (int64, error) {
	entries, err := embedded.ReadDir(".")
	if err != nil {
		return 0, err
	}
	var latest int64
	for _, entry := range entries {
		name := entry.Name()
		prefix, _, found := strings.Cut(name, "_")
		if !found {
			continue
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("migration file %s has no numeric version", name)
		}
		latest = max(latest, version)
	}
	return latest, nil
}

// Pending returns how many migrations have not been applied yet.
func Pending(ctx context.Context, db *sql.DB) (int, error) {
	statuses, err := StatusList(ctx, db)
	if err != nil {
		return 0, err
	}
	pending := 0
	for _, s := range statuses {
		if s.AppliedAt.IsZero() {
			pending++
		}
	}
	return pending, nil
}
