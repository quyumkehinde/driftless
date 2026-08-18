package importer

import (
	_ "embed"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/testpg"
)

//go:embed testdata/sync_engine_fixture.sql
var fixture string

func startImport(t *testing.T) (*Importer, *pgxpool.Pool) {
	t.Helper()
	pool := testpg.Start(t)
	if _, err := pool.Exec(t.Context(), fixture); err != nil {
		t.Fatalf("apply fixture: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(pool, logger), pool
}

func TestImportReconstructsObjects(t *testing.T) {
	imp, pool := startImport(t)

	result, err := imp.Run(t.Context(), "sync_engine")
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 3 || result.Imported != 6 || result.Skipped != 0 {
		t.Errorf("result = %+v, want 3 tables, 6 imported, 0 skipped", result)
	}

	var raw []byte
	if err := pool.QueryRow(t.Context(),
		`SELECT data FROM stripe.customers WHERE id = 'cus_imp1'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	if data["email"] != "one@x.y" || data["object"] != "customer" {
		t.Errorf("reconstructed customer = %v", data)
	}
	if data["created"] != float64(1700000001) {
		t.Errorf("created = %v, want the source epoch", data["created"])
	}
	if _, ok := data["updated_at"]; ok {
		t.Error("pipeline bookkeeping must be stripped from reconstructed data")
	}
	if meta, ok := data["metadata"].(map[string]any); !ok || meta["tier"] != "gold" {
		t.Errorf("metadata = %v, want the source jsonb", data["metadata"])
	}

	// null typed columns disappear instead of becoming explicit nulls
	if err := pool.QueryRow(t.Context(),
		`SELECT data FROM stripe.customers WHERE id = 'cus_imp2'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	data = nil
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	if _, ok := data["email"]; ok {
		t.Error("null source columns must be stripped, not imported as JSON null")
	}

	// sync-engine's deleted marker arrives as a soft delete, never as a
	// live row that verify would fight over
	var isDeleted bool
	var deletedAt *time.Time
	if err := pool.QueryRow(t.Context(),
		`SELECT is_deleted, deleted_at FROM stripe.customers WHERE id = 'cus_gone'`).Scan(&isDeleted, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if !isDeleted || deletedAt == nil {
		t.Errorf("deleted source row: is_deleted=%v deleted_at=%v, want soft-deleted", isDeleted, deletedAt)
	}
}

func TestImportFoldsTimestampsToEpochs(t *testing.T) {
	imp, pool := startImport(t)
	if _, err := imp.Run(t.Context(), "sync_engine"); err != nil {
		t.Fatal(err)
	}

	// the generated columns parse only if reconstruction produced epochs
	var currentPeriodEnd time.Time
	var created time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT current_period_end, created FROM stripe.subscriptions
		WHERE id = 'sub_imp'`).Scan(&currentPeriodEnd, &created); err != nil {
		t.Fatal(err)
	}
	if currentPeriodEnd.Unix() != 1702592100 {
		t.Errorf("current_period_end = %v, want epoch 1702592100", currentPeriodEnd)
	}
	if created.Unix() != 1700000004 {
		t.Errorf("created = %v, want epoch 1700000004", created)
	}
}

func TestImportNeverOverwritesMirrorRows(t *testing.T) {
	imp, pool := startImport(t)

	// the mirror already knows this customer with fresher data
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO stripe.customers (id, data)
		VALUES ('cus_existing', '{"id": "cus_existing", "object": "customer", "email": "fresh@x.y"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.object_state (object_type, object_id, last_synced_at, sync_source, fetch_failures)
		VALUES ('customer', 'cus_existing', now(), 'fetch', 0)`); err != nil {
		t.Fatal(err)
	}

	result, err := imp.Run(t.Context(), "sync_engine")
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped != 1 {
		t.Errorf("skipped = %d, want 1", result.Skipped)
	}

	var email, syncSource string
	if err := pool.QueryRow(t.Context(),
		`SELECT data->>'email' FROM stripe.customers WHERE id = 'cus_existing'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "fresh@x.y" {
		t.Errorf("email = %q: import must never overwrite mirror rows", email)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT sync_source FROM driftless.object_state
		WHERE object_type = 'customer' AND object_id = 'cus_existing'`).Scan(&syncSource); err != nil {
		t.Fatal(err)
	}
	if syncSource != "fetch" {
		t.Errorf("sync_source = %q: import must not relabel existing state", syncSource)
	}

	// imported rows are labeled import-sourced
	if err := pool.QueryRow(t.Context(), `
		SELECT sync_source FROM driftless.object_state
		WHERE object_type = 'customer' AND object_id = 'cus_imp1'`).Scan(&syncSource); err != nil {
		t.Fatal(err)
	}
	if syncSource != "import" {
		t.Errorf("sync_source = %q, want import", syncSource)
	}
}

func TestImportRejectsBadSchemas(t *testing.T) {
	imp, _ := startImport(t)
	for _, schema := range []string{"", "Bad-Name", "pg_catalog", "a;drop"} {
		if _, err := imp.Run(t.Context(), schema); err == nil {
			t.Errorf("schema %q must be rejected", schema)
		}
	}
	if _, err := imp.Run(t.Context(), "does_not_exist"); err == nil {
		t.Error("a schema with no sync-engine tables must be an error")
	}
}
