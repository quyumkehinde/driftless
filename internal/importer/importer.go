// Package importer migrates a stripe/sync-engine database into the
// mirror: rows are copied table by table, their typed columns folded back
// into minimal JSONB, and every imported object is marked as import-sourced
// so a follow-up verify --repair can re-fetch true state. Import trades
// fidelity for zero-downtime migration; verify provides the fidelity.
package importer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// tableOrder lists source tables in dependency-friendly order. Names match
// sync-engine's layout, which mirrors ours by design.
var tableOrder = []struct {
	table      string
	objectType stripeapi.ObjectType
}{
	{"products", stripeapi.ObjectProduct},
	{"prices", stripeapi.ObjectPrice},
	{"customers", stripeapi.ObjectCustomer},
	{"subscriptions", stripeapi.ObjectSubscription},
	{"subscription_items", stripeapi.ObjectSubscriptionItem},
	{"payment_methods", stripeapi.ObjectPaymentMethod},
	{"invoices", stripeapi.ObjectInvoice},
	{"charges", stripeapi.ObjectCharge},
	{"payment_intents", stripeapi.ObjectPaymentIntent},
	{"setup_intents", stripeapi.ObjectSetupIntent},
	{"refunds", stripeapi.ObjectRefund},
	{"disputes", stripeapi.ObjectDispute},
	{"checkout_sessions", stripeapi.ObjectCheckoutSession},
}

// bookkeepingColumns are sync-engine columns that describe their pipeline,
// not the Stripe object; they are stripped from the reconstructed JSONB.
var bookkeepingColumns = []string{"updated_at", "last_synced_at"}

// epochFields are Stripe fields our generated columns parse as epoch
// integers. A source storing them as SQL timestamps gets them folded back
// to epochs during reconstruction.
var epochFields = []string{
	"created", "canceled_at", "current_period_start", "current_period_end",
	"period_start", "period_end", "trial_end",
}

// Result sums one import run.
type Result struct {
	Tables   int
	Imported int64
	Skipped  int64 // rows whose id already existed in the mirror
}

// Importer copies a sync-engine schema into the mirror.
type Importer struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New wires an importer.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Importer {
	return &Importer{pool: pool, logger: obs.WithComponent(logger, "import")}
}

// Detect reports which importable tables the source schema holds. An empty
// result means the schema does not look like sync-engine at all.
func (i *Importer) Detect(ctx context.Context, sourceSchema string) ([]string, error) {
	rows, err := i.pool.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1`, sourceSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	present := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var found []string
	for _, entry := range tableOrder {
		if present[entry.table] {
			found = append(found, entry.table)
		}
	}
	return found, nil
}

// Run copies every detected table, one transaction per table so a failure
// keeps whole tables rather than leaving one half-copied.
func (i *Importer) Run(ctx context.Context, sourceSchema string) (Result, error) {
	if err := validateSchemaName(sourceSchema); err != nil {
		return Result{}, err
	}
	found, err := i.Detect(ctx, sourceSchema)
	if err != nil {
		return Result{}, err
	}
	if len(found) == 0 {
		return Result{}, fmt.Errorf("schema %q has no sync-engine tables", sourceSchema)
	}

	var result Result
	for _, entry := range tableOrder {
		if !contains(found, entry.table) {
			continue
		}
		imported, skipped, err := i.importTable(ctx, sourceSchema, entry.table, entry.objectType)
		if err != nil {
			return result, fmt.Errorf("import %s: %w", entry.table, err)
		}
		result.Tables++
		result.Imported += imported
		result.Skipped += skipped
		i.logger.Info("table imported", "table", entry.table, "rows", imported, "skipped", skipped)
	}
	return result, nil
}

// importTable copies one table inside one transaction. The source row is
// folded to JSONB with nulls and bookkeeping stripped: the minimal honest
// reconstruction of the object from typed columns. Existing mirror rows
// are never overwritten; whatever the mirror already knows is fresher than
// a migration copy.
func (i *Importer) importTable(ctx context.Context, sourceSchema, table string, objectType stripeapi.ObjectType) (imported, skipped int64, err error) {
	target, ok := mirror.Table(objectType)
	if !ok {
		return 0, 0, fmt.Errorf("no mirror table for %s", objectType)
	}
	err = pgx.BeginFunc(ctx, i.pool, func(tx pgx.Tx) error {
		var total int64
		if err := tx.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s.%s`, sourceSchema, table)).Scan(&total); err != nil {
			return err
		}

		expr, hasDeleted, err := reconstructionExpr(ctx, tx, sourceSchema, table, objectType)
		if err != nil {
			return err
		}
		// sync-engine marks deletions with a column on some tables; those
		// rows arrive already soft-deleted instead of masquerading as live
		deletedExpr := "false, NULL::timestamptz"
		if hasDeleted {
			deletedExpr = "coalesce(t.deleted, false), CASE WHEN coalesce(t.deleted, false) THEN now() END"
		}
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, data, is_deleted, deleted_at)
			SELECT t.id, %s, %s
			FROM %s.%s t
			ON CONFLICT (id) DO NOTHING`, target, expr, deletedExpr, sourceSchema, table))
		if err != nil {
			return err
		}
		imported = tag.RowsAffected()
		skipped = total - imported

		// every imported object is marked import-sourced so verify knows
		// its data is reconstructed, not fetched
		_, err = tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO driftless.object_state (object_type, object_id, last_synced_at, sync_source, fetch_failures)
			SELECT $1, t.id, now(), 'import', 0
			FROM %s.%s t
			ON CONFLICT (object_type, object_id) DO NOTHING`, sourceSchema, table), objectType)
		return err
	})
	return imported, skipped, err
}

// reconstructionExpr builds the SQL expression that folds one source row
// back into a minimal Stripe object: typed columns to JSONB, nulls and
// pipeline bookkeeping stripped, timestamp-typed epoch fields converted
// back to the integers Stripe speaks, and the object field stamped. It
// also reports whether the table carries sync-engine's deleted marker.
func reconstructionExpr(ctx context.Context, tx pgx.Tx, sourceSchema, table string, objectType stripeapi.ObjectType) (expr string, hasDeleted bool, err error) {
	rows, err := tx.Query(ctx, `
		SELECT column_name, data_type FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2`, sourceSchema, table)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	types := make(map[string]string)
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return "", false, err
		}
		types[name] = dataType
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}

	expr = "to_jsonb(t)"
	for _, col := range bookkeepingColumns {
		expr += fmt.Sprintf(" - '%s'", col)
	}
	expr = "jsonb_strip_nulls(" + expr + ")"
	for _, field := range epochFields {
		if strings.HasPrefix(types[field], "timestamp") || types[field] == "date" {
			expr = fmt.Sprintf(
				"jsonb_set(%s, '{%s}', coalesce(to_jsonb(extract(epoch from t.%s)::bigint), 'null'::jsonb), true)",
				expr, field, field)
		}
	}
	expr = fmt.Sprintf("jsonb_set(%s, '{object}', to_jsonb('%s'::text), true)", expr, objectType)
	return expr, types["deleted"] == "boolean", nil
}

// validateSchemaName rejects anything that cannot be a plain identifier,
// since schema names are embedded in SQL text.
func validateSchemaName(schema string) error {
	if schema == "" {
		return fmt.Errorf("source schema name is empty")
	}
	for _, r := range schema {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return fmt.Errorf("source schema %q: only lowercase letters, digits, and underscores are supported", schema)
		}
	}
	if strings.HasPrefix(schema, "pg_") {
		return fmt.Errorf("source schema %q is reserved", schema)
	}
	return nil
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
