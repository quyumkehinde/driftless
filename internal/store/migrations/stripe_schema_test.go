package migrations

import (
	"context"
	"slices"
	"testing"
	"time"
)

// stripeTables is every v1 mirror table in migration order.
var stripeTables = []string{
	"customers", "products", "prices", "subscriptions", "subscription_items",
	"invoices", "charges", "payment_intents", "payment_methods",
	"setup_intents", "refunds", "disputes", "checkout_sessions",
}

func TestStripeSchema(t *testing.T) {
	db := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := Up(ctx, db); err != nil {
		t.Fatalf("Up: %v", err)
	}

	t.Run("all thirteen tables exist", func(t *testing.T) {
		rows, err := db.QueryContext(ctx,
			`SELECT table_name FROM information_schema.tables WHERE table_schema = 'stripe' ORDER BY table_name`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var got []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			got = append(got, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := slices.Clone(stripeTables)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("tables = %v, want %v", got, want)
		}
	})

	t.Run("every table has the bookkeeping columns", func(t *testing.T) {
		for _, table := range stripeTables {
			for _, col := range []string{"id", "data", "created", "livemode", "account_id", "is_deleted", "deleted_at", "updated_at"} {
				var exists bool
				err := db.QueryRowContext(ctx,
					`SELECT EXISTS (SELECT 1 FROM information_schema.columns
					 WHERE table_schema = 'stripe' AND table_name = $1 AND column_name = $2)`,
					table, col).Scan(&exists)
				if err != nil {
					t.Fatal(err)
				}
				if !exists {
					t.Errorf("stripe.%s missing column %s", table, col)
				}
			}
		}
	})

	t.Run("money columns are bigint", func(t *testing.T) {
		for _, mc := range []struct{ table, column string }{
			{"prices", "unit_amount"},
			{"invoices", "total"},
			{"invoices", "amount_paid"},
			{"invoices", "amount_due"},
			{"charges", "amount"},
			{"charges", "amount_refunded"},
			{"payment_intents", "amount"},
			{"refunds", "amount"},
			{"disputes", "amount"},
			{"checkout_sessions", "amount_total"},
		} {
			var dataType string
			err := db.QueryRowContext(ctx,
				`SELECT data_type FROM information_schema.columns
				 WHERE table_schema = 'stripe' AND table_name = $1 AND column_name = $2`,
				mc.table, mc.column).Scan(&dataType)
			if err != nil {
				t.Fatalf("%s.%s: %v", mc.table, mc.column, err)
			}
			if dataType != "bigint" {
				t.Errorf("stripe.%s.%s is %s, money must be bigint minor units", mc.table, mc.column, dataType)
			}
		}
	})

	t.Run("generated columns compute from data", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO stripe.customers (id, data) VALUES
			('cus_gen', '{"id":"cus_gen","email":"gen@x.y","name":"Gen","created":1735689600,"currency":"usd","delinquent":false,"livemode":true}')`)
		if err != nil {
			t.Fatal(err)
		}
		var email, name, currency string
		var created time.Time
		var delinquent, livemode, isDeleted bool
		err = db.QueryRowContext(ctx, `
			SELECT email, name, currency, created, delinquent, livemode, is_deleted
			FROM stripe.customers WHERE id = 'cus_gen'`).
			Scan(&email, &name, &currency, &created, &delinquent, &livemode, &isDeleted)
		if err != nil {
			t.Fatal(err)
		}
		if email != "gen@x.y" || name != "Gen" || currency != "usd" || delinquent || !livemode || isDeleted {
			t.Errorf("generated columns wrong: email=%q name=%q currency=%q delinquent=%v livemode=%v is_deleted=%v",
				email, name, currency, delinquent, livemode, isDeleted)
		}
		if created.UTC() != time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) {
			t.Errorf("created = %v, want 2025-01-01T00:00:00Z", created.UTC())
		}

		// nested extractions and money
		_, err = db.ExecContext(ctx, `
			INSERT INTO stripe.prices (id, data) VALUES
			('price_gen', '{"id":"price_gen","product":"prod_1","active":true,"currency":"usd","unit_amount":9900,"recurring":{"interval":"month"},"type":"recurring","created":1735689600,"livemode":false}')`)
		if err != nil {
			t.Fatal(err)
		}
		var unitAmount int64
		var interval string
		err = db.QueryRowContext(ctx,
			`SELECT unit_amount, recurring_interval FROM stripe.prices WHERE id = 'price_gen'`).
			Scan(&unitAmount, &interval)
		if err != nil {
			t.Fatal(err)
		}
		if unitAmount != 9900 || interval != "month" {
			t.Errorf("unit_amount=%d recurring_interval=%q, want 9900/month", unitAmount, interval)
		}

		// absent optional epoch fields stay NULL rather than erroring
		_, err = db.ExecContext(ctx, `
			INSERT INTO stripe.subscriptions (id, data) VALUES
			('sub_gen', '{"id":"sub_gen","customer":"cus_gen","status":"active","created":1735689600,"livemode":false}')`)
		if err != nil {
			t.Fatal(err)
		}
		var canceledAt *time.Time
		err = db.QueryRowContext(ctx,
			`SELECT canceled_at FROM stripe.subscriptions WHERE id = 'sub_gen'`).Scan(&canceledAt)
		if err != nil {
			t.Fatal(err)
		}
		if canceledAt != nil {
			t.Errorf("canceled_at = %v, want NULL for a live subscription", canceledAt)
		}
	})

	t.Run("soft delete keeps data", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			UPDATE stripe.customers SET is_deleted = true, deleted_at = now() WHERE id = 'cus_gen'`)
		if err != nil {
			t.Fatal(err)
		}
		var email string
		var isDeleted bool
		err = db.QueryRowContext(ctx,
			`SELECT email, is_deleted FROM stripe.customers WHERE id = 'cus_gen'`).Scan(&email, &isDeleted)
		if err != nil {
			t.Fatal(err)
		}
		if !isDeleted || email != "gen@x.y" {
			t.Errorf("soft delete must keep last-known data: is_deleted=%v email=%q", isDeleted, email)
		}
	})

	t.Run("indexes are partial on not deleted", func(t *testing.T) {
		var pred string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(pg_get_expr(i.indpred, i.indrelid), '')
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'stripe' AND c.relname LIKE 'customers_email%'`).Scan(&pred)
		if err != nil {
			t.Fatal(err)
		}
		if pred == "" {
			t.Error("customers email index should be partial on NOT is_deleted")
		}
	})
}
