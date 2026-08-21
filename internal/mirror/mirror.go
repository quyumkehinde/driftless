// Package mirror owns every write to the stripe mirror schema: the shared
// upsert, soft delete, and change notification used by apply, backfill,
// and repair, so all writers stay on one code path.
package mirror

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// SyncSource is the object_state.sync_source value; fetch and payload
// double as the apply-mode metric label. Repair arrives with verify,
// import with the sync-engine importer.
type SyncSource string

const (
	SyncSourceFetch    SyncSource = "fetch"
	SyncSourcePayload  SyncSource = "payload"
	SyncSourceBackfill SyncSource = "backfill"
	SyncSourceRepair   SyncSource = "repair"
	SyncSourceImport   SyncSource = "import"
)

// EventSource is the events.source value, matching the schema's CHECK
// constraint. The two writers, ingest and the sweeper, must agree with
// the schema here.
type EventSource string

const (
	EventSourceWebhook EventSource = "webhook"
	EventSourceSweep   EventSource = "sweep"
)

// tables whitelists the object types the mirror schema stores. Table names
// are interpolated into SQL, which is safe only because they come from
// this map and never from input; sqlc cannot parameterize identifiers, and
// one dynamic statement keeps every writer on a single upsert code path.
var tables = map[stripeapi.ObjectType]string{
	stripeapi.ObjectCustomer:         "stripe.customers",
	stripeapi.ObjectSubscription:     "stripe.subscriptions",
	stripeapi.ObjectSubscriptionItem: "stripe.subscription_items",
	stripeapi.ObjectProduct:          "stripe.products",
	stripeapi.ObjectPrice:            "stripe.prices",
	stripeapi.ObjectInvoice:          "stripe.invoices",
	stripeapi.ObjectCharge:           "stripe.charges",
	stripeapi.ObjectPaymentIntent:    "stripe.payment_intents",
	stripeapi.ObjectPaymentMethod:    "stripe.payment_methods",
	stripeapi.ObjectSetupIntent:      "stripe.setup_intents",
	stripeapi.ObjectRefund:           "stripe.refunds",
	stripeapi.ObjectDispute:          "stripe.disputes",
	stripeapi.ObjectCheckoutSession:  "stripe.checkout_sessions",
}

// Table returns the mirror table for an object type. Callers embedding it
// in SQL must take the name from here and never from input.
func Table(objectType stripeapi.ObjectType) (string, bool) {
	table, ok := tables[objectType]
	return table, ok
}

// LockObject takes the per-object advisory lock inside tx, serializing
// every writer of one object across all processes. Apply and backfill
// must build the identical key or their mutual exclusion silently stops
// working, which is why the key lives here and nowhere else.
func LockObject(ctx context.Context, tx pgx.Tx, objectType stripeapi.ObjectType, id string) error {
	_, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(objectType)+":"+id)
	return err
}

// UpsertObject writes a freshly fetched object. Last-writer-wins is safe
// because every writer holds the per-object advisory lock and writes a
// fresh fetch; a resurrected id also clears the soft-delete flags.
func UpsertObject(ctx context.Context, tx pgx.Tx, objectType stripeapi.ObjectType, id string, data []byte) error {
	table, ok := tables[objectType]
	if !ok {
		return fmt.Errorf("mirror: no table for object type %q", objectType)
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, data, is_deleted, deleted_at, updated_at)
		VALUES ($1, $2, false, NULL, now())
		ON CONFLICT (id) DO UPDATE
		SET data = EXCLUDED.data,
		    is_deleted = false,
		    deleted_at = NULL,
		    updated_at = now()`, table), id, data)
	if err != nil {
		return fmt.Errorf("mirror: upsert %s %s: %w", objectType, id, err)
	}
	return nil
}

// SoftDeleteObject flags an object deleted, keeping the last-known data
// for auditability. Deleting an id that was never mirrored is a no-op.
func SoftDeleteObject(ctx context.Context, tx pgx.Tx, objectType stripeapi.ObjectType, id string) error {
	table, ok := tables[objectType]
	if !ok {
		return fmt.Errorf("mirror: no table for object type %q", objectType)
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE %s SET is_deleted = true, deleted_at = now(), updated_at = now()
		WHERE id = $1 AND NOT is_deleted`, table), id)
	if err != nil {
		return fmt.Errorf("mirror: soft delete %s %s: %w", objectType, id, err)
	}
	// a deleted subscription's items are gone with it; leaving them live
	// is the entitlement-phantom mirror of the truncation bug class
	if objectType == stripeapi.ObjectSubscription {
		if _, err := tx.Exec(ctx, `
			UPDATE stripe.subscription_items
			SET is_deleted = true, deleted_at = now(), updated_at = now()
			WHERE subscription = $1 AND NOT is_deleted`, id); err != nil {
			return fmt.Errorf("mirror: soft delete items of %s: %w", id, err)
		}
	}
	return nil
}

// NotifyChange pokes listeners after a successful write; delivery happens
// on commit. The payload is deliberately minimal: type and id only.
func NotifyChange(ctx context.Context, tx pgx.Tx, objectType stripeapi.ObjectType, id string) error {
	payload, err := json.Marshal(map[string]string{"type": string(objectType), "id": id})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify('driftless_changes', $1)`, string(payload))
	return err
}
