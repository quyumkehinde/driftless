package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// upsertSubscription writes the subscription row and explodes its items
// into stripe.subscription_items in the same transaction: items absent
// from the fetched set are soft-deleted, the rest upserted. The embedded
// items list is paginated at ten, so has_more pages through the collection
// before anything is written; truncating here is the entitlement-loss bug
// class.
func (e *Engine) upsertSubscription(ctx context.Context, tx pgx.Tx, subID string, raw []byte) error {
	var envelope struct {
		Items struct {
			Data    []json.RawMessage `json:"data"`
			HasMore bool              `json:"has_more"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("apply: parse subscription %s: %w", subID, err)
	}

	items := envelope.Items.Data
	if envelope.Items.HasMore {
		full, err := e.listAllSubscriptionItems(ctx, subID)
		if err != nil {
			return err
		}
		items = full
	}

	if err := upsertObject(ctx, tx, stripeapi.ObjectSubscription, subID, raw); err != nil {
		return err
	}

	keep := make([]string, 0, len(items))
	for _, item := range items {
		var itemID struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(item, &itemID); err != nil || itemID.ID == "" {
			return fmt.Errorf("apply: subscription %s item has no id", subID)
		}
		if err := upsertObject(ctx, tx, stripeapi.ObjectSubscriptionItem, itemID.ID, item); err != nil {
			return err
		}
		keep = append(keep, itemID.ID)
	}

	_, err := tx.Exec(ctx, `
		UPDATE stripe.subscription_items
		SET is_deleted = true, deleted_at = now(), updated_at = now()
		WHERE subscription = $1 AND NOT is_deleted AND NOT (id = ANY($2))`,
		subID, keep)
	if err != nil {
		return fmt.Errorf("apply: prune subscription items for %s: %w", subID, err)
	}
	return nil
}

// listAllSubscriptionItems pages through the item collection.
func (e *Engine) listAllSubscriptionItems(ctx context.Context, subID string) ([]json.RawMessage, error) {
	var items []json.RawMessage
	query := url.Values{"subscription": {subID}, "limit": {"100"}}
	for {
		page, err := e.client.List(ctx, stripeapi.PriorityWebhook, "/v1/subscription_items", query)
		if err != nil {
			return nil, err
		}
		items = append(items, page.Data...)
		if !page.HasMore || len(page.Data) == 0 {
			return items, nil
		}
		var lastID struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(page.Data[len(page.Data)-1], &lastID); err != nil {
			return nil, err
		}
		query.Set("starting_after", lastID.ID)
	}
}
