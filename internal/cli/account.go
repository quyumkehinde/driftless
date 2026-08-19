package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// ensureAccount fetches the key's account and guards the database against
// it: first contact records the account in driftless.meta, and any later
// mismatch refuses, so a prod database can never be silently corrupted by
// a test key or someone else's account. force overwrites the record.
func ensureAccount(ctx context.Context, pool *pgxpool.Pool, client *stripeapi.Client, force bool, logger *slog.Logger) error {
	raw, err := client.GetAccount(ctx, stripeapi.PriorityWebhook)
	if err != nil {
		return fmt.Errorf("stripe account check: %w", err)
	}
	var account struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &account); err != nil {
		return fmt.Errorf("stripe account response: %w", err)
	}
	livemode := stripeapi.IsLiveKey(client.Key())

	q := db.New(pool)

	if force {
		if err := q.ForceMeta(ctx, db.ForceMetaParams{
			StripeAccountID: &account.ID,
			Livemode:        &livemode,
		}); err != nil {
			return err
		}
		logger.Warn("account guard overridden", "account", account.ID, "livemode", livemode)
		return nil
	}

	meta, err := q.GetMeta(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		rows, err := q.InitMeta(ctx, db.InitMetaParams{
			StripeAccountID: &account.ID,
			Livemode:        &livemode,
		})
		if err != nil {
			return err
		}
		if rows == 1 {
			logger.Info("account recorded", "account", account.ID, "livemode", livemode)
			return nil
		}
		// lost the first-write race to a concurrent process: compare
		// against the winner's row so both agree or this one refuses
		if meta, err = q.GetMeta(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	recorded := ""
	if meta.StripeAccountID != nil {
		recorded = *meta.StripeAccountID
	}
	recordedLive := meta.Livemode != nil && *meta.Livemode
	if recorded != account.ID || recordedLive != livemode {
		return fmt.Errorf(
			"account guard: this database mirrors %s (livemode=%v) but the key is for %s (livemode=%v); pass --force-account only if this is intentional",
			recorded, recordedLive, account.ID, livemode)
	}
	return nil
}
