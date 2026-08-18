-- name: InitMeta :execrows
-- Inserts the account record only if none exists; the row count reports
-- whether this call created it.
INSERT INTO driftless.meta (stripe_account_id, livemode)
VALUES ($1, $2)
ON CONFLICT (id) DO NOTHING;

-- name: ForceMeta :exec
-- Unconditionally overwrites the account record.
INSERT INTO driftless.meta (stripe_account_id, livemode)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET
  stripe_account_id = EXCLUDED.stripe_account_id,
  livemode = EXCLUDED.livemode;

-- name: GetMeta :one
SELECT stripe_account_id, livemode, initialized_at FROM driftless.meta;
