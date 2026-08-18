-- name: InsertEvent :execrows
-- Append-only event log. A duplicate delivery affects zero rows, which is
-- how ingest distinguishes inserted from duplicate.
INSERT INTO driftless.events (event_id, type, api_version, account_id, created, source, payload, livemode)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (event_id) DO NOTHING;

-- name: GetEventByID :one
SELECT * FROM driftless.events WHERE event_id = $1;

-- name: CountEventsBySource :one
SELECT
  count(*) FILTER (WHERE source = 'webhook') AS webhook,
  count(*) FILTER (WHERE source = 'sweep') AS sweep
FROM driftless.events;

-- name: GetMeta :one
SELECT stripe_account_id, livemode, initialized_at FROM driftless.meta;
