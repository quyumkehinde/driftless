-- name: InsertEvent :execrows
-- Append-only event log. A duplicate delivery affects zero rows, which is
-- how ingest distinguishes inserted from duplicate.
INSERT INTO driftless.events (event_id, type, api_version, account_id, created, source, payload, livemode)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (event_id) DO NOTHING;
