-- name: CreateSweep :one
INSERT INTO driftless.sweeps (window_from, window_to)
VALUES ($1, $2)
RETURNING *;

-- name: FinishSweep :exec
UPDATE driftless.sweeps
SET status = $2, finished_at = now(), events_seen = $3, gaps_found = $4
WHERE id = $1;

-- name: GetLastSuccessfulSweep :one
-- The checkpoint: the next window starts overlap before this window_to.
SELECT * FROM driftless.sweeps WHERE status = 'done' ORDER BY window_to DESC, id DESC LIMIT 1;

-- name: InsertGap :exec
INSERT INTO driftless.gaps (event_id, sweep_id, event_created, lag)
VALUES ($1, $2, $3, now() - $3::timestamptz);

-- name: CountWebhookEventsBetween :one
-- Zero webhook arrivals in a window with gaps found means deliveries are
-- not reaching this endpoint at all.
SELECT count(*) FROM driftless.events
WHERE source = 'webhook' AND received_at BETWEEN $1 AND $2;
