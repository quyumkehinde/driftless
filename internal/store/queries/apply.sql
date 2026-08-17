-- name: GetEventForApply :one
SELECT type, payload FROM driftless.events WHERE event_id = $1;

-- name: MarkEventsProcessedForObject :exec
-- Marks the applied event and every event it superseded through
-- coalescing: same object, not newer than the applied event. Only
-- unprocessed events are scanned, which the partial index keeps cheap.
UPDATE driftless.events SET processed_at = now()
WHERE processed_at IS NULL
  AND payload->'data'->'object'->>'id' = sqlc.arg(object_id)::text
  AND created <= sqlc.arg(created);

-- name: UpsertObjectState :exec
-- Bookkeeping after a successful apply. last_event_* only move forward so
-- a slow concurrent apply can never rewind what a newer one recorded.
INSERT INTO driftless.object_state (object_type, object_id, last_synced_at, last_event_created, last_event_id, sync_source, fetch_failures)
VALUES ($1, $2, now(), $3, $4, $5, 0)
ON CONFLICT (object_type, object_id) DO UPDATE SET
  last_synced_at = now(),
  last_event_created = GREATEST(driftless.object_state.last_event_created, EXCLUDED.last_event_created),
  last_event_id = CASE
    WHEN COALESCE(EXCLUDED.last_event_created, '-infinity'::timestamptz)
         > COALESCE(driftless.object_state.last_event_created, '-infinity'::timestamptz)
      OR (COALESCE(EXCLUDED.last_event_created, '-infinity'::timestamptz)
          = COALESCE(driftless.object_state.last_event_created, '-infinity'::timestamptz)
          AND COALESCE(EXCLUDED.last_event_id, '') > COALESCE(driftless.object_state.last_event_id, ''))
    THEN EXCLUDED.last_event_id
    ELSE driftless.object_state.last_event_id
  END,
  sync_source = EXCLUDED.sync_source,
  fetch_failures = 0;

-- name: BumpFetchFailures :execrows
UPDATE driftless.object_state SET fetch_failures = fetch_failures + 1
WHERE object_type = $1 AND object_id = $2;

-- name: GetObjectState :one
SELECT * FROM driftless.object_state WHERE object_type = $1 AND object_id = $2;
