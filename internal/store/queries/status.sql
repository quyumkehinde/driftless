-- name: StatusEvents :one
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE processed_at IS NULL) AS unprocessed,
  coalesce(max(received_at), 'epoch')::timestamptz AS last_received
FROM driftless.events;

-- name: StatusOldestLiveJob :one
SELECT coalesce(min(latest_event_created), 'epoch')::timestamptz AS oldest
FROM driftless.jobs
WHERE status IN ('pending', 'running');

-- name: StatusUnprocessedEventTypes :many
SELECT type, count(*) AS count
FROM driftless.events
WHERE processed_at IS NULL
GROUP BY type
ORDER BY count DESC, type;

-- name: StatusLastSweep :one
SELECT window_from, window_to, finished_at, events_seen, gaps_found
FROM driftless.sweeps
WHERE status = 'done'
ORDER BY id DESC
LIMIT 1;

-- name: StatusGapsLastDay :one
SELECT count(*) FROM driftless.gaps
WHERE detected_at > now() - interval '24 hours';

-- name: StatusLatestBackfillRun :one
SELECT r.id, r.status, r.started_at, r.finished_at,
  (SELECT count(*) FROM driftless.backfill_tasks t WHERE t.run_id = r.id AND t.status = 'done') AS tasks_done,
  (SELECT count(*) FROM driftless.backfill_tasks t WHERE t.run_id = r.id) AS tasks_total,
  (SELECT coalesce(sum(t.objects_done), 0)::bigint FROM driftless.backfill_tasks t WHERE t.run_id = r.id) AS objects_done
FROM driftless.backfill_runs r
ORDER BY r.id DESC
LIMIT 1;

-- name: StatusLatestVerification :one
SELECT mode, started_at, finished_at, checked, drifted, repaired
FROM driftless.verifications
ORDER BY id DESC
LIMIT 1;
