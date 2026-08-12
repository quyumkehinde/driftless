-- name: EnqueueJob :one
-- Coalescing enqueue: at most one live job per (object_type, object_id).
-- A conflict bumps the existing job to the newest event instead of adding a
-- row; the bump only moves latest_event_* forward, never backward.
INSERT INTO driftless.jobs (kind, object_type, object_id, latest_event_id, latest_event_created, priority)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (object_type, object_id) WHERE status IN ('pending','running')
DO UPDATE SET
  latest_event_id = CASE
    WHEN COALESCE(EXCLUDED.latest_event_created, '-infinity') > COALESCE(driftless.jobs.latest_event_created, '-infinity')
    THEN EXCLUDED.latest_event_id
    ELSE driftless.jobs.latest_event_id
  END,
  latest_event_created = GREATEST(driftless.jobs.latest_event_created, EXCLUDED.latest_event_created),
  updated_at = now()
RETURNING id, (xmax = 0) AS inserted;

-- name: ClaimJob :one
-- Atomic claim: the SKIP LOCKED subquery guarantees no two workers get the
-- same row. attempts counts started tries, so jobs that crash a worker are
-- bounded by max_attempts through the reaper as well as the failure path.
UPDATE driftless.jobs
SET status = 'running',
    claimed_until = now() + sqlc.arg(visibility_timeout)::interval,
    attempts = attempts + 1,
    updated_at = now()
WHERE id = (
  SELECT id FROM driftless.jobs
  WHERE status = 'pending' AND run_after <= now()
  ORDER BY priority, id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: CompleteJob :one
-- If a newer event was coalesced onto the job while it ran, the work that
-- just finished may be stale: requeue instead of done.
UPDATE driftless.jobs
SET status = CASE
      WHEN COALESCE(latest_event_created, '-infinity'::timestamptz)
           > COALESCE(sqlc.narg(claimed_event_created)::timestamptz, '-infinity'::timestamptz)
      THEN 'pending'
      ELSE 'done'
    END,
    run_after = now(),
    claimed_until = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING status;

-- name: FailJob :one
UPDATE driftless.jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
    run_after = now() + sqlc.arg(backoff)::interval,
    claimed_until = NULL,
    last_error = sqlc.arg(last_error),
    updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING status;

-- name: ReapExpiredJobs :execrows
-- Resurrect jobs whose worker died without completing or failing them.
UPDATE driftless.jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
    claimed_until = NULL,
    last_error = COALESCE(last_error, 'worker crashed or lost its claim'),
    updated_at = now()
WHERE status = 'running' AND claimed_until < now();

-- name: ListJobs :many
SELECT * FROM driftless.jobs
WHERE status = sqlc.arg(status)
ORDER BY id
LIMIT sqlc.arg(row_limit);

-- name: RetryDeadJobs :execrows
UPDATE driftless.jobs
SET status = 'pending', attempts = 0, run_after = now(), last_error = NULL, updated_at = now()
WHERE status = 'dead';

-- name: RetryJob :execrows
UPDATE driftless.jobs
SET status = 'pending', attempts = 0, run_after = now(), last_error = NULL, updated_at = now()
WHERE id = $1 AND status = 'dead';

-- name: CountJobsByStatus :many
SELECT status, count(*) AS count FROM driftless.jobs GROUP BY status;
