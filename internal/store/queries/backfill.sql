-- name: CreateBackfillRun :one
INSERT INTO driftless.backfill_runs (requested_by, since)
VALUES ($1, $2)
RETURNING *;

-- name: GetRunningBackfillRun :one
SELECT id FROM driftless.backfill_runs WHERE status = 'running' LIMIT 1;

-- name: GetBackfillRun :one
SELECT * FROM driftless.backfill_runs WHERE id = $1;

-- name: FinishBackfillRun :exec
UPDATE driftless.backfill_runs SET status = $2, finished_at = now() WHERE id = $1;

-- name: ListResumableRuns :many
-- Runs a crash left in progress; serve's auto_resume picks these up.
-- Cancelled runs are deliberately absent: a human stopped them, so a
-- human restarts them.
SELECT * FROM driftless.backfill_runs WHERE status = 'running' ORDER BY id;

-- name: CancelBackfillRun :execrows
-- Records a deliberate stop, so auto_resume leaves the run alone.
UPDATE driftless.backfill_runs SET status = 'cancelled', finished_at = now()
WHERE id = $1 AND status = 'running';

-- name: ReactivateBackfillRun :execrows
-- An explicit resume of a cancelled run puts it back in flight.
UPDATE driftless.backfill_runs SET status = 'running', finished_at = NULL
WHERE id = $1 AND status IN ('running', 'cancelled');

-- name: CreateBackfillTask :one
INSERT INTO driftless.backfill_tasks (run_id, object_type)
VALUES ($1, $2)
ON CONFLICT (run_id, object_type) DO UPDATE SET run_id = EXCLUDED.run_id
RETURNING *;

-- name: ListRunTasks :many
SELECT * FROM driftless.backfill_tasks WHERE run_id = $1 ORDER BY id;

-- name: StartBackfillTask :exec
UPDATE driftless.backfill_tasks SET status = 'running', updated_at = now() WHERE id = $1;

-- name: AdvanceBackfillCursor :exec
-- Committed in the same transaction as the page's upserts: crash recovery
-- resumes at this page boundary.
UPDATE driftless.backfill_tasks
SET cursor = $2,
    pages_done = pages_done + 1,
    objects_done = objects_done + $3,
    updated_at = now()
WHERE id = $1;

-- name: FinishBackfillTask :exec
UPDATE driftless.backfill_tasks
SET status = $2, last_error = $3, updated_at = now()
WHERE id = $1;
