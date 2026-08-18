-- name: CreateVerification :one
INSERT INTO driftless.verifications (mode, object_type)
VALUES ($1, $2)
RETURNING id;

-- name: FinishVerification :exec
UPDATE driftless.verifications
SET finished_at = now(), checked = $2, drifted = $3, repaired = $4, report = $5
WHERE id = $1;
