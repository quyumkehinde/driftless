-- +goose Up
-- At most one backfill run may be in progress at a time. A crashed run
-- keeps status 'running' until resumed or cancelled, and counts.
CREATE UNIQUE INDEX backfill_single_running
  ON driftless.backfill_runs ((true))
  WHERE status = 'running';
