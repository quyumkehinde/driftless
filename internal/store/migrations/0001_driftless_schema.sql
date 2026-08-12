-- +goose Up

CREATE SCHEMA IF NOT EXISTS driftless;

-- Append-only log of every webhook event we have ever seen (received or swept).
CREATE TABLE driftless.events (
  event_id        text PRIMARY KEY,              -- evt_...
  type            text NOT NULL,                 -- e.g. customer.subscription.updated
  api_version     text,                          -- from event payload
  account_id      text,                          -- Connect; always NULL in v1
  created         timestamptz NOT NULL,          -- Stripe's event.created
  received_at     timestamptz NOT NULL DEFAULT now(),
  source          text NOT NULL DEFAULT 'webhook'
                  CHECK (source IN ('webhook','sweep','backfill','manual')),
  payload         jsonb NOT NULL,                -- full event body as delivered/fetched
  processed_at    timestamptz,                   -- set when the resulting job completed
  livemode        boolean NOT NULL
);
CREATE INDEX ON driftless.events (created);
CREATE INDEX ON driftless.events (type, created);
CREATE INDEX ON driftless.events (processed_at) WHERE processed_at IS NULL;

-- Work queue. One row per (object_type, object_id) pending unit of work (coalesced).
CREATE TABLE driftless.jobs (
  id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind                 text NOT NULL DEFAULT 'sync_object'
                       CHECK (kind IN ('sync_object','process_event')),
  object_type          text NOT NULL,            -- 'customer','subscription',...
  object_id            text NOT NULL,            -- cus_..., sub_...
  latest_event_id      text,                     -- most recent event that poked this
  latest_event_created timestamptz,
  status               text NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','running','done','dead')),
  priority             smallint NOT NULL DEFAULT 100, -- lower = sooner
  attempts             int NOT NULL DEFAULT 0,
  max_attempts         int NOT NULL DEFAULT 8,
  run_after            timestamptz NOT NULL DEFAULT now(),
  claimed_until        timestamptz,
  last_error           text,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);
-- Coalescing key: at most one live job per object.
CREATE UNIQUE INDEX jobs_live_object ON driftless.jobs (object_type, object_id)
  WHERE status IN ('pending','running');
CREATE INDEX jobs_claim ON driftless.jobs (status, run_after, priority, id)
  WHERE status = 'pending';

-- Per-object bookkeeping (what do we believe, when did we last confirm it).
CREATE TABLE driftless.object_state (
  object_type       text NOT NULL,
  object_id         text NOT NULL,
  last_synced_at    timestamptz NOT NULL,        -- when we last wrote the stripe row
  last_event_created timestamptz,                -- newest event.created applied/observed
  last_event_id     text,
  sync_source       text NOT NULL,               -- 'fetch','payload','backfill','repair'
  fetch_failures    int NOT NULL DEFAULT 0,
  PRIMARY KEY (object_type, object_id)
);

-- Gap sweeper findings: events Stripe generated that we never received via webhook.
CREATE TABLE driftless.gaps (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id      text NOT NULL REFERENCES driftless.events(event_id),
  detected_at   timestamptz NOT NULL DEFAULT now(),
  sweep_id      bigint NOT NULL,
  event_created timestamptz NOT NULL,
  lag           interval NOT NULL               -- detected_at - event_created
);

-- Sweep checkpoints & audit.
CREATE TABLE driftless.sweeps (
  id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  started_at        timestamptz NOT NULL DEFAULT now(),
  finished_at       timestamptz,
  window_from       timestamptz NOT NULL,        -- oldest event.created examined
  window_to         timestamptz NOT NULL,
  events_seen       int NOT NULL DEFAULT 0,
  gaps_found        int NOT NULL DEFAULT 0,
  status            text NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','done','failed'))
);

-- Backfill orchestration: one run row, many task rows (one per object type).
CREATE TABLE driftless.backfill_runs (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  requested_by text NOT NULL,                    -- 'cli','auto-init'
  since        timestamptz,                      -- NULL = full history
  status       text NOT NULL DEFAULT 'running'
               CHECK (status IN ('running','done','failed','cancelled'))
);

CREATE TABLE driftless.backfill_tasks (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id        bigint NOT NULL REFERENCES driftless.backfill_runs(id),
  object_type   text NOT NULL,
  status        text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','running','done','failed')),
  cursor        text,                            -- Stripe starting_after id; commits with each page
  pages_done    int NOT NULL DEFAULT 0,
  objects_done  int NOT NULL DEFAULT 0,
  last_error    text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, object_type)
);

-- Verify results (so drift history is queryable / graphable).
CREATE TABLE driftless.verifications (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  mode          text NOT NULL,                   -- 'quick','full'
  object_type   text,                            -- NULL = all
  checked       int NOT NULL DEFAULT 0,
  drifted       int NOT NULL DEFAULT 0,
  repaired      int NOT NULL DEFAULT 0,
  report        jsonb                            -- per-object drift details
);

-- Single-row settings/identity table (guards against pointing at the wrong account).
CREATE TABLE driftless.meta (
  id                 boolean PRIMARY KEY DEFAULT true CHECK (id),
  stripe_account_id  text,                       -- acct_..., learned at first API call
  livemode           boolean,
  schema_version     text NOT NULL,
  initialized_at     timestamptz NOT NULL DEFAULT now()
);
