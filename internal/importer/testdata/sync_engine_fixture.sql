-- A miniature stripe/sync-engine layout: typed columns, pipeline
-- bookkeeping, epoch fields stored both ways (integer and timestamptz)
-- to exercise reconstruction.
CREATE SCHEMA sync_engine;

CREATE TABLE sync_engine.products (
  id         text PRIMARY KEY,
  name       text,
  active     boolean,
  created    integer,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync_engine.customers (
  id         text PRIMARY KEY,
  email      text,
  name       text,
  metadata   jsonb,
  created    integer,
  deleted    boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync_engine.subscriptions (
  id                   text PRIMARY KEY,
  customer             text,
  status               text,
  created              timestamptz,
  current_period_start timestamptz,
  current_period_end   timestamptz,
  updated_at           timestamptz NOT NULL DEFAULT now()
);

INSERT INTO sync_engine.products (id, name, active, created) VALUES
  ('prod_imp', 'Pro', true, 1700000000);

INSERT INTO sync_engine.customers (id, email, name, metadata, created, deleted) VALUES
  ('cus_imp1', 'one@x.y', 'One', '{"tier": "gold"}', 1700000001, false),
  ('cus_imp2', NULL, 'Two', NULL, 1700000002, false),
  ('cus_existing', 'stale@x.y', 'Should Not Overwrite', NULL, 1700000003, false),
  ('cus_gone', 'gone@x.y', 'Deleted Upstream', NULL, 1700000005, true);

INSERT INTO sync_engine.subscriptions (id, customer, status, created, current_period_start, current_period_end) VALUES
  ('sub_imp', 'cus_imp1', 'active',
   to_timestamp(1700000004), to_timestamp(1700000100), to_timestamp(1702592100));
