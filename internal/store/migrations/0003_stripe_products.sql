-- +goose Up

CREATE TABLE stripe.products (
  id           text PRIMARY KEY,                 -- prod_...
  data         jsonb NOT NULL,
  name         text  GENERATED ALWAYS AS (data->>'name') STORED,
  active       boolean GENERATED ALWAYS AS ((data->>'active')::boolean) STORED,
  type         text  GENERATED ALWAYS AS (data->>'type') STORED,
  created      timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode     boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id   text,
  is_deleted   boolean NOT NULL DEFAULT false,
  deleted_at   timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.products (active) WHERE NOT is_deleted;
