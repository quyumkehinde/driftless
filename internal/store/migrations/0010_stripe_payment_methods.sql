-- +goose Up

CREATE TABLE stripe.payment_methods (
  id         text PRIMARY KEY,                   -- pm_...
  data       jsonb NOT NULL,
  customer   text  GENERATED ALWAYS AS (data->>'customer') STORED,
  type       text  GENERATED ALWAYS AS (data->>'type') STORED,
  card_brand text  GENERATED ALWAYS AS (data->'card'->>'brand') STORED,
  card_last4 text  GENERATED ALWAYS AS (data->'card'->>'last4') STORED,
  created    timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode   boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id text,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.payment_methods (customer) WHERE NOT is_deleted;
