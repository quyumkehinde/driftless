-- +goose Up

CREATE TABLE stripe.prices (
  id                 text PRIMARY KEY,           -- price_...
  data               jsonb NOT NULL,
  product            text  GENERATED ALWAYS AS (data->>'product') STORED,
  active             boolean GENERATED ALWAYS AS ((data->>'active')::boolean) STORED,
  currency           text  GENERATED ALWAYS AS (data->>'currency') STORED,
  unit_amount        bigint GENERATED ALWAYS AS ((data->>'unit_amount')::bigint) STORED,
  recurring_interval text  GENERATED ALWAYS AS (data->'recurring'->>'interval') STORED,
  type               text  GENERATED ALWAYS AS (data->>'type') STORED,
  created            timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode           boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id         text,
  is_deleted         boolean NOT NULL DEFAULT false,
  deleted_at         timestamptz,
  updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.prices (product) WHERE NOT is_deleted;
