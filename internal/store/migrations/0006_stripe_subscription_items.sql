-- +goose Up

CREATE TABLE stripe.subscription_items (
  id           text PRIMARY KEY,                 -- si_...
  data         jsonb NOT NULL,
  subscription text  GENERATED ALWAYS AS (data->>'subscription') STORED,
  price        text  GENERATED ALWAYS AS (data->'price'->>'id') STORED,
  quantity     bigint GENERATED ALWAYS AS ((data->>'quantity')::bigint) STORED,
  created      timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode     boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id   text,
  is_deleted   boolean NOT NULL DEFAULT false,
  deleted_at   timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.subscription_items (subscription) WHERE NOT is_deleted;
