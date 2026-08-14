-- +goose Up

CREATE TABLE stripe.checkout_sessions (
  id             text PRIMARY KEY,               -- cs_...
  data           jsonb NOT NULL,
  customer       text  GENERATED ALWAYS AS (data->>'customer') STORED,
  subscription   text  GENERATED ALWAYS AS (data->>'subscription') STORED,
  payment_intent text  GENERATED ALWAYS AS (data->>'payment_intent') STORED,
  status         text  GENERATED ALWAYS AS (data->>'status') STORED,
  mode           text  GENERATED ALWAYS AS (data->>'mode') STORED,
  amount_total   bigint GENERATED ALWAYS AS ((data->>'amount_total')::bigint) STORED,
  currency       text  GENERATED ALWAYS AS (data->>'currency') STORED,
  created        timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode       boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id     text,
  is_deleted     boolean NOT NULL DEFAULT false,
  deleted_at     timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.checkout_sessions (customer) WHERE NOT is_deleted;
CREATE INDEX ON stripe.checkout_sessions (subscription) WHERE NOT is_deleted;
