-- +goose Up

CREATE TABLE stripe.setup_intents (
  id             text PRIMARY KEY,               -- seti_...
  data           jsonb NOT NULL,
  customer       text  GENERATED ALWAYS AS (data->>'customer') STORED,
  status         text  GENERATED ALWAYS AS (data->>'status') STORED,
  payment_method text  GENERATED ALWAYS AS (data->>'payment_method') STORED,
  created        timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode       boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id     text,
  is_deleted     boolean NOT NULL DEFAULT false,
  deleted_at     timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.setup_intents (customer) WHERE NOT is_deleted;
