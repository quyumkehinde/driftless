-- +goose Up

CREATE TABLE stripe.payment_intents (
  id            text PRIMARY KEY,                -- pi_...
  data          jsonb NOT NULL,
  customer      text  GENERATED ALWAYS AS (data->>'customer') STORED,
  status        text  GENERATED ALWAYS AS (data->>'status') STORED,
  amount        bigint GENERATED ALWAYS AS ((data->>'amount')::bigint) STORED,
  currency      text  GENERATED ALWAYS AS (data->>'currency') STORED,
  latest_charge text  GENERATED ALWAYS AS (data->>'latest_charge') STORED,
  created       timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode      boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id    text,
  is_deleted    boolean NOT NULL DEFAULT false,
  deleted_at    timestamptz,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.payment_intents (customer) WHERE NOT is_deleted;
CREATE INDEX ON stripe.payment_intents (status) WHERE NOT is_deleted;
