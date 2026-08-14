-- +goose Up

CREATE TABLE stripe.charges (
  id              text PRIMARY KEY,              -- ch_...
  data            jsonb NOT NULL,
  customer        text  GENERATED ALWAYS AS (data->>'customer') STORED,
  invoice         text  GENERATED ALWAYS AS (data->>'invoice') STORED,
  payment_intent  text  GENERATED ALWAYS AS (data->>'payment_intent') STORED,
  status          text  GENERATED ALWAYS AS (data->>'status') STORED,
  amount          bigint GENERATED ALWAYS AS ((data->>'amount')::bigint) STORED,
  amount_refunded bigint GENERATED ALWAYS AS ((data->>'amount_refunded')::bigint) STORED,
  currency        text  GENERATED ALWAYS AS (data->>'currency') STORED,
  paid            boolean GENERATED ALWAYS AS ((data->>'paid')::boolean) STORED,
  refunded        boolean GENERATED ALWAYS AS ((data->>'refunded')::boolean) STORED,
  created         timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode        boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id      text,
  is_deleted      boolean NOT NULL DEFAULT false,
  deleted_at      timestamptz,
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.charges (customer) WHERE NOT is_deleted;
CREATE INDEX ON stripe.charges (payment_intent) WHERE NOT is_deleted;
