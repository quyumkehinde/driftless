-- +goose Up

CREATE TABLE stripe.refunds (
  id             text PRIMARY KEY,               -- re_...
  data           jsonb NOT NULL,
  charge         text  GENERATED ALWAYS AS (data->>'charge') STORED,
  payment_intent text  GENERATED ALWAYS AS (data->>'payment_intent') STORED,
  status         text  GENERATED ALWAYS AS (data->>'status') STORED,
  amount         bigint GENERATED ALWAYS AS ((data->>'amount')::bigint) STORED,
  currency       text  GENERATED ALWAYS AS (data->>'currency') STORED,
  reason         text  GENERATED ALWAYS AS (data->>'reason') STORED,
  created        timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode       boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id     text,
  is_deleted     boolean NOT NULL DEFAULT false,
  deleted_at     timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.refunds (charge) WHERE NOT is_deleted;
