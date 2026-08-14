-- +goose Up

CREATE TABLE stripe.subscriptions (
  id                   text PRIMARY KEY,         -- sub_...
  data                 jsonb NOT NULL,
  customer             text  GENERATED ALWAYS AS (data->>'customer') STORED,
  status               text  GENERATED ALWAYS AS (data->>'status') STORED,
  cancel_at_period_end boolean GENERATED ALWAYS AS ((data->>'cancel_at_period_end')::boolean) STORED,
  current_period_start timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'current_period_start')::bigint)) STORED,
  current_period_end   timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'current_period_end')::bigint)) STORED,
  canceled_at          timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'canceled_at')::bigint)) STORED,
  trial_end            timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'trial_end')::bigint)) STORED,
  created              timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode             boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id           text,
  is_deleted           boolean NOT NULL DEFAULT false,
  deleted_at           timestamptz,
  updated_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.subscriptions (customer) WHERE NOT is_deleted;
CREATE INDEX ON stripe.subscriptions (status) WHERE NOT is_deleted;
