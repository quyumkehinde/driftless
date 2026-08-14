-- +goose Up

CREATE TABLE stripe.invoices (
  id           text PRIMARY KEY,                 -- in_...
  data         jsonb NOT NULL,
  customer     text  GENERATED ALWAYS AS (data->>'customer') STORED,
  subscription text  GENERATED ALWAYS AS (data->>'subscription') STORED,
  status       text  GENERATED ALWAYS AS (data->>'status') STORED,
  total        bigint GENERATED ALWAYS AS ((data->>'total')::bigint) STORED,
  amount_paid  bigint GENERATED ALWAYS AS ((data->>'amount_paid')::bigint) STORED,
  amount_due   bigint GENERATED ALWAYS AS ((data->>'amount_due')::bigint) STORED,
  currency     text  GENERATED ALWAYS AS (data->>'currency') STORED,
  period_start timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'period_start')::bigint)) STORED,
  period_end   timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'period_end')::bigint)) STORED,
  number       text  GENERATED ALWAYS AS (data->>'number') STORED,
  created      timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  livemode     boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  account_id   text,
  is_deleted   boolean NOT NULL DEFAULT false,
  deleted_at   timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.invoices (customer) WHERE NOT is_deleted;
CREATE INDEX ON stripe.invoices (subscription) WHERE NOT is_deleted;
CREATE INDEX ON stripe.invoices (status) WHERE NOT is_deleted;
