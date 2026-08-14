-- +goose Up

CREATE SCHEMA IF NOT EXISTS stripe;

CREATE TABLE stripe.customers (
  id           text PRIMARY KEY,                 -- cus_...
  data         jsonb NOT NULL,                   -- full object as fetched
  -- generated typed columns for the fields people actually query:
  email        text  GENERATED ALWAYS AS (data->>'email') STORED,
  name         text  GENERATED ALWAYS AS (data->>'name') STORED,
  created      timestamptz GENERATED ALWAYS AS (to_timestamp((data->>'created')::bigint)) STORED,
  currency     text  GENERATED ALWAYS AS (data->>'currency') STORED,
  delinquent   boolean GENERATED ALWAYS AS ((data->>'delinquent')::boolean) STORED,
  livemode     boolean GENERATED ALWAYS AS ((data->>'livemode')::boolean) STORED,
  -- driftless bookkeeping:
  account_id   text,                             -- Connect, NULL in v1
  is_deleted   boolean NOT NULL DEFAULT false,
  deleted_at   timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON stripe.customers (email) WHERE NOT is_deleted;
