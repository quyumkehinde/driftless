# Driftless

**Your Postgres never drifts from Stripe.**

Driftless is a single-binary Go service that receives Stripe webhooks, verifies and
deduplicates them, detects the events Stripe never delivered, backfills history through
the REST API, and materializes everything into a `stripe` schema in **your own Postgres**
— with a reconciliation command that proves your database matches Stripe.

It is the supported, hardened answer to a problem every Stripe integrator eventually
hand-rolls: webhooks are at-least-once, unordered, and silently droppable, and the
event log only goes back 30 days. Existing tools cover fragments of the problem;
Driftless owns the whole receiver side.

## Why

Stripe's own engineering blog teaches customers to build DLQ pipelines and nightly
reconciliation jobs because "events may arrive out of sequence" and "retries can result
in duplicate deliveries." Teams report percent-level subscription drift and months-long
revenue leaks from handlers that returned 200 before the DB commit. Driftless is one
`docker run` (or one binary) that makes the whole class of problems go away —
self-hosted, so your payment data never touches a third party.

## How it works

Two dependencies: the `driftless` binary and your Postgres (17+). No queue, no broker,
no third-party service.

- **Receive** — an HTTP endpoint for Stripe webhooks with signature verification,
  secret rotation support, and ack-after-commit semantics.
- **Dedupe & order** — every event lands in an append-only log exactly once; object
  state is applied in a way that tolerates duplicates and out-of-order delivery.
- **Gap detection** — a sweeper polls Stripe's event list and finds the events your
  endpoint was never sent, before the 30-day event-log cliff erases the evidence.
- **Backfill** — full or incremental history import through the REST API, resumable
  across restarts.
- **Materialize** — clean per-object tables (`stripe.customers`, `stripe.invoices`,
  `stripe.subscriptions`, …) with the raw JSON preserved alongside typed columns.
- **Prove** — `driftless verify` reconciles your database against Stripe and reports
  (or repairs) any drift; exit codes make it a natural CI check.

Driftless is strictly **read-only against Stripe** — it never writes to your Stripe
account, and works with a restricted read-only API key.

## Status

⚠️ **Early development — not yet usable.** The first working release (v1.0.0) targets
Stripe only, self-hosted only, Postgres only. Watch the repo for the first release.

## License

Apache-2.0. No CLA.
