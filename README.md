# Driftless

**Your Postgres never drifts from Stripe.**

Driftless is a single-binary Go service that receives Stripe webhooks, verifies and deduplicates them, detects the events Stripe never delivered, backfills history through the REST API, and materializes everything into a `stripe` schema in **your own Postgres**, with a reconciliation command that proves your database matches Stripe.

It is the supported, hardened answer to a problem every Stripe integrator eventually hand-rolls: webhook delivery is at-least-once, unordered, and silently droppable, and the event log only goes back 30 days. Existing tools cover fragments of the problem; Driftless owns the whole receiver side.

Driftless is strictly **read-only against Stripe**: it never writes to your Stripe account, and works with a restricted read-only API key (`driftless doctor` lists the exact scopes to grant).

## Quickstart

Ten minutes from zero to a proven-correct mirror. You need Postgres 17+, a Stripe API key (a restricted read-only `rk_...` key is recommended), and a webhook signing secret.

```bash
# 1. Install: one static binary, no runtime
#    https://github.com/quyumkehinde/driftless/releases
tar xzf driftless_*.tar.gz && sudo install driftless /usr/local/bin/

# 2. Point it at your Postgres and your Stripe account
export DRIFTLESS_DATABASE_URL='postgres://...'
export DRIFTLESS_STRIPE_API_KEY='rk_test_...'
export DRIFTLESS_STRIPE_WEBHOOK_SECRET='whsec_...'

# 3. First-run setup: environment checks, migrations, offer of a backfill
driftless init

# 4. Run it, and point a Stripe webhook endpoint at your host
#    https://<your-host>/webhooks/stripe
#    (dashboard -> Developers -> Webhooks; subscribe to the event types
#    you mirror, or "all events"; unknown types are stored, never lost)
driftless serve

# 5. Prove it
driftless verify --full
```

Prefer containers? A Docker Compose quickstart, a distroless image at `ghcr.io/quyumkehinde/driftless`, and Kubernetes manifests ship in [`deploy/`](deploy/).

`verify` exits `0` when your database provably matches Stripe, and `3` when it found drift (with a per-object report). From then on, serve keeps the mirror correct on its own: webhooks apply in seconds, a sweeper finds anything Stripe generated but never delivered, and a nightly verification runs by default.

Query your data like any other tables:

```sql
SELECT id, email, created FROM stripe.customers WHERE NOT is_deleted;
SELECT status, count(*) FROM stripe.subscriptions GROUP BY status;
```

Every table carries typed, indexed columns for the hot fields and the full raw JSON alongside (`data`), so nothing Stripe sends is ever truncated away.

## How it works

Two dependencies: the `driftless` binary and your Postgres (17+). No queue, no broker, no third-party service.

- **Receive**: an HTTP endpoint for Stripe webhooks with signature verification, secret rotation support, and ack-after-commit semantics: Stripe never gets a 200 until the event is durably recorded.
- **Dedupe & order**: every event lands in an append-only log exactly once; object state is applied in a way that tolerates duplicates, out-of-order delivery, and same-second timestamp ties.
- **Gap detection**: a sweeper polls Stripe's event list on a schedule and finds the events your endpoint was never sent (misconfigured URLs, load balancers eating deliveries, downtime past Stripe's retry window) before the 30-day event-log cliff erases the evidence. It alarms loudly when the cliff approaches.
- **Backfill**: full or incremental history import through the REST API, resumable across restarts and `kill -9`, including the canceled subscriptions the default listing hides.
- **Materialize**: clean per-object tables (`stripe.customers`, `stripe.invoices`, `stripe.subscriptions`, …) with soft deletes, generated columns, and the raw JSON preserved.
- **Prove**: `driftless verify` reconciles your database against Stripe and reports (or repairs) any drift.

What Driftless promises, precisely: every event Stripe delivers is recorded once, or Stripe gets a non-2xx and retries. Every event Stripe generates but fails to deliver is found by the sweeper while the events API still holds it. Object rows converge to Stripe's current truth regardless of delivery order, duplication, or ties. A `kill -9` at any moment loses no acknowledged data. (Note what it does not say: "exactly-once delivery" is not a thing anyone can promise you; recorded-exactly-once is.)

## Drift as a failing CI check

`verify --format json` exits `3` when drift is found, which turns "is billing data correct?" into a check that can page you:

```yaml
name: billing-drift
on:
  schedule:
    - cron: "0 6 * * *"
jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: |
          docker run --rm \
            -e DRIFTLESS_DATABASE_URL="${{ secrets.DRIFTLESS_DATABASE_URL }}" \
            -e DRIFTLESS_STRIPE_API_KEY="${{ secrets.STRIPE_RK_KEY }}" \
            ghcr.io/quyumkehinde/driftless:latest verify --full --format json
```

A failing run means your database and Stripe disagree; the job log contains the exact objects and the kind of divergence. `verify --repair` fixes what it finds.

## Migrating from stripe/sync-engine

Driftless mirrors sync-engine's table naming, and `driftless import` migrates a sync-engine database in place with zero copying of your Postgres data out:

```bash
# stop sync-engine, then rename its schema out of the way (instant, metadata-only)
psql "$DATABASE_URL" -c 'ALTER SCHEMA stripe RENAME TO sync_engine'

driftless migrate up                    # creates the fresh stripe schema
driftless import --from-sync-engine     # copies rows, table by table
driftless verify --repair               # re-fetches true state for every object
driftless serve                         # from here on, it stays correct

# when satisfied:
psql "$DATABASE_URL" -c 'DROP SCHEMA sync_engine CASCADE'
```

Import reconstructs objects from sync-engine's typed columns and marks them import-sourced; the `verify --repair` step re-fetches every one of them from Stripe, so the mirror ends byte-true regardless of what the old pipeline had dropped.

## Alternatives

| | Driftless | stripe/sync-engine | Hookdeck / Svix | Airbyte / Fivetran |
|---|---|---|---|---|
| Webhook receive + verify | yes | yes | yes | no |
| Dedupe guaranteed | yes | partial | best-effort, documented as such | n/a |
| Out-of-order and same-second safe | yes | known open issue | disclaimed | n/a |
| Detects never-delivered events | yes | no | no | no |
| Resumable backfill incl. canceled subscriptions | yes | known open issue | no | polling sync |
| Reconciliation proof | `verify`, exit-code contract | no | no | no |
| Where your data lives | your Postgres | your Postgres | their cloud, then yours | your warehouse, hours later |

## Operating it

Full command reference, generated from the binary's own help text, lives in [`docs/cli/`](docs/cli/driftless.md).

| Command | Does |
|---|---|
| `serve` | run everything: ingest, workers, sweeper, retention, nightly verify |
| `init` | first-run setup: checks, migrations, offer of a backfill |
| `backfill` | import history, resumable, `--since` / `--type` / `--dry-run` |
| `verify` | reconcile against Stripe; exit 3 on drift; `--repair` fixes |
| `status` | one-screen health summary |
| `doctor` | environment checks with copy-paste fixes |
| `jobs list` / `jobs retry` | inspect and requeue dead work |
| `events show` | dump one stored event, payload included |
| `import` | migrate from stripe/sync-engine |
| `migrate up` / `migrate status` | schema migrations, embedded, forward-only |
| `config print` | effective configuration, secrets redacted |
- Prometheus metrics on `:8725/metrics`; recommended alert rules with runbook annotations ship in [`contrib/alerts.yaml`](contrib/alerts.yaml).
- Deploy recipes for Docker Compose, systemd, and Kubernetes live in [`deploy/`](deploy/). Replicas beyond one are safe: work is claimed with `SKIP LOCKED` and scheduled jobs elect a leader per pass.

## Limits, honestly

Stripe only. Self-hosted only. Postgres only. No Stripe Connect yet (single-account mirrors). Payment methods are mirrored shallowly (per-customer, for customers with subscriptions). If you need a hosted control plane, that may come later; the OSS core is complete and stays that way.

## Status

Release candidate. The chaos suite (crash-kill during load, backfill kill -9 and resume, 429 storms, delivered-shuffled-and-duplicated convergence) runs green under `-race` in CI. v1.0.0 follows a soak period on live accounts.

## License

Apache-2.0. No CLA.
