# Driftless

**Your Postgres never drifts from Stripe.**

Driftless mirrors your Stripe account into a `stripe` schema in your own
Postgres, then proves the mirror matches. It is one static Go binary. No
queue, no broker, no third-party service.

It is read-only against Stripe: it never writes to your account, and runs on
a restricted read-only API key (`driftless doctor` lists the scopes to grant).

## The problem

Stripe's webhook delivery is at-least-once, unordered, and can fail silently.
The event log only goes back 30 days, so by the time you notice a gap, the
evidence of it is already gone. Most teams end up hand-rolling a dedupe table,
a replay handler and a resync cron, and still have no way to answer "is my
billing data correct right now?"

Driftless replaces that machinery. `driftless verify` exits `0` when your
database provably matches Stripe and `3` when it finds drift, with a
per-object report.

## Quickstart

You need Postgres 17+, a Stripe API key (a restricted read-only `rk_...` key
is recommended), and a webhook signing secret.

```bash
# 1. Install: one static binary, no runtime
curl -fsSL https://raw.githubusercontent.com/quyumkehinde/driftless/main/scripts/install.sh | sh

# 2. Point it at your Postgres and your Stripe account
export DRIFTLESS_DATABASE_URL='postgres://...'
export DRIFTLESS_STRIPE_API_KEY='rk_test_...'
export DRIFTLESS_STRIPE_WEBHOOK_SECRET='whsec_...'

# 3. First-run setup: environment checks, migrations, offer of a backfill
driftless init

# 4. Run it
driftless serve

# 5. Prove it
driftless verify --full
```

Then send Stripe's webhooks to it. In the Stripe dashboard under Developers >
Webhooks, add `https://<your-host>/webhooks/stripe` and subscribe to the event
types you mirror, or to all events; types Driftless does not model are still
stored, never dropped. Working locally, use the Stripe CLI instead:
`stripe listen --forward-to localhost:8724/webhooks/stripe`.

Prefer containers? Docker Compose, a distroless image at
`ghcr.io/quyumkehinde/driftless`, and Kubernetes manifests ship in
[`deploy/`](deploy/).

From here `serve` keeps the mirror correct on its own: webhooks apply in
seconds, a sweeper picks up anything Stripe generated but never delivered, and
a verification runs nightly by default.

Coming from `stripe/sync-engine`? `driftless import` migrates a sync-engine
database in place, without copying your data out of Postgres. See
[`docs/migrating-from-sync-engine.md`](docs/migrating-from-sync-engine.md).

## What gets mirrored

| | |
|---|---|
| Customers and billing | `customers`, `subscriptions`, `subscription_items`, `invoices` |
| Payments | `payment_intents`, `charges`, `refunds`, `disputes` |
| Setup and checkout | `payment_methods`, `setup_intents`, `checkout_sessions` |
| Catalog | `products`, `prices` |

All under the `stripe` schema. Each table carries typed, indexed columns for
the hot fields and the complete raw payload alongside them in `data`, so
nothing Stripe sends is truncated away. Deletes are soft.

## Using the mirror

The mirror replaces two things in your application: reads that used to be
Stripe API calls, and reactions that used to be webhook handlers.

Reads become SQL. Anywhere you call the Stripe API or maintain hand-rolled
billing state, query the tables and join them against your own:

```sql
-- can this user access the product?
SELECT s.status IN ('active', 'trialing')
FROM stripe.subscriptions s
JOIN app.users u ON u.stripe_customer_id = s.customer
WHERE u.id = $1 AND NOT s.is_deleted
ORDER BY s.created DESC LIMIT 1;
```

Reactions become LISTEN/NOTIFY. Every applied change fires `pg_notify` on the
`driftless_changes` channel, in the same transaction as the write, carrying
ids only: `{"type": "subscription", "id": "sub_123"}`. Your worker listens,
re-reads the row and acts. By the time it hears about a change the state is
already committed, deduplicated and ordered, so no payload parsing, signature
checking or replay handling is left in your code.

Notifications are not durable. A listener that was down misses the ring but
never the row, so catch up on everything since your last cursor when you
connect, then listen:

```go
conn, _ := pgx.Connect(ctx, dbURL) // dedicated connection, not a pool
conn.Exec(ctx, "LISTEN driftless_changes")

// catch up on anything missed while nobody was listening
rows, _ := conn.Query(ctx,
	"SELECT 'subscription', id FROM stripe.subscriptions WHERE updated_at > $1", since)
// handle each row, then:

for {
	n, _ := conn.WaitForNotification(ctx) // {"type":"subscription","id":"sub_123"}
	// read the row, act
}
```

One thing that will bite you: the listener needs a direct or session-pooled
connection. Transaction-mode poolers (pgbouncer, Supavisor) hand statements to
arbitrary backend sessions, so `LISTEN` through them silently receives
nothing. The rest of your app can keep using the pooler.

Node and Python equivalents, and notes for Supabase, are in
[`docs/listening-for-changes.md`](docs/listening-for-changes.md).

Separately from the mirror tables, `driftless.events` keeps raw events past
Stripe's roughly 30-day window (90 days by default, `retention.events_days: 0`
to keep them forever) as an audit trail of what Stripe actually sent. Inspect
one with `driftless events show evt_...`.

## How it works

Two dependencies: the `driftless` binary and your Postgres.

- **Receive.** An HTTP endpoint with signature verification and secret
  rotation. Stripe never gets a 200 until the event is durably committed.
- **Dedupe and order.** Every event lands in an append-only log exactly once.
  Object state is applied so that duplicates, out-of-order delivery and
  same-second timestamp ties all converge on the same result.
- **Detect gaps.** A sweeper polls Stripe's event list on a schedule and finds
  the events your endpoint was never sent, whether from a misconfigured URL, a
  load balancer eating deliveries, or downtime past Stripe's retry window. It
  alarms loudly as the 30-day cliff approaches.
- **Backfill.** Full or incremental history import through the REST API,
  resumable across restarts and `kill -9`, including the canceled
  subscriptions that Stripe's default listing hides.
- **Verify.** `driftless verify` reconciles the database against Stripe, and
  reports or repairs whatever it finds.

## What it guarantees

- Every event Stripe delivers is recorded exactly once, or Stripe gets a
  non-2xx and retries.
- Every event Stripe generates but fails to deliver is found by the sweeper
  while the events API still holds it.
- Object rows converge on Stripe's current truth regardless of delivery order,
  duplication or same-second ties.
- A `kill -9` at any moment loses no acknowledged data.

Exactly-once *delivery* is deliberately not on that list. No receiver can
promise it. Recorded exactly once, and provably converged, are what you get
instead.

## Operating it

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

The full reference, generated from the binary's own help text, is in
[`docs/cli/`](docs/cli/driftless.md).

Prometheus metrics are served on `:8725/metrics`, and recommended alert rules
with runbook annotations ship in [`contrib/alerts.yaml`](contrib/alerts.yaml).
Deploy recipes for Docker Compose, systemd and Kubernetes live in
[`deploy/`](deploy/); running more than one replica is safe, because work is
claimed with `SKIP LOCKED` and scheduled jobs elect a leader per pass.

Because `verify` exits `3` on drift, any scheduler can turn "is billing data
correct?" into a check that pages you:

```bash
docker run --rm \
  -e DRIFTLESS_DATABASE_URL -e DRIFTLESS_STRIPE_API_KEY \
  ghcr.io/quyumkehinde/driftless:latest verify --full --format json
```

The output names the exact objects and the kind of divergence.
`verify --repair` fixes them.

## Compared to the alternatives

| | Driftless | stripe/sync-engine | Hookdeck / Svix | Airbyte / Fivetran |
|---|---|---|---|---|
| Webhook receive + signature check | yes | yes | yes | no |
| Guaranteed dedupe | yes | partial | best-effort | n/a |
| Out-of-order and same-second safe | yes | no | no | n/a |
| Detects never-delivered events | yes | no | no | no |
| Resumable backfill incl. canceled subs | yes | no | no | polling sync |
| Reconciliation proof | `verify`, exit-code contract | no | no | no |
| Where your data lives | your Postgres | your Postgres | their cloud, then yours | your warehouse, hours later |

These are overlapping tools, not strictly competing ones. Hookdeck and Svix are
general webhook infrastructure across many providers, and are the better
answer if Stripe is one of several sources you need to handle uniformly.
Airbyte and Fivetran feed analytics, where hours of lag is fine. Driftless only
does Stripe, and the thing it does that the others do not is prove the result
is correct.

## Limits

Stripe only, Postgres only, self-hosted only. No Stripe Connect yet, so
single-account mirrors. Payment methods are mirrored shallowly: per customer,
for customers with subscriptions.

Everything that makes the mirror trustworthy lives in the open-source binary
and always will. A paid tier, if one ever exists, would only add hosting and
dashboards on top. If that is the part you want,
[leave an email on the waitlist](https://getdriftless.dev/#managed).

## Status

Stable. The chaos suite (crash-kill during load, backfill `kill -9` and
resume, 429 storms, delivered-shuffled-and-duplicated convergence) runs green
under `-race` in CI.

## License

Apache-2.0. No CLA.
