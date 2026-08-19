## driftless

Keep your Postgres in sync with Stripe

### Synopsis

Driftless receives Stripe webhooks, detects delivery gaps, backfills history,
and materializes everything into a stripe schema in your own Postgres.

### Options

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
  -h, --help                help for driftless
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless backfill](driftless_backfill.md)	 - Import history from the Stripe API into the mirror
* [driftless config](driftless_config.md)	 - Inspect configuration
* [driftless doctor](driftless_doctor.md)	 - Check the environment: database, migrations, Stripe key, webhook setup
* [driftless events](driftless_events.md)	 - Inspect stored events
* [driftless import](driftless_import.md)	 - Migrate rows from a stripe/sync-engine database into the mirror
* [driftless init](driftless_init.md)	 - First-run setup: check the environment, migrate, offer a backfill
* [driftless jobs](driftless_jobs.md)	 - Inspect and retry queue jobs
* [driftless migrate](driftless_migrate.md)	 - Manage database schema migrations
* [driftless serve](driftless_serve.md)	 - Run the webhook receiver and metrics listeners
* [driftless status](driftless_status.md)	 - Show sync health at a glance: events, queue, sweeps, backfill, drift
* [driftless verify](driftless_verify.md)	 - Reconcile the mirror against Stripe and report drift
* [driftless version](driftless_version.md)	 - Print the driftless version

