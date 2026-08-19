## driftless serve

Run the webhook receiver and metrics listeners

### Synopsis

Serve is the long-running service: it receives and verifies Stripe webhooks,
applies events to the mirror with a worker pool, sweeps for events Stripe
never delivered, purges expired raw events, and runs the nightly quick
verification. Listeners: webhook ingest on server.listen, metrics and
readiness on server.metrics_listen.

Running more than one replica is safe. Workers claim jobs with SKIP LOCKED
and the scheduled passes elect a leader through Postgres advisory locks.

```
driftless serve [flags]
```

### Options

```
      --force-account   overwrite the recorded Stripe account instead of refusing on mismatch
  -h, --help            help for serve
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

