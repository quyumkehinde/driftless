## driftless verify

Reconcile the mirror against Stripe and report drift

### Synopsis

Verify re-reads objects from Stripe and compares them against the mirror,
reporting every divergence: objects Stripe has that the mirror lacks,
objects whose stored data differs, and live mirror rows Stripe no longer
has. Full mode walks everything; quick mode walks the last day and
spot-checks a random sample of older history.

Exit codes are the CI contract: 0 means no drift, 3 means drift was found
(even if --repair fixed it), 4 means migrations are pending. Every run is
recorded in the verifications history table.

```
driftless verify [flags]
```

### Options

```
      --format string   output format: table|json (default "table")
      --full            exhaustive walk comparing every object (default)
  -h, --help            help for verify
      --quick           recent-window walk plus random spot-checks per type
      --repair          re-fetch and fix drifted objects
      --since string    only objects created on or after this date (2024-01-31 or RFC3339)
      --type strings    subset of object types, comma separated
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

