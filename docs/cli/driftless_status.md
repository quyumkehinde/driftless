## driftless status

Show sync health at a glance: events, queue, sweeps, backfill, drift

### Synopsis

Status prints a one-screen summary of the mirror: event totals and
freshness, queue depth by state, apply lag, the last sweep and any gaps it
found, the latest backfill run, the latest verification, and stored event
types that map to no known object.

```
driftless status [flags]
```

### Options

```
  -h, --help   help for status
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

