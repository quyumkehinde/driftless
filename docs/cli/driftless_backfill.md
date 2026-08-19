## driftless backfill

Import history from the Stripe API into the mirror

### Synopsis

Backfill walks the Stripe list APIs and imports history into the mirror,
newest first, resumable across restarts and crashes. Progress is tracked
per object type; an interrupted run picks up at the last committed page.

Without flags it imports full history. Combine with --since for a bounded
import, --type for a subset, or --dry-run to see the plan.

```
driftless backfill [flags]
```

### Options

```
      --dry-run        print the plan without fetching
      --full           all history (default when no --since)
  -h, --help           help for backfill
      --resume int     resume an interrupted run by id
      --since string   only objects created on or after this date (2024-01-31 or RFC3339)
      --type strings   subset of object types, comma separated
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

