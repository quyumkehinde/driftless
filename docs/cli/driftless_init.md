## driftless init

First-run setup: check the environment, migrate, offer a backfill

### Synopsis

Init is the interactive first run: it checks the database and the Stripe
key, applies pending migrations, and offers to start a full backfill.
Safe to re-run at any time.

```
driftless init [flags]
```

### Options

```
  -h, --help   help for init
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

