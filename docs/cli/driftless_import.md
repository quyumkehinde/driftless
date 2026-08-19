## driftless import

Migrate rows from a stripe/sync-engine database into the mirror

### Synopsis

Import copies a stripe/sync-engine schema into the mirror, one table per
transaction, reconstructing each object from its typed columns. Existing
mirror rows are never overwritten. Imported objects are marked
import-sourced; run verify --repair afterward to re-fetch true state for
every one of them.

The sync-engine schema must be renamed away from stripe first, since the
mirror itself lives there:
  ALTER SCHEMA stripe RENAME TO sync_engine

```
driftless import [flags]
```

### Options

```
      --from-sync-engine   import from a stripe/sync-engine schema layout
  -h, --help               help for import
      --schema string      schema holding the sync-engine tables (default "sync_engine")
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

