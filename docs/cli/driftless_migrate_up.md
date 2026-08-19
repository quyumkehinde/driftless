## driftless migrate up

Apply pending migrations

### Synopsis

Migrations are embedded in the binary and applied forward-only. Up is
safe to run before swapping binaries during an upgrade.

```
driftless migrate up [flags]
```

### Options

```
  -h, --help   help for up
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless migrate](driftless_migrate.md)	 - Manage database schema migrations

