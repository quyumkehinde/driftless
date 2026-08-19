## driftless doctor

Check the environment: database, migrations, Stripe key, webhook setup

### Synopsis

Doctor probes everything serve depends on and prints one line per check:
database connectivity, schema currency, whether the API key works and
which account it belongs to, agreement with the recorded account, webhook
secret presence, key scope, and whether webhooks are actually arriving.

Warnings exit 0; any hard failure exits 1.

```
driftless doctor [flags]
```

### Options

```
  -h, --help   help for doctor
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless](driftless.md)	 - Keep your Postgres in sync with Stripe

