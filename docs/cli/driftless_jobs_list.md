## driftless jobs list

List queue jobs by status

```
driftless jobs list [flags]
```

### Options

```
  -h, --help            help for list
      --limit int32     maximum rows to show (default 50)
      --status string   job status to list: pending|running|done|dead (default "dead")
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless jobs](driftless_jobs.md)	 - Inspect and retry queue jobs

