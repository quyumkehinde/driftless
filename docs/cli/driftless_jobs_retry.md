## driftless jobs retry

Requeue dead jobs with a fresh attempt budget

### Synopsis

A job goes dead after exhausting its retry budget; its last error is
kept on the row. Retry resets the budget for one job by id, or for every
dead job with --all-dead.

```
driftless jobs retry [job-id] [flags]
```

### Options

```
      --all-dead   requeue every dead job
  -h, --help       help for retry
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless jobs](driftless_jobs.md)	 - Inspect and retry queue jobs

