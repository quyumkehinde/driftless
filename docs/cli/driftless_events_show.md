## driftless events show

Dump one stored event: metadata and raw payload

### Synopsis

Prints the stored event's metadata (type, timestamps, source, processing
state) followed by its raw payload, pretty-printed. This is the intended
way to inspect payloads; log output never contains them.

```
driftless events show <event-id> [flags]
```

### Options

```
  -h, --help   help for show
```

### Options inherited from parent commands

```
      --config string       path to config file (default: ./driftless.yaml, then /etc/driftless/driftless.yaml)
      --log-format string   log format: json|text (overrides config)
      --log-level string    log level: debug|info|warn|error (overrides config)
```

### SEE ALSO

* [driftless events](driftless_events.md)	 - Inspect stored events

