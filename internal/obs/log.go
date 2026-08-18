// Package obs provides the observability building blocks: slog setup, the
// prometheus registry, health handlers, and the metrics listener. Nothing
// here starts on its own; serve wires it together.
package obs

import (
	"fmt"
	"io"
	"log/slog"
)

// ParseLevel maps a config log level to its slog value; it is the single
// source of truth config validation checks against.
func ParseLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q (debug|info|warn|error)", level)
	}
}

// ValidFormat reports whether a log format is supported.
func ValidFormat(format string) bool {
	return format == "json" || format == "text"
}

// NewLogger builds a slog.Logger writing to w. format is "json" or "text";
// level is one of debug, info, warn, error.
func NewLogger(w io.Writer, level, format string) (*slog.Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("invalid log format %q (json|text)", format)
	}
	return slog.New(handler), nil
}

// WithComponent tags a logger for one component; every log line carries the
// component attribute.
func WithComponent(logger *slog.Logger, component string) *slog.Logger {
	return logger.With("component", component)
}

// Critical logs at ERROR with critical=true, the marker reserved for the
// self-diagnoses that should page an operator.
func Critical(logger *slog.Logger, msg string, args ...any) {
	logger.Error(msg, append([]any{"critical", true}, args...)...)
}
