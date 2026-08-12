// Package obs provides the observability building blocks: slog setup, the
// prometheus registry, health handlers, and the metrics listener. Nothing
// here starts on its own; serve wires it together.
package obs

import (
	"fmt"
	"io"
	"log/slog"
)

// NewLogger builds a slog.Logger writing to w. format is "json" or "text";
// level is one of debug, info, warn, error.
func NewLogger(w io.Writer, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid log level %q", level)
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("invalid log format %q", format)
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
