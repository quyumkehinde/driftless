package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that reads and writes YAML in the compact
// go duration syntax: 300s, 5m, 24h.
type Duration time.Duration

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string {
	v := time.Duration(d)
	switch {
	case v == 0:
		return "0s"
	case v%time.Hour == 0:
		return fmt.Sprintf("%dh", v/time.Hour)
	case v%time.Minute == 0:
		return fmt.Sprintf("%dm", v/time.Minute)
	case v%time.Second == 0:
		return fmt.Sprintf("%ds", v/time.Second)
	default:
		return v.String()
	}
}

// MarshalYAML renders the duration compactly.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// UnmarshalYAML parses a scalar like "300s", "5m", or "24h".
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected a duration string, got %s", node.Tag)
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q", node.Value)
	}
	*d = Duration(parsed)
	return nil
}

func parseDuration(s string) (Duration, error) {
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	return Duration(parsed), nil
}
