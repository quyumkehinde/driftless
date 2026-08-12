package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationParsing(t *testing.T) {
	valid := map[string]time.Duration{
		"300s":  300 * time.Second,
		"5m":    5 * time.Minute,
		"24h":   24 * time.Hour,
		"1h30m": 90 * time.Minute,
	}
	for in, want := range valid {
		var d Duration
		if err := yaml.Unmarshal([]byte(in), &d); err != nil {
			t.Errorf("unmarshal %q: %v", in, err)
		} else if d.Std() != want {
			t.Errorf("unmarshal %q = %v, want %v", in, d.Std(), want)
		}
	}
	for _, in := range []string{"nope", "5 minutes", "12"} {
		var d Duration
		if err := yaml.Unmarshal([]byte(in), &d); err == nil {
			t.Errorf("unmarshal %q should fail", in)
		}
	}
}
