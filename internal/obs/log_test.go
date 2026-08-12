package obs

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger(t *testing.T) {
	for _, tc := range []struct {
		level, format string
		wantErr       bool
	}{
		{"debug", "json", false},
		{"info", "json", false},
		{"warn", "text", false},
		{"error", "text", false},
		{"loud", "json", true},
		{"info", "xml", true},
	} {
		_, err := NewLogger(&strings.Builder{}, tc.level, tc.format)
		if (err != nil) != tc.wantErr {
			t.Errorf("NewLogger(%q, %q) error = %v, wantErr %v", tc.level, tc.format, err, tc.wantErr)
		}
	}
}

func TestLoggerLevelFiltersAndJSON(t *testing.T) {
	var buf strings.Builder
	logger, err := NewLogger(&buf, "warn", "json")
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Warn("shown")
	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Error("info line should be filtered at warn level")
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(out), &line); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	if line["msg"] != "shown" {
		t.Errorf("msg = %v", line["msg"])
	}
}

func TestWithComponentAndCritical(t *testing.T) {
	var buf strings.Builder
	logger, err := NewLogger(&buf, "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	Critical(WithComponent(logger, "sweeper"), "no events arriving", "window", "24h")

	var line map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &line); err != nil {
		t.Fatal(err)
	}
	if line["component"] != "sweeper" {
		t.Errorf("component = %v", line["component"])
	}
	if line["critical"] != true {
		t.Errorf("critical = %v", line["critical"])
	}
	if line["level"] != "ERROR" {
		t.Errorf("level = %v", line["level"])
	}
	if line["window"] != "24h" {
		t.Errorf("window = %v", line["window"])
	}
}
