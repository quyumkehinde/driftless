package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "driftless.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaults(t *testing.T) {
	got := Defaults()
	want := Config{
		Stripe: StripeConfig{
			APIBaseURL:         "https://api.stripe.com",
			APIRPS:             25,
			SignatureTolerance: Duration(300 * time.Second),
		},
		Server: ServerConfig{
			Listen:        ":8724",
			MetricsListen: ":8725",
		},
		Workers: WorkersConfig{
			Count:             8,
			VisibilityTimeout: Duration(300 * time.Second),
			MaxAttempts:       8,
		},
		Sweep: SweepConfig{
			Interval:         Duration(5 * time.Minute),
			Overlap:          Duration(10 * time.Minute),
			FirstRunLookback: Duration(24 * time.Hour),
		},
		Apply:     ApplyConfig{PayloadModeTypes: []string{}},
		Backfill:  BackfillConfig{AutoResume: true},
		Verify:    VerifyConfig{AutoTime: "03:00"},
		Retention: RetentionConfig{EventsDays: 90},
		Log:       LogConfig{Level: "info", Format: "json"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Defaults() = %+v, want %+v", got, want)
	}
}

func TestLoadFileOverridesDefaults(t *testing.T) {
	path := writeConfig(t, `
database_url: postgres://localhost/driftless
stripe:
  api_key: rk_test_abc
  api_rps: 50
workers:
  count: 16
sweep:
  interval: 10m
log:
  format: text
`)
	cfg, err := Load(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://localhost/driftless" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Stripe.APIRPS != 50 {
		t.Errorf("Stripe.APIRPS = %d, want 50", cfg.Stripe.APIRPS)
	}
	if cfg.Workers.Count != 16 {
		t.Errorf("Workers.Count = %d, want 16", cfg.Workers.Count)
	}
	if cfg.Sweep.Interval.Std() != 10*time.Minute {
		t.Errorf("Sweep.Interval = %v, want 10m", cfg.Sweep.Interval)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("Log.Format = %q, want text", cfg.Log.Format)
	}
	// untouched keys keep their defaults
	if cfg.Workers.MaxAttempts != 8 {
		t.Errorf("Workers.MaxAttempts = %d, want default 8", cfg.Workers.MaxAttempts)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want default info", cfg.Log.Level)
	}
}

func TestLoadUnknownKeyRejected(t *testing.T) {
	path := writeConfig(t, "databse_url: oops\n")
	_, err := Load(path, true)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	var verrs ValidationErrors
	if !errors.As(err, &verrs) {
		t.Errorf("error type %T, want ValidationErrors", err)
	}
}

func TestLoadExplicitMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"), true)
	if err == nil {
		t.Fatal("expected error for missing explicit config file")
	}
	var ec interface{ ExitCode() int }
	if !errors.As(err, &ec) || ec.ExitCode() != 2 {
		t.Errorf("expected exit code 2 error, got %v", err)
	}
}

func TestLoadNoFileUsesDefaultsAndEnv(t *testing.T) {
	t.Setenv("DRIFTLESS_DATABASE_URL", "postgres://envhost/db")
	cfg, err := Load("", false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://envhost/db" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Workers.Count != 8 {
		t.Errorf("Workers.Count = %d, want default 8", cfg.Workers.Count)
	}
}

func TestResolvePath(t *testing.T) {
	if p, explicit := ResolvePath("/some/path.yaml"); p != "/some/path.yaml" || !explicit {
		t.Errorf("explicit path not honored: %q %v", p, explicit)
	}

	dir := t.TempDir()
	t.Chdir(dir)
	if p, _ := ResolvePath(""); p != "" {
		t.Errorf("no config anywhere should resolve empty, got %q", p)
	}
	if err := os.WriteFile(filepath.Join(dir, "driftless.yaml"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if p, explicit := ResolvePath(""); p != "driftless.yaml" || explicit {
		t.Errorf("cwd config not found: %q %v", p, explicit)
	}
}
