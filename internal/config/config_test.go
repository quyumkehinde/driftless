package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
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
			APIRPS:             25,
			SignatureTolerance: Duration(300 * time.Second),
		},
		Server: ServerConfig{
			Listen:        ":8724",
			MetricsListen: ":8725",
		},
		Workers: WorkersConfig{
			Count:             8,
			VisibilityTimeout: Duration(120 * time.Second),
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

func TestEnvOverridesFile(t *testing.T) {
	path := writeConfig(t, `
database_url: postgres://filehost/db
stripe:
  api_key: rk_test_file
workers:
  count: 4
`)
	t.Setenv("DRIFTLESS_DATABASE_URL", "postgres://envhost/db")
	t.Setenv("DRIFTLESS_STRIPE_WEBHOOK_SECRET_SECONDARY", "whsec_second")
	t.Setenv("DRIFTLESS_WORKERS_COUNT", "32")
	t.Setenv("DRIFTLESS_SERVER_ENABLE_PPROF", "true")
	t.Setenv("DRIFTLESS_SWEEP_OVERLAP", "20m")
	t.Setenv("DRIFTLESS_APPLY_PAYLOAD_MODE_TYPES", "invoice, customer")
	t.Setenv("DRIFTLESS_BACKFILL_AUTO_RESUME", "false")
	t.Setenv("DRIFTLESS_VERIFY_AUTO_TIME", "04:30")
	t.Setenv("DRIFTLESS_RETENTION_EVENTS_DAYS", "0")
	t.Setenv("DRIFTLESS_LOG_LEVEL", "debug")

	cfg, err := Load(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://envhost/db" {
		t.Errorf("env should beat file: DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Stripe.APIKey != "rk_test_file" {
		t.Errorf("file value without env override lost: APIKey = %q", cfg.Stripe.APIKey)
	}
	if cfg.Stripe.WebhookSecretSecondary != "whsec_second" {
		t.Errorf("WebhookSecretSecondary = %q", cfg.Stripe.WebhookSecretSecondary)
	}
	if cfg.Workers.Count != 32 {
		t.Errorf("Workers.Count = %d, want 32", cfg.Workers.Count)
	}
	if !cfg.Server.EnablePprof {
		t.Error("Server.EnablePprof should be true")
	}
	if cfg.Sweep.Overlap.Std() != 20*time.Minute {
		t.Errorf("Sweep.Overlap = %v", cfg.Sweep.Overlap)
	}
	if want := []string{"invoice", "customer"}; !reflect.DeepEqual(cfg.Apply.PayloadModeTypes, want) {
		t.Errorf("PayloadModeTypes = %v, want %v", cfg.Apply.PayloadModeTypes, want)
	}
	if cfg.Backfill.AutoResume {
		t.Error("Backfill.AutoResume should be false")
	}
	if cfg.Verify.AutoTime != "04:30" {
		t.Errorf("Verify.AutoTime = %q", cfg.Verify.AutoTime)
	}
	if cfg.Retention.EventsDays != 0 {
		t.Errorf("Retention.EventsDays = %d, want 0", cfg.Retention.EventsDays)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q", cfg.Log.Level)
	}
}

func TestEnvParseErrorsAggregate(t *testing.T) {
	t.Setenv("DRIFTLESS_WORKERS_COUNT", "many")
	t.Setenv("DRIFTLESS_SWEEP_INTERVAL", "sometimes")
	_, err := Load("", false)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "DRIFTLESS_WORKERS_COUNT") || !strings.Contains(msg, "DRIFTLESS_SWEEP_INTERVAL") {
		t.Errorf("both env failures should be reported, got: %s", msg)
	}
}

func TestEnvTableCoversEveryField(t *testing.T) {
	expected := map[string]bool{}
	var walk func(t reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
			if tag == "" || tag == "-" {
				continue
			}
			name := prefix + "_" + strings.ToUpper(tag)
			if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(Duration(0)) {
				walk(f.Type, name)
				continue
			}
			expected[name] = true
		}
	}
	walk(reflect.TypeOf(Config{}), "DRIFTLESS")

	got := map[string]bool{}
	for _, e := range envTable {
		got[e.name] = true
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("env table mismatch:\n got: %v\nwant: %v", got, expected)
	}
}

func TestInterpolation(t *testing.T) {
	t.Setenv("TEST_DB_URL", "postgres://interp/db")
	_ = os.Unsetenv("TEST_UNSET_VAR")
	path := writeConfig(t, `
database_url: ${TEST_DB_URL}
stripe:
  api_key: ${TEST_UNSET_VAR}
  webhook_secret: "cost is $5 literal"
`)
	cfg, err := Load(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://interp/db" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Stripe.APIKey != "" {
		t.Errorf("unset var should interpolate to empty, got %q", cfg.Stripe.APIKey)
	}
	if cfg.Stripe.WebhookSecret != "cost is $5 literal" {
		t.Errorf("bare $ should be untouched, got %q", cfg.Stripe.WebhookSecret)
	}
}

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

func TestValidateAggregates(t *testing.T) {
	cfg := Defaults()
	cfg.Workers.Count = 0
	cfg.Stripe.APIRPS = 101
	cfg.Log.Level = "loud"
	// database_url and api_key also missing: five violations at once
	_, err := cfg.Validate(ScopeDefault)
	if err == nil {
		t.Fatal("expected validation errors")
	}
	var verrs ValidationErrors
	if !errors.As(err, &verrs) {
		t.Fatalf("error type %T", err)
	}
	if len(verrs) != 5 {
		t.Errorf("got %d violations, want 5: %v", len(verrs), err)
	}
	if verrs.ExitCode() != 2 {
		t.Errorf("ExitCode = %d, want 2", verrs.ExitCode())
	}
}

func TestValidateBounds(t *testing.T) {
	base := func() Config {
		cfg := Defaults()
		cfg.DatabaseURL = "postgres://localhost/db"
		cfg.Stripe.APIKey = "rk_test_x"
		return cfg
	}

	cfg := base()
	if _, err := cfg.Validate(ScopeDefault); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{"workers.count low", func(c *Config) { c.Workers.Count = 0 }},
		{"workers.count high", func(c *Config) { c.Workers.Count = 65 }},
		{"api_rps low", func(c *Config) { c.Stripe.APIRPS = 0 }},
		{"api_rps high", func(c *Config) { c.Stripe.APIRPS = 101 }},
		{"log.format", func(c *Config) { c.Log.Format = "xml" }},
	} {
		cfg := base()
		tc.mutate(&cfg)
		if _, err := cfg.Validate(ScopeDefault); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
}

func TestValidateServeScope(t *testing.T) {
	cfg := Defaults()
	cfg.DatabaseURL = "postgres://localhost/db"
	cfg.Stripe.APIKey = "rk_test_x"
	if _, err := cfg.Validate(ScopeDefault); err != nil {
		t.Errorf("webhook_secret should not be required outside serve: %v", err)
	}
	if _, err := cfg.Validate(ScopeServe); err == nil {
		t.Error("webhook_secret should be required for serve")
	}
	cfg.Stripe.WebhookSecret = "whsec_x"
	if _, err := cfg.Validate(ScopeServe); err != nil {
		t.Errorf("valid serve config rejected: %v", err)
	}
}

func TestSkLiveWarns(t *testing.T) {
	cfg := Defaults()
	cfg.DatabaseURL = "postgres://localhost/db"
	cfg.Stripe.APIKey = "sk_live_dangerous"
	warnings, err := cfg.Validate(ScopeDefault)
	if err != nil {
		t.Fatalf("sk_live must warn, not fail: %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "restricted") {
		t.Errorf("warnings = %v", warnings)
	}
}

func TestRedaction(t *testing.T) {
	cfg := Defaults()
	cfg.DatabaseURL = "postgres://driftless:hunter2@db.internal:5432/driftless"
	cfg.Stripe.APIKey = "sk_live_abcdef123456"
	cfg.Stripe.WebhookSecret = "whsec_supersecret"
	cfg.Stripe.WebhookSecretSecondary = "whsec_rotating"

	red := cfg.Redacted()
	if red.Stripe.APIKey != "sk_live_***REDACTED***" {
		t.Errorf("APIKey = %q", red.Stripe.APIKey)
	}
	if red.Stripe.WebhookSecret != "whsec_***REDACTED***" {
		t.Errorf("WebhookSecret = %q", red.Stripe.WebhookSecret)
	}
	if red.Stripe.WebhookSecretSecondary != "whsec_***REDACTED***" {
		t.Errorf("WebhookSecretSecondary = %q", red.Stripe.WebhookSecretSecondary)
	}
	if red.DatabaseURL != "postgres://driftless:***REDACTED***@db.internal:5432/driftless" {
		t.Errorf("DatabaseURL = %q", red.DatabaseURL)
	}
	// non-secrets untouched
	if red.Server.Listen != cfg.Server.Listen || red.Workers.Count != cfg.Workers.Count {
		t.Error("non-secret fields must not change")
	}
	// original untouched
	if cfg.Stripe.APIKey != "sk_live_abcdef123456" {
		t.Error("Redacted must not mutate the receiver")
	}

	// rk_test keys keep their prefix too
	cfg.Stripe.APIKey = "rk_test_xyz"
	if got := cfg.Redacted().Stripe.APIKey; got != "rk_test_***REDACTED***" {
		t.Errorf("rk_test redaction = %q", got)
	}

	// redacted output must round-trip as valid YAML
	out, err := yaml.Marshal(cfg.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	var back Config
	if err := yaml.Unmarshal(out, &back); err != nil {
		t.Errorf("redacted YAML does not round-trip: %v\n%s", err, out)
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
