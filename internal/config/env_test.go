package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

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
