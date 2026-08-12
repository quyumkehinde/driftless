package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

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
