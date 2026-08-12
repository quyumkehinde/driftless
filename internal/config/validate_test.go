package config

import (
	"errors"
	"strings"
	"testing"
)

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
