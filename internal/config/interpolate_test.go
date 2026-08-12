package config

import (
	"os"
	"testing"
)

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
