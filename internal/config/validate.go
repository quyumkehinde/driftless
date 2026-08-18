package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/quyumkehinde/driftless/internal/obs"
)

// Scope selects which validation rules apply: serve needs more than the
// one-shot commands.
type Scope int

const (
	// ScopeDefault covers every command except serve.
	ScopeDefault Scope = iota
	// ScopeServe additionally requires the webhook secret.
	ScopeServe
)

// ValidationErrors aggregates every violation found in one pass. It carries
// exit code 2.
type ValidationErrors []error

func (v ValidationErrors) Error() string {
	msgs := make([]string, len(v))
	for i, err := range v {
		msgs[i] = "  - " + err.Error()
	}
	return "invalid configuration:\n" + strings.Join(msgs, "\n")
}

// ExitCode returns the exit code for invalid config or flags.
func (v ValidationErrors) ExitCode() int { return 2 }

// Unwrap exposes the individual violations to errors.Is and errors.As.
func (v ValidationErrors) Unwrap() []error { return v }

// Validate checks the whole config at once and returns every violation
// together, plus non-fatal warnings for the caller to surface.
func (c *Config) Validate(scope Scope) (warnings []string, err error) {
	var errs ValidationErrors

	if c.DatabaseURL == "" {
		errs = append(errs, fmt.Errorf("database_url is required"))
	}
	switch {
	case c.Stripe.APIKey == "":
		errs = append(errs, fmt.Errorf("stripe.api_key is required"))
	case strings.HasPrefix(c.Stripe.APIKey, "sk_live_"):
		warnings = append(warnings,
			"stripe.api_key is a full-access live key; prefer a restricted read-only key (rk_live_...)")
	}
	if scope == ScopeServe && c.Stripe.WebhookSecret == "" {
		errs = append(errs, fmt.Errorf("stripe.webhook_secret is required for serve"))
	}
	if c.Stripe.APIRPS < 1 || c.Stripe.APIRPS > 100 {
		errs = append(errs, fmt.Errorf("stripe.api_rps must be between 1 and 100, got %d", c.Stripe.APIRPS))
	}
	if c.Workers.Count < 1 || c.Workers.Count > 64 {
		errs = append(errs, fmt.Errorf("workers.count must be between 1 and 64, got %d", c.Workers.Count))
	}
	if c.Verify.Auto {
		if _, err := time.Parse("15:04", c.Verify.AutoTime); err != nil {
			errs = append(errs, fmt.Errorf("verify.auto_time must be HH:MM, got %q", c.Verify.AutoTime))
		}
	}
	if _, err := obs.ParseLevel(c.Log.Level); err != nil {
		errs = append(errs, err)
	}
	if !obs.ValidFormat(c.Log.Format) {
		errs = append(errs, fmt.Errorf("log.format must be one of json|text, got %q", c.Log.Format))
	}

	if len(errs) > 0 {
		return warnings, errs
	}
	return warnings, nil
}
