package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// envEntry maps one DRIFTLESS_* variable to the field it overrides. The
// table is explicit rather than derived by splitting the variable name,
// because key names themselves contain underscores.
type envEntry struct {
	name string
	set  func(c *Config, v string) error
}

var envTable = []envEntry{
	{"DRIFTLESS_DATABASE_URL", setString(func(c *Config) *string { return &c.DatabaseURL })},

	{"DRIFTLESS_STRIPE_API_KEY", setString(func(c *Config) *string { return &c.Stripe.APIKey })},
	{"DRIFTLESS_STRIPE_WEBHOOK_SECRET", setString(func(c *Config) *string { return &c.Stripe.WebhookSecret })},
	{"DRIFTLESS_STRIPE_WEBHOOK_SECRET_SECONDARY", setString(func(c *Config) *string { return &c.Stripe.WebhookSecretSecondary })},
	{"DRIFTLESS_STRIPE_API_BASE_URL", setString(func(c *Config) *string { return &c.Stripe.APIBaseURL })},
	{"DRIFTLESS_STRIPE_API_RPS", setInt(func(c *Config) *int { return &c.Stripe.APIRPS })},
	{"DRIFTLESS_STRIPE_SIGNATURE_TOLERANCE", setDuration(func(c *Config) *Duration { return &c.Stripe.SignatureTolerance })},

	{"DRIFTLESS_SERVER_LISTEN", setString(func(c *Config) *string { return &c.Server.Listen })},
	{"DRIFTLESS_SERVER_METRICS_LISTEN", setString(func(c *Config) *string { return &c.Server.MetricsListen })},
	{"DRIFTLESS_SERVER_ENABLE_PPROF", setBool(func(c *Config) *bool { return &c.Server.EnablePprof })},

	{"DRIFTLESS_WORKERS_COUNT", setInt(func(c *Config) *int { return &c.Workers.Count })},
	{"DRIFTLESS_WORKERS_VISIBILITY_TIMEOUT", setDuration(func(c *Config) *Duration { return &c.Workers.VisibilityTimeout })},
	{"DRIFTLESS_WORKERS_MAX_ATTEMPTS", setInt(func(c *Config) *int { return &c.Workers.MaxAttempts })},

	{"DRIFTLESS_SWEEP_INTERVAL", setDuration(func(c *Config) *Duration { return &c.Sweep.Interval })},
	{"DRIFTLESS_SWEEP_OVERLAP", setDuration(func(c *Config) *Duration { return &c.Sweep.Overlap })},
	{"DRIFTLESS_SWEEP_FIRST_RUN_LOOKBACK", setDuration(func(c *Config) *Duration { return &c.Sweep.FirstRunLookback })},

	{"DRIFTLESS_APPLY_PAYLOAD_MODE_TYPES", setStringList(func(c *Config) *[]string { return &c.Apply.PayloadModeTypes })},

	{"DRIFTLESS_BACKFILL_AUTO_RESUME", setBool(func(c *Config) *bool { return &c.Backfill.AutoResume })},

	{"DRIFTLESS_VERIFY_AUTO", setBool(func(c *Config) *bool { return &c.Verify.Auto })},
	{"DRIFTLESS_VERIFY_AUTO_TIME", setString(func(c *Config) *string { return &c.Verify.AutoTime })},

	{"DRIFTLESS_RETENTION_EVENTS_DAYS", setInt(func(c *Config) *int { return &c.Retention.EventsDays })},

	{"DRIFTLESS_LOG_LEVEL", setString(func(c *Config) *string { return &c.Log.Level })},
	{"DRIFTLESS_LOG_FORMAT", setString(func(c *Config) *string { return &c.Log.Format })},
}

// applyEnv applies every set DRIFTLESS_* override to c, collecting parse
// failures instead of stopping at the first.
func applyEnv(c *Config) []error {
	var errs []error
	for _, e := range envTable {
		v, ok := os.LookupEnv(e.name)
		if !ok {
			continue
		}
		if err := e.set(c, v); err != nil {
			errs = append(errs, fmt.Errorf("%s: %v", e.name, err))
		}
	}
	return errs
}

func setString(field func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error {
		*field(c) = v
		return nil
	}
}

func setInt(field func(*Config) *int) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid integer %q", v)
		}
		*field(c) = n
		return nil
	}
}

func setBool(field func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid boolean %q", v)
		}
		*field(c) = b
		return nil
	}
}

func setDuration(field func(*Config) *Duration) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := parseDuration(v)
		if err != nil {
			return err
		}
		*field(c) = d
		return nil
	}
}

func setStringList(field func(*Config) *[]string) func(*Config, string) error {
	return func(c *Config, v string) error {
		items := []string{}
		for part := range strings.SplitSeq(v, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				items = append(items, trimmed)
			}
		}
		*field(c) = items
		return nil
	}
}
