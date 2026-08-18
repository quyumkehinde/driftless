// Package config loads, validates, redacts, and prints the driftless
// configuration: defaults, then a YAML file with ${VAR} interpolation,
// then DRIFTLESS_* environment overrides.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// Config is the full configuration tree. Start from Defaults; zero values
// are not usable directly.
type Config struct {
	DatabaseURL string          `yaml:"database_url"`
	Stripe      StripeConfig    `yaml:"stripe"`
	Server      ServerConfig    `yaml:"server"`
	Workers     WorkersConfig   `yaml:"workers"`
	Sweep       SweepConfig     `yaml:"sweep"`
	Apply       ApplyConfig     `yaml:"apply"`
	Backfill    BackfillConfig  `yaml:"backfill"`
	Verify      VerifyConfig    `yaml:"verify"`
	Retention   RetentionConfig `yaml:"retention"`
	Log         LogConfig       `yaml:"log"`
}

// StripeConfig holds credentials and client behavior for the Stripe API.
type StripeConfig struct {
	APIKey                 string   `yaml:"api_key"`
	WebhookSecret          string   `yaml:"webhook_secret"`
	WebhookSecretSecondary string   `yaml:"webhook_secret_secondary"`
	APIBaseURL             string   `yaml:"api_base_url"`
	APIRPS                 int      `yaml:"api_rps"`
	SignatureTolerance     Duration `yaml:"signature_tolerance"`
}

// ServerConfig holds the listen addresses for ingest and metrics.
type ServerConfig struct {
	Listen        string `yaml:"listen"`
	MetricsListen string `yaml:"metrics_listen"`
	EnablePprof   bool   `yaml:"enable_pprof"`
}

// WorkersConfig holds the job worker pool settings.
type WorkersConfig struct {
	Count             int      `yaml:"count"`
	VisibilityTimeout Duration `yaml:"visibility_timeout"`
	MaxAttempts       int      `yaml:"max_attempts"`
}

// SweepConfig holds the gap sweeper schedule.
type SweepConfig struct {
	Interval         Duration `yaml:"interval"`
	Overlap          Duration `yaml:"overlap"`
	FirstRunLookback Duration `yaml:"first_run_lookback"`
}

// ApplyConfig holds apply-engine behavior.
type ApplyConfig struct {
	PayloadModeTypes []string `yaml:"payload_mode_types"`
}

// BackfillConfig holds backfill behavior.
type BackfillConfig struct {
	AutoResume bool `yaml:"auto_resume"`
}

// VerifyConfig holds scheduled verification settings.
type VerifyConfig struct {
	Auto     bool   `yaml:"auto"`
	AutoTime string `yaml:"auto_time"`
}

// RetentionConfig holds data retention settings.
type RetentionConfig struct {
	EventsDays int `yaml:"events_days"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Defaults returns the documented default for every key.
func Defaults() Config {
	return Config{
		Stripe: StripeConfig{
			APIBaseURL:         stripeapi.DefaultBaseURL,
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
		Apply: ApplyConfig{
			PayloadModeTypes: []string{},
		},
		Backfill: BackfillConfig{
			AutoResume: true,
		},
		Verify: VerifyConfig{
			AutoTime: "03:00",
		},
		Retention: RetentionConfig{
			EventsDays: 90,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// DefaultPaths are probed in order when no explicit config path is given.
var DefaultPaths = []string{"driftless.yaml", "/etc/driftless/driftless.yaml"}

// ResolvePath picks the config file to load. It returns the explicit path
// when one was given, otherwise the first default path that exists. An empty
// path means no file: defaults plus environment only.
func ResolvePath(explicit string) (path string, wasExplicit bool) {
	if explicit != "" {
		return explicit, true
	}
	for _, p := range DefaultPaths {
		if _, err := os.Stat(p); err == nil {
			return p, false
		}
	}
	return "", false
}

// Load builds the effective config: defaults, then the file at path (if any)
// with ${VAR} interpolation applied, then environment overrides. A missing
// file is an error only when its path was explicitly given. Errors are
// ValidationErrors and carry exit code 2.
func Load(path string, explicit bool) (*Config, error) {
	cfg := Defaults()
	if path != "" {
		raw, err := os.ReadFile(path)
		switch {
		case err != nil && explicit:
			return nil, ValidationErrors{fmt.Errorf("config file: %v", err)}
		case err == nil:
			dec := yaml.NewDecoder(bytes.NewReader(interpolate(raw)))
			// Unknown keys are almost always typos; refuse them.
			dec.KnownFields(true)
			if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
				return nil, ValidationErrors{fmt.Errorf("config file %s: %v", path, err)}
			}
		}
	}
	if errs := applyEnv(&cfg); len(errs) > 0 {
		return nil, ValidationErrors(errs)
	}
	return &cfg, nil
}
