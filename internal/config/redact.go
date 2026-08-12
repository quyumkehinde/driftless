package config

import (
	"net/url"
	"regexp"
	"strings"
)

const redactedMarker = "***REDACTED***"

// secretPrefixPattern captures the recognizable key prefix, like sk_live_,
// rk_test_, or whsec_, so redacted output stays diagnosable.
var secretPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9]+_(?:live_|test_)?`)

// Redacted returns a copy of the config safe to print or log: secret values
// keep their prefix and lose the rest; the database URL loses its password.
func (c Config) Redacted() Config {
	c.Stripe.APIKey = redactSecret(c.Stripe.APIKey)
	c.Stripe.WebhookSecret = redactSecret(c.Stripe.WebhookSecret)
	c.Stripe.WebhookSecretSecondary = redactSecret(c.Stripe.WebhookSecretSecondary)
	c.DatabaseURL = redactDatabaseURL(c.DatabaseURL)
	return c
}

func redactSecret(s string) string {
	if s == "" {
		return ""
	}
	if m := secretPrefixPattern.FindString(s); m != "" && m != s {
		return m + redactedMarker
	}
	return redactedMarker
}

// redactDatabaseURL blanks only the password. If the URL does not parse it
// is redacted wholesale rather than risk leaking credentials.
func redactDatabaseURL(s string) string {
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil {
		return redactedMarker
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), redactedMarker)
		}
	}
	// The URL encoder percent-escapes the marker's asterisks; restore them
	// so the output stays recognizable.
	return strings.ReplaceAll(u.String(), url.QueryEscape(redactedMarker), redactedMarker)
}
