package config

import (
	"os"
	"regexp"
)

var interpolationPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// interpolate replaces every ${VAR} in raw with the value of the environment
// variable VAR. Unset variables become empty strings so that required-field
// validation reports them instead of a cryptic parse failure. A bare $ not
// followed by {NAME} is left untouched.
func interpolate(raw []byte) []byte {
	return interpolationPattern.ReplaceAllFunc(raw, func(m []byte) []byte {
		name := interpolationPattern.FindSubmatch(m)[1]
		return []byte(os.Getenv(string(name)))
	})
}
