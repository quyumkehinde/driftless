// Package crashpoint kills the process at named instruction boundaries so
// chaos tests can simulate kill -9 at exact points. The hook only exists in
// builds made with -tags crashpoint; release builds compile it to nothing.
package crashpoint
