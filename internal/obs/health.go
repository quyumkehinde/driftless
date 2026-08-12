package obs

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Check reports whether one readiness dependency is healthy.
type Check struct {
	Name string
	Run  func(ctx context.Context) error
}

// Healthz returns the liveness handler: 200 once the process is serving.
// It belongs on the ingest listener, never behind the webhook path.
func Healthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
}

// Readyz returns the readiness handler: 200 iff every check passes, else 503
// listing the failing checks.
func Readyz(checks ...Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var failures []string
		for _, c := range checks {
			if err := c.Run(ctx); err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", c.Name, err))
			}
		}
		if len(failures) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintln(w, strings.Join(failures, "\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ready")
	})
}
