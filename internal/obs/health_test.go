package obs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	Healthz().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	pass := Check{Name: "postgres", Run: func(context.Context) error { return nil }}
	fail := Check{Name: "migrations", Run: func(context.Context) error { return errors.New("2 pending") }}

	for _, tc := range []struct {
		name     string
		checks   []Check
		wantCode int
		wantBody string
	}{
		{"no checks", nil, http.StatusOK, "ready"},
		{"all passing", []Check{pass}, http.StatusOK, "ready"},
		{"one failing", []Check{pass, fail}, http.StatusServiceUnavailable, "migrations: 2 pending"},
		{"all failing", []Check{fail}, http.StatusServiceUnavailable, "migrations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Readyz(tc.checks...).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tc.wantCode {
				t.Errorf("readyz = %d, want %d", rec.Code, tc.wantCode)
			}
			if !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("body %q does not contain %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}
