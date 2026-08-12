package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsServer(t *testing.T) {
	reg := NewRegistry()
	srv := NewMetricsServer(":0", reg, Readyz(), false)

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Error("/metrics missing go collector families")
	}

	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("pprof should be absent when disabled, got %d", rec.Code)
	}

	withPprof := NewMetricsServer(":0", reg, Readyz(), true)
	rec = httptest.NewRecorder()
	withPprof.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("pprof index = %d, want 200", rec.Code)
	}
}
