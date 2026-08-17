package fakestripe

import (
	"net/http"
	"testing"
	"time"
)

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestFailNextConsumesInOrder(t *testing.T) {
	s := New(t, "whsec_faults")
	s.Put("customer", "cus_1", nil, "customer.created")

	s.FailNext(2, http.StatusInternalServerError)
	for range 2 {
		if resp := get(t, s.URL()+"/v1/customers/cus_1"); resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d, want injected 500", resp.StatusCode)
		}
	}
	if resp := get(t, s.URL()+"/v1/customers/cus_1"); resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d after faults consumed, want 200", resp.StatusCode)
	}
}

func TestFailNext429CarriesRetryAfter(t *testing.T) {
	s := New(t, "whsec_faults")
	s.Put("customer", "cus_1", nil, "customer.created")

	s.FailNext(1, http.StatusTooManyRequests)
	resp := get(t, s.URL()+"/v1/customers/cus_1")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
}

func TestFailRate(t *testing.T) {
	s := New(t, "whsec_faults")
	s.Put("customer", "cus_1", nil, "customer.created")

	s.FailRate(1.0, http.StatusTooManyRequests)
	if resp := get(t, s.URL()+"/v1/customers/cus_1"); resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("rate 1.0: status = %d, want 429", resp.StatusCode)
	}
	s.FailRate(0, 0)
	if resp := get(t, s.URL()+"/v1/customers/cus_1"); resp.StatusCode != http.StatusOK {
		t.Errorf("rate 0: status = %d, want 200", resp.StatusCode)
	}
}

func TestLatency(t *testing.T) {
	s := New(t, "whsec_faults")
	s.Put("customer", "cus_1", nil, "customer.created")

	s.Latency(120 * time.Millisecond)
	start := time.Now()
	get(t, s.URL()+"/v1/customers/cus_1")
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("elapsed = %v, want at least the injected latency", elapsed)
	}
}

func TestForce404(t *testing.T) {
	s := New(t, "whsec_faults")
	s.Put("customer", "cus_gone", nil, "customer.created")

	s.Force404("cus_gone")
	if resp := get(t, s.URL()+"/v1/customers/cus_gone"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for deprecated id", resp.StatusCode)
	}
	// the object is still in the store and other objects are unaffected
	if _, ok := s.Object("customer", "cus_gone"); !ok {
		t.Error("deprecated object must stay in the store")
	}
}
