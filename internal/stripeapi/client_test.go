package stripeapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	l := NewLimiter(1000)
	t.Cleanup(l.Stop)
	c := New(srv.URL, "rk_test_client", l, nil)
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return c
}

func TestGetObjectSendsPinnedVersion(t *testing.T) {
	var gotAuth, gotVersion, gotPath string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotVersion = r.Header.Get("Stripe-Version")
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"id":"cus_1","object":"customer","email":"a@b.c"}`))
	}))

	raw, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_1")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"id":"cus_1","object":"customer","email":"a@b.c"}` {
		t.Errorf("raw = %s: bytes must round-trip untouched", raw)
	}
	if gotAuth != "Bearer rk_test_client" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotVersion != StripeVersion {
		t.Errorf("Stripe-Version = %q, want the pin %q", gotVersion, StripeVersion)
	}
	if gotPath != "/v1/customers/cus_1" {
		t.Errorf("path = %q", gotPath)
	}
}

func TestCheckoutSessionPath(t *testing.T) {
	var gotPath string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"id":"cs_1"}`))
	}))
	if _, err := c.GetObject(context.Background(), PriorityWebhook, "checkout_session", "cs_1"); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/checkout/sessions/cs_1" {
		t.Errorf("path = %q", gotPath)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","code":"resource_missing"}}`))
	}))

	_, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_gone")
	var notFound *NotFoundError
	if !errors.As(err, &notFound) {
		t.Errorf("err = %v, want NotFoundError", err)
	}
}

func TestRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls atomic.Int64
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"id":"cus_1"}`))
	}))

	if _, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_1"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3 (two 500s, one success)", calls.Load())
	}
}

func TestNoRetryOnOther4xx(t *testing.T) {
	var calls atomic.Int64
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"api_key_expired"}}`))
	}))

	_, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden || apiErr.Code != "api_key_expired" {
		t.Errorf("err = %v, want 403 api_key_expired", err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want exactly 1: 4xx must not retry", calls.Load())
	}
}

func TestRetriesExhausted(t *testing.T) {
	var calls atomic.Int64
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))

	_, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_1")
	if err == nil {
		t.Fatal("expected failure after exhausted retries")
	}
	if calls.Load() != maxAttempts {
		t.Errorf("calls = %d, want %d", calls.Load(), maxAttempts)
	}
}

func Test429HalvesEffectiveRate(t *testing.T) {
	var calls atomic.Int64
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"cus_1"}`))
	}))

	if _, err := c.GetObject(context.Background(), PriorityWebhook, "customer", "cus_1"); err != nil {
		t.Fatal(err)
	}
	if got := c.limiter.EffectiveRPS(); got != 500 {
		t.Errorf("effective rate = %v, want 500 (half of 1000)", got)
	}
}

func TestListDecodesPage(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("subscription") != "sub_1" {
			t.Errorf("query = %v", r.URL.Query())
		}
		_, _ = w.Write([]byte(`{"object":"list","has_more":true,"data":[{"id":"si_1"},{"id":"si_2"}]}`))
	}))

	page, err := c.List(context.Background(), PriorityWebhook, "/v1/subscription_items",
		map[string][]string{"subscription": {"sub_1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 2 || !page.HasMore {
		t.Errorf("page = %+v", page)
	}
}

func TestUnknownObjectType(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if _, err := c.GetObject(context.Background(), PriorityWebhook, "plan", "plan_1"); err == nil {
		t.Error("unknown object type must error")
	}
}
