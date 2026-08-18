package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func newTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool, *Metrics) {
	t.Helper()
	pool := testpg.Start(t)
	q := queue.New(pool, 2*time.Minute, 8)
	verifier := NewVerifier(testSecret, "", tolerance)
	metrics := NewMetrics(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(NewServer(pool, q, verifier, logger, metrics).Handler())
	t.Cleanup(srv.Close)
	return srv, pool, metrics
}

func eventBody(id, eventType, objectID string, created time.Time) []byte {
	return fmt.Appendf(nil,
		`{"id":%q,"object":"event","api_version":"2026-01-01","created":%d,"livemode":false,"type":%q,"data":{"object":{"id":%q}}}`,
		id, created.Unix(), eventType, objectID)
}

func deliver(t *testing.T, url string, body []byte, header string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/webhooks/stripe", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if header != "" {
		req.Header.Set("Stripe-Signature", header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookInsertsAndEnqueues(t *testing.T) {
	srv, pool, metrics := newTestServer(t)
	ctx := context.Background()

	body := eventBody("evt_1", "customer.updated", "cus_1", time.Now())
	resp := deliver(t, srv.URL, body, signHeader(testSecret, time.Now(), body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	payload, _ := io.ReadAll(resp.Body)
	if string(payload) != `{"received": true}` {
		t.Errorf("body = %s", payload)
	}

	// the 200 arrived after commit: rows must already be visible
	var eventCount, jobCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM driftless.jobs WHERE object_type='customer' AND object_id='cus_1' AND status='pending'`).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 || jobCount != 1 {
		t.Errorf("events=%d jobs=%d, want 1 and 1", eventCount, jobCount)
	}
	var source string
	if err := pool.QueryRow(ctx, `SELECT source FROM driftless.events WHERE event_id='evt_1'`).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if source != "webhook" {
		t.Errorf("source = %q", source)
	}
	if got := testutil.ToFloat64(metrics.Events.WithLabelValues("inserted")); got != 1 {
		t.Errorf("inserted metric = %v", got)
	}
}

func TestWebhookDuplicateDelivery(t *testing.T) {
	srv, pool, metrics := newTestServer(t)
	ctx := context.Background()

	body := eventBody("evt_dup", "customer.updated", "cus_1", time.Now())
	for range 3 {
		resp := deliver(t, srv.URL, body, signHeader(testSecret, time.Now(), body))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 for duplicates too", resp.StatusCode)
		}
	}

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs`).Scan(&jobCount)
	if eventCount != 1 || jobCount != 1 {
		t.Errorf("events=%d jobs=%d, want 1 and 1", eventCount, jobCount)
	}
	if got := testutil.ToFloat64(metrics.Events.WithLabelValues("duplicate")); got != 2 {
		t.Errorf("duplicate metric = %v, want 2", got)
	}
}

func TestWebhookConcurrentIdenticalDeliveries(t *testing.T) {
	srv, pool, _ := newTestServer(t)
	ctx := context.Background()

	body := eventBody("evt_race", "customer.subscription.updated", "sub_1", time.Now())
	header := signHeader(testSecret, time.Now(), body)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhooks/stripe", bytes.NewReader(body))
			req.Header.Set("Stripe-Signature", header)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("status %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs`).Scan(&jobCount)
	if eventCount != 1 || jobCount != 1 {
		t.Errorf("events=%d jobs=%d, want exactly 1 and 1", eventCount, jobCount)
	}
}

func TestWebhookBadSignature(t *testing.T) {
	srv, pool, metrics := newTestServer(t)
	ctx := context.Background()

	body := eventBody("evt_bad", "customer.updated", "cus_1", time.Now())

	for _, header := range []string{
		"",
		"garbage",
		signHeader("whsec_wrong", time.Now(), body),
		signHeader(testSecret, time.Now().Add(-time.Hour), body),
	} {
		resp := deliver(t, srv.URL, body, header)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("header %q: status = %d, want 400", header, resp.StatusCode)
		}
	}

	var eventCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	if eventCount != 0 {
		t.Errorf("events = %d, want 0: unauthenticated payloads are never stored", eventCount)
	}
	if got := testutil.ToFloat64(metrics.Events.WithLabelValues("bad_signature")); got != 4 {
		t.Errorf("bad_signature metric = %v, want 4", got)
	}
}

func TestWebhookSignedGarbagePayload(t *testing.T) {
	srv, pool, _ := newTestServer(t)
	ctx := context.Background()

	for _, body := range [][]byte{
		[]byte(`{not json`),
		[]byte(`{"object":"event"}`),
		[]byte(`{"id":"evt_x"}`),
	} {
		resp := deliver(t, srv.URL, body, signHeader(testSecret, time.Now(), body))
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, resp.StatusCode)
		}
	}
	var eventCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	if eventCount != 0 {
		t.Errorf("events = %d, want 0", eventCount)
	}
}

func TestWebhookBodyTooLarge(t *testing.T) {
	srv, _, _ := newTestServer(t)

	big := []byte(`{"id":"evt_big","padding":"` + strings.Repeat("x", maxBodyBytes) + `"}`)
	resp := deliver(t, srv.URL, big, signHeader(testSecret, time.Now(), big))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

func TestWebhookUnknownEventType(t *testing.T) {
	srv, pool, metrics := newTestServer(t)
	ctx := context.Background()

	body := eventBody("evt_unknown", "plan.created", "plan_1", time.Now())
	resp := deliver(t, srv.URL, body, signHeader(testSecret, time.Now(), body))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: unknown types are stored, not rejected", resp.StatusCode)
	}

	var eventCount, jobCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.events`).Scan(&eventCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM driftless.jobs`).Scan(&jobCount)
	if eventCount != 1 {
		t.Errorf("events = %d, want 1: never silently dropped", eventCount)
	}
	if jobCount != 0 {
		t.Errorf("jobs = %d, want 0: nothing to apply", jobCount)
	}
	if got := testutil.ToFloat64(metrics.Unhandled.WithLabelValues("plan.created")); got != 1 {
		t.Errorf("unhandled metric = %v, want 1", got)
	}
}

func TestWebhookPostgresDown(t *testing.T) {
	srv, pool, _ := newTestServer(t)

	body := eventBody("evt_down", "customer.updated", "cus_1", time.Now())
	pool.Close()

	resp := deliver(t, srv.URL, body, signHeader(testSecret, time.Now(), body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 so Stripe retries", resp.StatusCode)
	}
}

func TestHealthzOnIngestListener(t *testing.T) {
	srv, _, _ := newTestServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d, want 200", resp.StatusCode)
	}
}
