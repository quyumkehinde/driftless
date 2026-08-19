// Package stripeapi is the only path to the Stripe API: a pinned-version,
// read-only client behind one shared rate limiter with priority tiers,
// AIMD backpressure, and bounded retries. No other package may talk to
// Stripe directly.
package stripeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// StripeVersion pins every API call. The stripe schema speaks this version
// regardless of what the customer's webhook endpoint is configured with,
// because fetch mode re-fetches with the pin. Provisional until the
// pre-release live-sandbox smoke validates it against real Stripe.
const StripeVersion = "2026-01-01"

// DefaultBaseURL is the real API; tests point at fakestripe instead.
const DefaultBaseURL = "https://api.stripe.com"

const (
	maxAttempts    = 5
	connectTimeout = 5 * time.Second
	totalTimeout   = 30 * time.Second
	retryBaseDelay = 500 * time.Millisecond
)

// NotFoundError reports a 404 or resource_missing: the object is gone,
// which for apply means soft-delete.
type NotFoundError struct {
	Path string
}

func (e *NotFoundError) Error() string { return fmt.Sprintf("stripe: %s not found", e.Path) }

// APIError is any other non-2xx outcome that survived retries.
type APIError struct {
	Status int
	Code   string
	Path   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("stripe: %s returned %d (%s)", e.Path, e.Status, e.Code)
}

// Metrics holds the client's prometheus instruments.
type Metrics struct {
	Requests       *prometheus.CounterVec
	RateLimited    prometheus.Counter
	RequestSeconds prometheus.Histogram
	EffectiveRPS   prometheus.GaugeFunc
}

// NewMetrics registers the stripe client metric families on reg.
func NewMetrics(reg *prometheus.Registry, limiter *Limiter) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_stripe_requests_total",
			Help: "Stripe API requests by priority and status code.",
		}, []string{"priority", "code"}),
		RateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driftless_stripe_rate_limited_total",
			Help: "429 responses from Stripe.",
		}),
		RequestSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "driftless_stripe_request_seconds",
			Help:    "Stripe API request latency.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}),
		EffectiveRPS: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "driftless_stripe_effective_rps",
			Help: "Current AIMD-adjusted request rate.",
		}, limiter.EffectiveRPS),
	}
	reg.MustRegister(m.Requests, m.RateLimited, m.RequestSeconds, m.EffectiveRPS)
	return m
}

// CollectionPath returns the API collection path for an object type, the
// single source every lister derives its endpoints from.
func CollectionPath(objectType string) (string, bool) {
	path, ok := objectPaths[objectType]
	return path, ok
}

// IsDeletionStub reports whether a fetched object is Stripe's deletion
// marker: deletable objects are served as 200 responses carrying
// deleted: true, not as 404s.
func IsDeletionStub(raw []byte) bool {
	var stub struct {
		Deleted bool `json:"deleted"`
	}
	if err := json.Unmarshal(raw, &stub); err != nil {
		return false
	}
	return stub.Deleted
}

// LastID extracts the final item's id from a page, for cursor advancement.
func LastID(page *ListPage) (string, error) {
	if len(page.Data) == 0 {
		return "", fmt.Errorf("stripe: empty page has no cursor")
	}
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(page.Data[len(page.Data)-1], &envelope); err != nil {
		return "", err
	}
	return envelope.ID, nil
}

// objectPaths maps object types to their API collection paths.
var objectPaths = map[string]string{
	ObjectCustomer:         "/v1/customers",
	ObjectSubscription:     "/v1/subscriptions",
	ObjectSubscriptionItem: "/v1/subscription_items",
	ObjectProduct:          "/v1/products",
	ObjectPrice:            "/v1/prices",
	ObjectInvoice:          "/v1/invoices",
	ObjectCharge:           "/v1/charges",
	ObjectPaymentIntent:    "/v1/payment_intents",
	ObjectPaymentMethod:    "/v1/payment_methods",
	ObjectSetupIntent:      "/v1/setup_intents",
	ObjectRefund:           "/v1/refunds",
	ObjectDispute:          "/v1/disputes",
	ObjectCheckoutSession:  "/v1/checkout/sessions",
}

// Client is the read-only Stripe client. All methods go through the shared
// limiter and return raw JSON so stored objects keep byte fidelity.
type Client struct {
	baseURL string
	apiKey  string
	limiter *Limiter
	http    *http.Client
	metrics *Metrics

	// sleep is replaceable so retry tests run instantly.
	sleep func(context.Context, time.Duration) error
}

// New builds a client. metrics may be nil.
func New(baseURL, apiKey string, limiter *Limiter, metrics *Metrics) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		limiter: limiter,
		metrics: metrics,
		http: &http.Client{
			Timeout: totalTimeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: connectTimeout}).DialContext,
			},
		},
		sleep: sleepCtx,
	}
}

// GetObject fetches one object by type and id, returning the raw JSON.
func (c *Client) GetObject(ctx context.Context, p Priority, objectType, id string) (json.RawMessage, error) {
	base, ok := objectPaths[objectType]
	if !ok {
		return nil, fmt.Errorf("stripe: unknown object type %q", objectType)
	}
	return c.get(ctx, p, base+"/"+url.PathEscape(id), nil)
}

// GetAccount fetches the account the API key belongs to, for identity
// checks in init, doctor, and the meta guard.
func (c *Client) GetAccount(ctx context.Context, p Priority) (json.RawMessage, error) {
	return c.get(ctx, p, "/v1/account", nil)
}

// ListPage is one page of a Stripe list response.
type ListPage struct {
	Data    []json.RawMessage `json:"data"`
	HasMore bool              `json:"has_more"`
}

// List fetches one page of a collection.
func (c *Client) List(ctx context.Context, p Priority, path string, query url.Values) (*ListPage, error) {
	raw, err := c.get(ctx, p, path, query)
	if err != nil {
		return nil, err
	}
	var page ListPage
	if err := json.Unmarshal(raw, &page); err != nil {
		return nil, fmt.Errorf("stripe: decode list %s: %w", path, err)
	}
	return &page, nil
}

// get performs a rate-limited GET with retries on 429, 5xx, and network
// errors. Other 4xx outcomes bubble immediately.
func (c *Client) get(ctx context.Context, p Priority, path string, query url.Values) (json.RawMessage, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.limiter.Acquire(ctx, p); err != nil {
			return nil, err
		}

		body, status, retryAfter, err := c.do(ctx, p, path, query)
		switch {
		case err != nil:
			lastErr = err // network class: retry
		case status == http.StatusOK:
			return body, nil
		case status == http.StatusNotFound:
			return nil, &NotFoundError{Path: path}
		case status == http.StatusTooManyRequests:
			if c.metrics != nil {
				c.metrics.RateLimited.Inc()
			}
			c.limiter.On429(retryAfter)
			lastErr = &APIError{Status: status, Code: "rate_limited", Path: path}
		case status >= 500:
			lastErr = &APIError{Status: status, Code: apiErrorCode(body), Path: path}
		default:
			return nil, &APIError{Status: status, Code: apiErrorCode(body), Path: path}
		}

		if attempt < maxAttempts {
			delay := retryBaseDelay << (attempt - 1)
			delay = time.Duration(float64(delay) * (0.5 + rand.Float64()))
			if retryAfter > delay {
				delay = retryAfter
			}
			if err := c.sleep(ctx, delay); err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("stripe: %s failed after %d attempts: %w", path, maxAttempts, lastErr)
}

func (c *Client) do(ctx context.Context, p Priority, path string, query url.Values) (body []byte, status int, retryAfter time.Duration, err error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Stripe-Version", StripeVersion)

	start := time.Now()
	resp, err := c.http.Do(req)
	if c.metrics != nil {
		c.metrics.RequestSeconds.Observe(time.Since(start).Seconds())
	}
	if err != nil {
		if c.metrics != nil {
			c.metrics.Requests.WithLabelValues(p.String(), "network_error").Inc()
		}
		return nil, 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if c.metrics != nil {
		c.metrics.Requests.WithLabelValues(p.String(), strconv.Itoa(resp.StatusCode)).Inc()
	}

	body, err = io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, 0, 0, err
	}
	if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
		retryAfter = time.Duration(seconds) * time.Second
	}
	return body, resp.StatusCode, retryAfter, nil
}

// apiErrorCode extracts error.code from a Stripe error body, best effort.
func apiErrorCode(body []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error.Code == "" {
		return "unknown"
	}
	return envelope.Error.Code
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
