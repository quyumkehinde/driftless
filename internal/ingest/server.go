package ingest

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/crashpoint"
	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
)

// maxBodyBytes caps webhook request bodies. Stripe events are far smaller;
// the limit guards the server, not the protocol.
const maxBodyBytes = 1 << 20

// unhandledWarnInterval rate-limits the WARN log for unknown event types to
// once per type per hour; the metric counts every occurrence.
const unhandledWarnInterval = time.Hour

// Metrics holds the ingest prometheus instruments.
type Metrics struct {
	Events      *prometheus.CounterVec
	AckSeconds  prometheus.Histogram
	Unhandled   *prometheus.CounterVec
	DeliveryLag prometheus.Histogram
}

// NewMetrics registers the ingest metric families on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_ingest_events_total",
			Help: "Webhook deliveries by outcome.",
		}, []string{"result"}),
		AckSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "driftless_ingest_ack_seconds",
			Help:    "Time from request start to acknowledgment.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		Unhandled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "driftless_events_unhandled_total",
			Help: "Stored events whose type maps to no object.",
		}, []string{"type"}),
		// The is-Stripe-delivery-healthy signal: event creation to webhook
		// arrival, so it excludes swept and backfilled events. Buckets span
		// seconds (normal) to Stripe's three-day retry window.
		DeliveryLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "driftless_delivery_lag_seconds",
			Help:    "Time from Stripe event creation to webhook receipt.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 18),
		}),
	}
	reg.MustRegister(m.Events, m.AckSeconds, m.Unhandled, m.DeliveryLag)
	return m
}

// Server receives Stripe webhooks: verify, record, enqueue, acknowledge.
type Server struct {
	pool     *pgxpool.Pool
	queue    *queue.Queue
	verifier *Verifier
	logger   *slog.Logger
	metrics  *Metrics

	mu                sync.Mutex
	lastUnhandledWarn map[string]time.Time
}

// NewServer wires the webhook receiver. metrics may be nil in tests.
func NewServer(pool *pgxpool.Pool, q *queue.Queue, verifier *Verifier, logger *slog.Logger, metrics *Metrics) *Server {
	return &Server{
		pool:              pool,
		queue:             q,
		verifier:          verifier,
		logger:            obs.WithComponent(logger, "ingest"),
		metrics:           metrics,
		lastUnhandledWarn: make(map[string]time.Time),
	}
}

// Handler returns the ingest listener's routes: the webhook endpoint and
// liveness. Readiness lives on the metrics listener, so load balancer
// health checks never point at the webhook path.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /webhooks/stripe", http.HandlerFunc(s.handleWebhook))
	mux.Handle("GET /healthz", obs.Healthz())
	return mux
}

// eventEnvelope is the subset of a Stripe event ingest needs.
type eventEnvelope struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	APIVersion string `json:"api_version"`
	Created    int64  `json:"created"`
	Livemode   bool   `json:"livemode"`
	Data       struct {
		Object struct {
			ID string `json:"id"`
		} `json:"object"`
	} `json:"data"`
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.AckSeconds.Observe(time.Since(start).Seconds())
		}
	}()

	body, err := readBody(w, r)
	if err != nil {
		s.count("error")
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}

	if err := s.verifier.Verify(r.Header.Get("Stripe-Signature"), body); err != nil {
		// Never log any part of the payload: it failed authentication.
		s.count("bad_signature")
		s.logger.Warn("webhook signature rejected", "reason", err.Error(), "remote_addr", r.RemoteAddr)
		http.Error(w, "invalid signature", http.StatusBadRequest)
		return
	}

	var event eventEnvelope
	if err := json.Unmarshal(body, &event); err != nil || event.ID == "" || event.Type == "" {
		s.count("error")
		s.logger.Warn("signed payload is not a usable event", "remote_addr", r.RemoteAddr)
		http.Error(w, "invalid event payload", http.StatusBadRequest)
		return
	}

	inserted, err := s.record(r, event, body)
	if err != nil {
		s.count("error")
		s.logger.Error("recording event failed", "event_id", event.ID, "error", err)
		http.Error(w, "storage failure", http.StatusInternalServerError)
		return
	}

	if inserted {
		s.count("inserted")
		if s.metrics != nil {
			s.metrics.DeliveryLag.Observe(time.Since(time.Unix(event.Created, 0)).Seconds())
		}
	} else {
		s.count("duplicate")
	}
	crashpoint.Maybe("ingest.after-commit")
	// The transaction is committed: acknowledging is now safe.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"received": true}`))
}

// record stores the event and enqueues its job in one transaction, so a
// crash can never acknowledge an event that has no job. inserted is false
// for duplicate deliveries.
func (s *Server) record(r *http.Request, event eventEnvelope, body []byte) (inserted bool, err error) {
	ctx := r.Context()
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		params := db.InsertEventParams{
			EventID:  event.ID,
			Type:     event.Type,
			Created:  time.Unix(event.Created, 0).UTC(),
			Source:   string(mirror.EventSourceWebhook),
			Payload:  body,
			Livemode: event.Livemode,
		}
		if event.APIVersion != "" {
			params.ApiVersion = &event.APIVersion
		}
		rows, err := db.New(tx).InsertEvent(ctx, params)
		if err != nil {
			return err
		}
		if rows == 0 {
			return nil // duplicate: nothing to enqueue
		}
		inserted = true

		objectType, known := apply.ResolveType(event.Type)
		if !known {
			s.noteUnhandled(event.Type)
			return nil // stored and counted, never silently dropped
		}
		if event.Data.Object.ID == "" {
			// A mapped event without an object id cannot be applied. Keep
			// the event row for forensics, count it as unhandled.
			s.noteUnhandled(event.Type)
			s.logger.Warn("mapped event has no data.object.id", "event_id", event.ID, "type", event.Type)
			return nil
		}
		if _, _, err := s.queue.Enqueue(ctx, tx, queue.EnqueueParams{
			ObjectType:   string(objectType),
			ObjectID:     event.Data.Object.ID,
			EventID:      event.ID,
			EventCreated: time.Unix(event.Created, 0).UTC(),
		}); err != nil {
			return err
		}
		crashpoint.Maybe("ingest.before-commit")
		return nil
	})
	return inserted, err
}

func (s *Server) noteUnhandled(eventType string) {
	if s.metrics != nil {
		s.metrics.Unhandled.WithLabelValues(eventType).Inc()
	}
	s.mu.Lock()
	last, seen := s.lastUnhandledWarn[eventType]
	warn := !seen || time.Since(last) >= unhandledWarnInterval
	if warn {
		s.lastUnhandledWarn[eventType] = time.Now()
	}
	s.mu.Unlock()
	if warn {
		s.logger.Warn("unhandled event type stored without processing", "type", eventType)
	}
}

func (s *Server) count(result string) {
	if s.metrics != nil {
		s.metrics.Events.WithLabelValues(result).Inc()
	}
}

// readBody reads at most maxBodyBytes of the request body.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	limited := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = limited.Close() }()
	return io.ReadAll(limited)
}
