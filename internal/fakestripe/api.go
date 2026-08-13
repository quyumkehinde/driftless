package fakestripe

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// pluralPaths maps URL segments to object types. Checkout sessions live
// under a two-segment path and are routed explicitly.
var pluralPaths = map[string]string{
	"customers":          "customer",
	"subscriptions":      "subscription",
	"subscription_items": "subscription_item",
	"products":           "product",
	"prices":             "price",
	"invoices":           "invoice",
	"charges":            "charge",
	"payment_intents":    "payment_intent",
	"payment_methods":    "payment_method",
	"setup_intents":      "setup_intent",
	"refunds":            "refund",
	"disputes":           "dispute",
}

const defaultPageLimit = 10

func (s *Server) apiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/account", s.handleAccount)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/checkout/sessions", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, "checkout_session")
	})
	mux.HandleFunc("GET /v1/checkout/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.handleGet(w, "checkout_session", r.PathValue("id"))
	})
	mux.HandleFunc("GET /v1/{plural}", func(w http.ResponseWriter, r *http.Request) {
		objectType, ok := pluralPaths[r.PathValue("plural")]
		if !ok {
			writeError(w, http.StatusNotFound, "unknown resource")
			return
		}
		s.handleList(w, r, objectType)
	})
	mux.HandleFunc("GET /v1/{plural}/{id}", func(w http.ResponseWriter, r *http.Request) {
		objectType, ok := pluralPaths[r.PathValue("plural")]
		if !ok {
			writeError(w, http.StatusNotFound, "unknown resource")
			return
		}
		s.handleGet(w, objectType, r.PathValue("id"))
	})
	return mux
}

func (s *Server) handleAccount(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"id": AccountID, "object": "account", "livemode": false})
}

func (s *Server) handleGet(w http.ResponseWriter, objectType, id string) {
	obj, ok := s.Object(objectType, id)
	if !ok {
		writeError(w, http.StatusNotFound, "resource_missing")
		return
	}
	writeJSON(w, obj)
}

// handleList serves cursor pagination the way Stripe does: insertion order
// reversed (newest first), limit, starting_after.
func (s *Server) handleList(w http.ResponseWriter, r *http.Request, objectType string) {
	s.mu.Lock()
	ids := slices.Clone(s.order[objectType])
	objs := make([]map[string]any, 0, len(ids))
	slices.Reverse(ids)
	for _, id := range ids {
		objs = append(objs, s.objects[objectType][id])
	}
	s.mu.Unlock()

	page, hasMore := paginate(objs, r, func(o map[string]any) string { return o["id"].(string) })
	writeJSON(w, map[string]any{
		"object":   "list",
		"url":      r.URL.Path,
		"has_more": hasMore,
		"data":     page,
	})
}

// handleEvents serves the event log newest first with created filters.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	gte, lte := createdBounds(r)

	s.mu.Lock()
	events := make([]json.RawMessage, 0, len(s.events))
	for i := len(s.events) - 1; i >= 0; i-- {
		e := s.events[i]
		if !gte.IsZero() && e.Created.Before(gte) {
			continue
		}
		if !lte.IsZero() && e.Created.After(lte) {
			continue
		}
		events = append(events, json.RawMessage(e.Payload))
	}
	s.mu.Unlock()

	page, hasMore := paginate(events, r, func(raw json.RawMessage) string {
		var envelope struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &envelope)
		return envelope.ID
	})
	writeJSON(w, map[string]any{
		"object":   "list",
		"url":      "/v1/events",
		"has_more": hasMore,
		"data":     page,
	})
}

// paginate applies starting_after and limit to an already-ordered slice.
func paginate[T any](items []T, r *http.Request, idOf func(T) string) (page []T, hasMore bool) {
	start := 0
	if after := r.URL.Query().Get("starting_after"); after != "" {
		for i, item := range items {
			if idOf(item) == after {
				start = i + 1
				break
			}
		}
	}
	limit := defaultPageLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	end := min(start+limit, len(items))
	page = items[start:end]
	if page == nil {
		page = []T{}
	}
	return page, end < len(items)
}

func createdBounds(r *http.Request) (gte, lte time.Time) {
	if raw := r.URL.Query().Get("created[gte]"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			gte = time.Unix(n, 0).UTC()
		}
	}
	if raw := r.URL.Query().Get("created[lte]"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			lte = time.Unix(n, 0).UTC()
		}
	}
	return gte, lte
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"type": "invalid_request_error", "code": code},
	})
}
