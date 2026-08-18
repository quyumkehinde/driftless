package fakestripe

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// pluralPaths maps URL segments to object types, derived from the
// client's own collection table so the double's routes cannot drift from
// the paths production code requests. Checkout sessions live under a
// two-segment path and are routed explicitly.
var pluralPaths = func() map[string]string {
	paths := make(map[string]string, len(stripeapi.AllObjectTypes))
	for _, objectType := range stripeapi.AllObjectTypes {
		path, ok := stripeapi.CollectionPath(objectType)
		if !ok || objectType == stripeapi.ObjectCheckoutSession {
			continue
		}
		paths[strings.TrimPrefix(path, "/v1/")] = objectType
	}
	return paths
}()

const defaultPageLimit = 10

func (s *Server) apiHandler() http.Handler {
	mux := s.apiRoutes()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.interceptFault(w) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) apiRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/account", s.handleAccount)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/checkout/sessions", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, stripeapi.ObjectCheckoutSession)
	})
	mux.HandleFunc("GET /v1/checkout/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.handleGet(w, stripeapi.ObjectCheckoutSession, r.PathValue("id"))
	})
	mux.HandleFunc("GET /v1/customers/{id}/payment_methods", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, stripeapi.ObjectPaymentMethod, filter{"customer", r.PathValue("id")})
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
	if !ok || s.isForced404(id) {
		writeError(w, http.StatusNotFound, "resource_missing")
		return
	}
	writeJSON(w, obj)
}

// listFilterKeys are the query filters Stripe supports on the collections
// driftless lists; a set filter keeps objects whose field matches.
var listFilterKeys = []string{"subscription", "customer"}

// filter is one field constraint a route adds beyond the query string,
// like the customer id baked into a path-shaped endpoint.
type filter struct {
	key, value string
}

// handleList serves cursor pagination the way Stripe does: insertion order
// reversed (newest first), limit, starting_after, field filters, and
// created bounds.
func (s *Server) handleList(w http.ResponseWriter, r *http.Request, objectType string, extra ...filter) {
	gte, lte := createdBounds(r)
	filters := make(map[string]string, len(listFilterKeys)+len(extra))
	for _, key := range listFilterKeys {
		if want := r.URL.Query().Get(key); want != "" {
			filters[key] = want
		}
	}
	for _, f := range extra {
		filters[f.key] = f.value
	}

	s.mu.Lock()
	ids := slices.Clone(s.order[objectType])
	objs := make([]map[string]any, 0, len(ids))
	slices.Reverse(ids)
	for _, id := range ids {
		obj := s.objects[objectType][id]
		matched := true
		for key, want := range filters {
			if obj[key] != want {
				matched = false
				break
			}
		}
		if matched && (!gte.IsZero() || !lte.IsZero()) {
			created, ok := objCreated(obj)
			if ok {
				if !gte.IsZero() && created.Before(gte) {
					matched = false
				}
				if !lte.IsZero() && created.After(lte) {
					matched = false
				}
			}
		}
		// Stripe's subscriptions listing omits canceled subscriptions
		// unless status is given; status=all returns everything. The
		// double replicates the trap so backfill has to dodge it.
		if matched && objectType == stripeapi.ObjectSubscription {
			switch status := r.URL.Query().Get("status"); status {
			case "":
				matched = obj["status"] != "canceled"
			case "all":
			default:
				matched = obj["status"] == status
			}
		}
		if matched {
			objs = append(objs, obj)
		}
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

// matchesTypePatterns reports whether an event type matches any pattern;
// patterns are exact types or prefix wildcards like invoice.*, the shape
// the events API types filter accepts. No patterns means everything.
func matchesTypePatterns(eventType string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
			if strings.HasPrefix(eventType, prefix) {
				return true
			}
		} else if eventType == pattern {
			return true
		}
	}
	return false
}

// handleEvents serves the event log newest first with created and type
// filters.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	gte, lte := createdBounds(r)
	patterns := r.URL.Query()["types[]"]

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
		if !matchesTypePatterns(e.Type, patterns) {
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
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= stripeapi.MaxPageLimit {
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

// objCreated reads an object's created epoch, tolerating the numeric types
// test objects carry.
func objCreated(obj map[string]any) (time.Time, bool) {
	switch v := obj["created"].(type) {
	case int:
		return time.Unix(int64(v), 0).UTC(), true
	case int64:
		return time.Unix(v, 0).UTC(), true
	case float64:
		return time.Unix(int64(v), 0).UTC(), true
	default:
		return time.Time{}, false
	}
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
