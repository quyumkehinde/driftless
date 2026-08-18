// Package fakestripe is a scripted Stripe double for tests. It serves the
// read API subset driftless calls, keeps an in-memory object store, appends
// a synthetic event for every mutation (so the events list always matches
// object history), and drives signed webhook deliveries with failure
// options real incidents exhibited.
package fakestripe

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// AccountID is the fixed account the double reports on GET /v1/account.
const AccountID = "acct_fake000000000001"

// Event is one synthetic event with its finished payload.
type Event struct {
	ID      string
	Type    string
	Created time.Time
	Payload []byte
}

// Server is the double. All methods are safe for concurrent use, but
// mutations must run on the test goroutine: a marshal failure fails the
// test via t.Fatalf.
type Server struct {
	t      *testing.T
	secret string

	mu       sync.Mutex
	objects  map[string]map[string]map[string]any // objectType -> id -> object
	order    map[string][]string                  // objectType -> ids in insertion order
	events   []Event                              // append-only, oldest first
	clock    time.Time
	seq      int
	instance int64
	faults   faults

	srv *httptest.Server
}

// instanceSeq distinguishes event ids across instances in one process:
// real Stripe event ids are globally unique, so the double's must be too,
// or a receiver fed by two doubles misreads the second as a duplicate.
var instanceSeq atomic.Int64

// New starts the double. secret signs webhook deliveries; the internal
// clock starts at a fixed instant and advances one second per mutation so
// event ordering is deterministic.
func New(t *testing.T, secret string) *Server {
	t.Helper()
	s := &Server{
		t:        t,
		secret:   secret,
		objects:  make(map[string]map[string]map[string]any),
		order:    make(map[string][]string),
		clock:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		instance: instanceSeq.Add(1),
	}
	s.srv = httptest.NewServer(s.apiHandler())
	t.Cleanup(s.srv.Close)
	return s
}

// URL is the base URL for API clients, in place of https://api.stripe.com.
func (s *Server) URL() string { return s.srv.URL }

// Advance moves the double's clock forward.
func (s *Server) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = s.clock.Add(d)
}

// Put stores (or replaces) an object and appends the matching event.
// The object's "id" and "object" fields are set from the arguments.
func (s *Server) Put(objectType, id string, obj map[string]any, eventType string) Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(objectType, id, obj, eventType)
}

// PutTied is Put with the event stamped at the same instant as the
// previous event, for same-second ordering-tie scenarios.
func (s *Server) PutTied(objectType, id string, obj map[string]any, eventType string) Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	// appendEvent advances the clock by one second; rewinding first pins
	// this event to the previous event's timestamp
	s.clock = s.clock.Add(-time.Second)
	return s.putLocked(objectType, id, obj, eventType)
}

func (s *Server) putLocked(objectType, id string, obj map[string]any, eventType string) Event {
	copied := make(map[string]any, len(obj)+2)
	for k, v := range obj {
		copied[k] = v
	}
	copied["id"] = id
	copied["object"] = objectType
	// every real Stripe object carries its creation time; updates keep the
	// original, the way Stripe does
	if copied["created"] == nil {
		if existing := s.objects[objectType][id]; existing != nil && existing["created"] != nil {
			copied["created"] = existing["created"]
		} else {
			copied["created"] = s.clock.Unix()
		}
	}

	if s.objects[objectType] == nil {
		s.objects[objectType] = make(map[string]map[string]any)
	}
	if _, exists := s.objects[objectType][id]; !exists {
		s.order[objectType] = append(s.order[objectType], id)
	}
	s.objects[objectType][id] = copied

	return s.appendEvent(eventType, copied)
}

// Delete removes an object and appends the matching deleted event, whose
// data.object carries "deleted": true the way Stripe sends it.
func (s *Server) Delete(objectType, id string, eventType string) Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj := s.objects[objectType][id]
	if obj == nil {
		obj = map[string]any{"id": id, "object": objectType}
	}
	tombstone := make(map[string]any, len(obj)+1)
	for k, v := range obj {
		tombstone[k] = v
	}
	tombstone["deleted"] = true

	delete(s.objects[objectType], id)
	for i, oid := range s.order[objectType] {
		if oid == id {
			s.order[objectType] = append(s.order[objectType][:i], s.order[objectType][i+1:]...)
			break
		}
	}
	return s.appendEvent(eventType, tombstone)
}

// Object returns a copy of a stored object, or ok=false after deletion.
func (s *Server) Object(objectType, id string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[objectType][id]
	if !ok {
		return nil, false
	}
	copied := make(map[string]any, len(obj))
	for k, v := range obj {
		copied[k] = v
	}
	return copied, true
}

// Events returns all events, oldest first.
func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Event returns one event by id.
func (s *Server) Event(id string) (Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.ID == id {
			return e, true
		}
	}
	return Event{}, false
}

// appendEvent builds and records the event. Callers hold s.mu.
func (s *Server) appendEvent(eventType string, dataObject map[string]any) Event {
	s.seq++
	s.clock = s.clock.Add(time.Second)
	id := fmt.Sprintf("evt_fake%d_%06d", s.instance, s.seq)

	payload, err := json.Marshal(map[string]any{
		"id":          id,
		"object":      "event",
		"api_version": stripeapi.StripeVersion,
		"created":     s.clock.Unix(),
		"livemode":    false,
		"type":        eventType,
		"data":        map[string]any{"object": dataObject},
	})
	if err != nil {
		s.t.Fatalf("fakestripe: event %s for type %s does not marshal: %v", id, eventType, err)
	}
	event := Event{ID: id, Type: eventType, Created: s.clock, Payload: payload}
	s.events = append(s.events, event)
	return event
}
