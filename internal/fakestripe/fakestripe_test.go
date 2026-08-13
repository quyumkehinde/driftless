package fakestripe

import (
	"strings"
	"testing"
	"time"
)

func TestPutStoresAndAppendsEvent(t *testing.T) {
	s := New(t, "whsec_test")

	event := s.Put("customer", "cus_1", map[string]any{"email": "a@b.c"}, "customer.created")
	if event.Type != "customer.created" {
		t.Errorf("event type = %q", event.Type)
	}

	obj, ok := s.Object("customer", "cus_1")
	if !ok {
		t.Fatal("object not stored")
	}
	if obj["id"] != "cus_1" || obj["object"] != "customer" || obj["email"] != "a@b.c" {
		t.Errorf("object = %v", obj)
	}

	events := s.Events()
	if len(events) != 1 || events[0].ID != event.ID {
		t.Errorf("events = %v", events)
	}
}

func TestEventsAreOrderedAndClockAdvances(t *testing.T) {
	s := New(t, "whsec_test")

	first := s.Put("customer", "cus_1", nil, "customer.created")
	second := s.Put("customer", "cus_1", map[string]any{"email": "x@y.z"}, "customer.updated")
	s.Advance(time.Hour)
	third := s.Put("customer", "cus_2", nil, "customer.created")

	if !second.Created.After(first.Created) {
		t.Error("each mutation must advance the clock")
	}
	if third.Created.Sub(second.Created) < time.Hour {
		t.Error("Advance must move subsequent events forward")
	}
}

func TestDeleteAppendsTombstoneEvent(t *testing.T) {
	s := New(t, "whsec_test")
	s.Put("customer", "cus_1", map[string]any{"email": "a@b.c"}, "customer.created")

	event := s.Delete("customer", "cus_1", "customer.deleted")
	if _, ok := s.Object("customer", "cus_1"); ok {
		t.Error("object should be gone after delete")
	}
	if want := `"deleted":true`; !strings.Contains(string(event.Payload), want) {
		t.Errorf("tombstone payload missing %s: %s", want, event.Payload)
	}
	// history preserved: create + delete
	if n := len(s.Events()); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
}
