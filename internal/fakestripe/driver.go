package fakestripe

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"
)

// delivery collects the options for one Deliver call.
type delivery struct {
	copies         int
	wrongSignature bool
	delay          time.Duration
}

// DeliverOption tweaks how an event is delivered.
type DeliverOption func(*delivery)

// Duplicate delivers the event n times total, the way Stripe's at-least-once
// retries do.
func Duplicate(n int) DeliverOption {
	return func(d *delivery) { d.copies = n }
}

// WrongSignature signs the delivery with a key the receiver does not know.
func WrongSignature() DeliverOption {
	return func(d *delivery) { d.wrongSignature = true }
}

// Delay sleeps before delivering, for late-arrival interleavings.
func Delay(dur time.Duration) DeliverOption {
	return func(d *delivery) { d.delay = dur }
}

// Deliver signs one event and POSTs it to target's webhook endpoint,
// returning the HTTP status of each copy in order.
func (s *Server) Deliver(t *testing.T, target string, eventID string, opts ...DeliverOption) []int {
	t.Helper()
	event, ok := s.Event(eventID)
	if !ok {
		t.Fatalf("fakestripe: no event %s", eventID)
	}

	d := delivery{copies: 1}
	for _, opt := range opts {
		opt(&d)
	}
	if d.delay > 0 {
		time.Sleep(d.delay)
	}

	secret := s.secret
	if d.wrongSignature {
		secret = "whsec_attacker_controlled"
	}

	statuses := make([]int, 0, d.copies)
	for range d.copies {
		statuses = append(statuses, s.post(t, target, event, secret))
	}
	return statuses
}

// DeliverAll delivers every event in order; OutOfOrder shuffles first.
// Returns event ids in the order delivered.
func (s *Server) DeliverAll(t *testing.T, target string, outOfOrder bool) []string {
	t.Helper()
	events := s.Events()
	if outOfOrder {
		seed := rand.Uint64()
		t.Logf("fakestripe: out-of-order delivery, shuffle seed %d", seed)
		rng := rand.New(rand.NewPCG(seed, 0))
		rng.Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })
	}
	ids := make([]string, 0, len(events))
	for _, e := range events {
		status := s.post(t, target, e, s.secret)
		if status != http.StatusOK {
			t.Fatalf("fakestripe: delivering %s: status %d", e.ID, status)
		}
		ids = append(ids, e.ID)
	}
	return ids
}

// TryDeliver delivers one event and reports transport failures as errors
// instead of failing the test. Chaos tests use it when the receiver is
// expected to die mid-request.
func (s *Server) TryDeliver(target, eventID string) (int, error) {
	event, ok := s.Event(eventID)
	if !ok {
		return 0, fmt.Errorf("fakestripe: no event %s", eventID)
	}
	return s.tryPost(target, event, s.secret)
}

// DeliverConcurrent delivers the same event n times at once, the dedup-race
// shape, and returns every status.
func (s *Server) DeliverConcurrent(t *testing.T, target, eventID string, n int) []int {
	t.Helper()
	type result struct {
		status int
		err    error
	}
	results := make(chan result, n)
	start := make(chan struct{})
	for range n {
		go func() {
			<-start
			status, err := s.TryDeliver(target, eventID)
			results <- result{status, err}
		}()
	}
	close(start)

	statuses := make([]int, 0, n)
	for range n {
		r := <-results
		if r.err != nil {
			t.Fatalf("fakestripe: concurrent delivery: %v", r.err)
		}
		statuses = append(statuses, r.status)
	}
	return statuses
}

func (s *Server) post(t *testing.T, target string, event Event, secret string) int {
	t.Helper()
	status, err := s.tryPost(target, event, secret)
	if err != nil {
		t.Fatalf("fakestripe: delivering %s: %v", event.ID, err)
	}
	return status
}

func (s *Server) tryPost(target string, event Event, secret string) (int, error) {
	req, err := http.NewRequest(http.MethodPost, target+"/webhooks/stripe", bytes.NewReader(event.Payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Stripe-Signature", signHeader(secret, time.Now(), event.Payload))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// signHeader produces the Stripe-Signature header. A cross-check test
// pins this construction against stripe-go's verifier.
func signHeader(secret string, at time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.", at.Unix())
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", at.Unix(), hex.EncodeToString(mac.Sum(nil)))
}
