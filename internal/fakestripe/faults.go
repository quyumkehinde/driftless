package fakestripe

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// faults holds the injectable failure state, guarded by Server.mu.
type faults struct {
	nextStatuses []int         // one-shot failures, consumed in order
	rate         float64       // probability of failing any API request
	rateStatus   int           // status the rate failures return
	latency      time.Duration // added to every API response
	force404     map[string]bool
	rng          *rand.Rand
}

// FailNext queues n one-shot failures with the given status; they consume
// before rate-based failures apply.
func (s *Server) FailNext(n, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range n {
		s.faults.nextStatuses = append(s.faults.nextStatuses, status)
	}
}

// FailRate makes every API request fail with status at probability p, until
// called again with p = 0. Deterministically seeded.
func (s *Server) FailRate(p float64, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults.rate = p
	s.faults.rateStatus = status
}

// Latency adds a fixed delay to every API response.
func (s *Server) Latency(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults.latency = d
}

// Force404 makes fetches of one object id answer resource_missing even
// though it remains in the store: the shape of a deprecated resource or a
// fetch racing a deletion.
func (s *Server) Force404(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.force404 == nil {
		s.faults.force404 = make(map[string]bool)
	}
	s.faults.force404[id] = true
}

// interceptFault applies latency and, when armed, writes a failure
// response. It reports whether the request was consumed. Force404 is
// enforced in handleGet, where the object id is known.
func (s *Server) interceptFault(w http.ResponseWriter) bool {
	s.mu.Lock()
	latency := s.faults.latency
	status := 0
	if len(s.faults.nextStatuses) > 0 {
		status = s.faults.nextStatuses[0]
		s.faults.nextStatuses = s.faults.nextStatuses[1:]
	} else if s.faults.rate > 0 {
		if s.faults.rng == nil {
			s.faults.rng = rand.New(rand.NewPCG(7, 0))
		}
		if s.faults.rng.Float64() < s.faults.rate {
			status = s.faults.rateStatus
		}
	}
	s.mu.Unlock()

	if latency > 0 {
		time.Sleep(latency)
	}
	if status != 0 {
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", strconv.Itoa(1))
		}
		writeError(w, status, "injected_failure")
		return true
	}
	return false
}

// isForced404 reports whether fetches of this id are forced to answer
// resource_missing, whatever the cause an operator had in mind.
func (s *Server) isForced404(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults.force404 != nil && s.faults.force404[id]
}
