package stripeapi

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Priority orders competing callers for the shared rate budget. Lower is
// more urgent; a starving backfill is acceptable, a starved webhook fetch
// is not.
type Priority int

// The four tiers, most to least urgent.
const (
	PriorityWebhook Priority = iota
	PrioritySweep
	PriorityBackfill
	PriorityVerify
	priorityCount
)

func (p Priority) String() string {
	switch p {
	case PriorityWebhook:
		return "webhook"
	case PrioritySweep:
		return "sweep"
	case PriorityBackfill:
		return "backfill"
	case PriorityVerify:
		return "verify"
	default:
		return "unknown"
	}
}

// ErrLimiterClosed is returned by Acquire after Stop.
var ErrLimiterClosed = errors.New("rate limiter stopped")

// recoveryStep is the multiplicative rate recovery applied per minute
// without a 429: additive-increase in the AIMD sense, 10% per minute.
const recoveryStep = 1.10

// Limiter is one shared token bucket for every Stripe call in the process,
// drained strictly in priority order. On a 429 the effective rate halves
// and then recovers 10% per minute up to the configured rate.
type Limiter struct {
	configured float64 // requests per second, the ceiling

	mu         sync.Mutex
	effective  float64   // current rate after AIMD adjustments
	lastGrow   time.Time // last time recovery was applied
	tokens     float64
	lastRefill time.Time
	waiters    [priorityCount][]*waiter
	arrivals   chan struct{}
	done       chan struct{}
	stopOnce   sync.Once

	// now is replaceable so tests control time.
	now func() time.Time
}

type waiter struct {
	granted   chan struct{}
	abandoned bool
}

// NewLimiter starts the limiter's dispatcher at rps requests per second.
func NewLimiter(rps int) *Limiter {
	l := &Limiter{
		configured: float64(rps),
		effective:  float64(rps),
		arrivals:   make(chan struct{}, 1),
		done:       make(chan struct{}),
		now:        time.Now,
	}
	l.lastRefill = l.now()
	l.lastGrow = l.now()
	go l.dispatch()
	return l
}

// Stop shuts the dispatcher down; pending and future Acquires fail.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() { close(l.done) })
}

// Acquire blocks until a token is granted in priority order, the context
// ends, or the limiter stops.
func (l *Limiter) Acquire(ctx context.Context, p Priority) error {
	w := &waiter{granted: make(chan struct{})}
	l.mu.Lock()
	l.waiters[p] = append(l.waiters[p], w)
	l.mu.Unlock()

	// wake the dispatcher
	select {
	case l.arrivals <- struct{}{}:
	default:
	}

	select {
	case <-w.granted:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		w.abandoned = true
		l.mu.Unlock()
		return ctx.Err()
	case <-l.done:
		return ErrLimiterClosed
	}
}

// On429 halves the effective rate; retryAfter, when positive, additionally
// empties the bucket so no call goes out before the server asked.
func (l *Limiter) On429(retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked()
	l.effective = max(1, l.effective/2)
	l.lastGrow = l.now()
	if retryAfter > 0 {
		l.tokens = -retryAfter.Seconds() * l.effective
	}
	if l.tokens > 0 {
		l.tokens = 0
	}
}

// EffectiveRPS reports the current AIMD-adjusted rate.
func (l *Limiter) EffectiveRPS() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recoverLocked()
	return l.effective
}

// dispatch grants tokens to the highest-priority waiter, sleeping exactly
// until the next token accrues.
func (l *Limiter) dispatch() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		l.mu.Lock()
		l.recoverLocked()
		l.refillLocked()
		for l.tokens >= 1 {
			w := l.popLocked()
			if w == nil {
				break
			}
			if w.abandoned {
				continue
			}
			l.tokens--
			close(w.granted)
		}
		wait := time.Hour // idle: park until an arrival
		if l.hasWaitersLocked() {
			deficit := 1 - l.tokens
			wait = time.Duration(deficit / l.effective * float64(time.Second))
			if wait < time.Millisecond {
				wait = time.Millisecond
			}
		}
		l.mu.Unlock()

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-l.done:
			return
		case <-l.arrivals:
		case <-timer.C:
		}
	}
}

// refillLocked accrues tokens since the last refill, capped at one second
// of burst.
func (l *Limiter) refillLocked() {
	now := l.now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.lastRefill = now
	l.tokens += elapsed * l.effective
	if l.tokens > l.effective {
		l.tokens = l.effective
	}
}

// recoverLocked applies the 10% per minute recovery toward the ceiling.
func (l *Limiter) recoverLocked() {
	now := l.now()
	for l.effective < l.configured && now.Sub(l.lastGrow) >= time.Minute {
		l.effective = min(l.configured, l.effective*recoveryStep)
		l.lastGrow = l.lastGrow.Add(time.Minute)
	}
	if l.effective >= l.configured {
		l.lastGrow = now
	}
}

func (l *Limiter) popLocked() *waiter {
	for p := range l.waiters {
		if len(l.waiters[p]) > 0 {
			w := l.waiters[p][0]
			l.waiters[p] = l.waiters[p][1:]
			return w
		}
	}
	return nil
}

func (l *Limiter) hasWaitersLocked() bool {
	for p := range l.waiters {
		if len(l.waiters[p]) > 0 {
			return true
		}
	}
	return false
}
