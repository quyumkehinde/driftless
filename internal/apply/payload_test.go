package apply

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"testing/quick"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/queue"
)

// payloadEvent builds a signed-shape event payload for one customer state,
// inserts it as ingest would, and returns the job that would be enqueued.
func payloadEvent(t *testing.T, pool *pgxpool.Pool, eventID string, created time.Time, email string) queue.Job {
	t.Helper()
	payload := fmt.Appendf(nil,
		`{"id":%q,"object":"event","created":%d,"livemode":false,"type":"customer.updated","data":{"object":{"id":"cus_p","object":"customer","email":%q,"created":1735689600}}}`,
		eventID, created.Unix(), email)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO driftless.events (event_id, type, created, source, payload, livemode)
		VALUES ($1, 'customer.updated', $2, 'webhook', $3, false)
		ON CONFLICT (event_id) DO NOTHING`,
		eventID, created, payload)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Job{
		ObjectType: "customer", ObjectID: "cus_p",
		LatestEventID: &eventID, LatestEventCreated: &created,
	}
}

func mirroredEmail(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var email string
	if err := pool.QueryRow(context.Background(),
		`SELECT email FROM stripe.customers WHERE id = 'cus_p'`).Scan(&email); err != nil {
		t.Fatal(err)
	}
	return email
}

func TestPayloadModeAppliesWithoutFetch(t *testing.T) {
	engine, fs, pool := newPayloadEngine(t, "customer")
	ctx := context.Background()

	// upstream disagrees with the payload; payload mode must not fetch,
	// so the payload's version lands
	fs.Put("customer", "cus_p", map[string]any{"email": "upstream@x.y"}, "customer.updated")
	job := payloadEvent(t, pool, "evt_pm1", time.Unix(1000000, 0).UTC(), "payload@x.y")

	if err := engine.Apply(ctx, job); err != nil {
		t.Fatal(err)
	}
	if email := mirroredEmail(t, pool); email != "payload@x.y" {
		t.Errorf("email = %q: payload mode must apply the event body, not fetch", email)
	}

	var syncSource string
	if err := pool.QueryRow(ctx,
		`SELECT sync_source FROM driftless.object_state WHERE object_id = 'cus_p'`).Scan(&syncSource); err != nil {
		t.Fatal(err)
	}
	if syncSource != "payload" {
		t.Errorf("sync_source = %q, want payload", syncSource)
	}
}

func TestPayloadModeSkipsStaleEvent(t *testing.T) {
	engine, _, pool := newPayloadEngine(t, "customer")
	ctx := context.Background()

	newer := payloadEvent(t, pool, "evt_new", time.Unix(2000000, 0).UTC(), "newer@x.y")
	older := payloadEvent(t, pool, "evt_old", time.Unix(1000000, 0).UTC(), "older@x.y")

	if err := engine.Apply(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := engine.Apply(ctx, older); err != nil {
		t.Fatal(err)
	}

	if email := mirroredEmail(t, pool); email != "newer@x.y" {
		t.Errorf("email = %q: stale delivery must not clobber newer state", email)
	}
	// the stale event is still marked processed
	var processed *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT processed_at FROM driftless.events WHERE event_id = 'evt_old'`).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if processed == nil {
		t.Error("skipped stale event must still be marked processed")
	}
}

func TestPayloadModeSameTimestampTieBreak(t *testing.T) {
	// same created second: the higher event id must win in BOTH delivery
	// orders; sync-engine's < guard failed exactly this
	created := time.Unix(3000000, 0).UTC()
	for name, order := range map[string][2]string{
		"low then high": {"evt_tie_a", "evt_tie_b"},
		"high then low": {"evt_tie_b", "evt_tie_a"},
	} {
		t.Run(name, func(t *testing.T) {
			engine, _, pool := newPayloadEngine(t, "customer")
			ctx := context.Background()

			emails := map[string]string{"evt_tie_a": "a@x.y", "evt_tie_b": "b@x.y"}
			for _, eventID := range order {
				job := payloadEvent(t, pool, eventID, created, emails[eventID])
				if err := engine.Apply(ctx, job); err != nil {
					t.Fatal(err)
				}
			}
			// evt_tie_b > evt_tie_a lexicographically: b wins either way
			if email := mirroredEmail(t, pool); email != "b@x.y" {
				t.Errorf("email = %q: the higher event id must win the tie", email)
			}
		})
	}
}

// TestPayloadModeConvergesForAnyOrder is the property the guard promises:
// for any delivery order with duplicates, the final state is the state of
// the maximum (created, id) event.
func TestPayloadModeConvergesForAnyOrder(t *testing.T) {
	engine, _, pool := newPayloadEngine(t, "customer")
	ctx := context.Background()

	// ten events across three created seconds, forcing ties
	type ev struct {
		id      string
		created time.Time
		email   string
	}
	var events []ev
	for i := range 10 {
		events = append(events, ev{
			id:      fmt.Sprintf("evt_prop_%02d", i),
			created: time.Unix(int64(4000000+i/4), 0).UTC(), // 4 per second
			email:   fmt.Sprintf("v%02d@x.y", i),
		})
	}
	// winner: max created, then max id = evt_prop_09
	const wantEmail = "v09@x.y"

	rng := rand.New(rand.NewPCG(42, 0))
	for trial := range 5 {
		// fresh delivery order with duplicates mixed in
		sequence := append([]ev{}, events...)
		sequence = append(sequence, events[rng.IntN(len(events))], events[rng.IntN(len(events))])
		rng.Shuffle(len(sequence), func(i, j int) { sequence[i], sequence[j] = sequence[j], sequence[i] })

		if _, err := pool.Exec(ctx, `
			TRUNCATE stripe.customers;
			DELETE FROM driftless.object_state WHERE object_id = 'cus_p';
			DELETE FROM driftless.events`); err != nil {
			t.Fatal(err)
		}
		for _, e := range sequence {
			job := payloadEvent(t, pool, e.id, e.created, e.email)
			if err := engine.Apply(ctx, job); err != nil {
				t.Fatal(err)
			}
		}
		if email := mirroredEmail(t, pool); email != wantEmail {
			t.Fatalf("trial %d: email = %q, want %q (order %v)", trial, email, wantEmail, sequence)
		}
	}
}

// TestEventNewerModel checks the guard against a straightforward model
// over generated inputs: strictly greater (created, id) and nothing else.
func TestEventNewerModel(t *testing.T) {
	base := time.Unix(5000000, 0).UTC()
	property := func(evSec, stSec uint8, evID, stID uint8, haveState bool) bool {
		evCreated := base.Add(time.Duration(evSec) * time.Second)
		evIDStr := fmt.Sprintf("evt_%03d", evID)
		if !haveState {
			return eventNewer(&evCreated, &evIDStr, nil, nil) == true
		}
		stCreated := base.Add(time.Duration(stSec) * time.Second)
		stIDStr := fmt.Sprintf("evt_%03d", stID)

		want := evSec > stSec || (evSec == stSec && evIDStr > stIDStr)
		return eventNewer(&evCreated, &evIDStr, &stCreated, &stIDStr) == want
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 5000}); err != nil {
		t.Error(err)
	}
}
