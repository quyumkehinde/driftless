package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func customerEmail(_ *testing.T, pool *pgxpool.Pool, id string) func() (string, bool) {
	return func() (string, bool) {
		var email string
		err := pool.QueryRow(context.Background(),
			`SELECT email FROM stripe.customers WHERE id = $1 AND NOT is_deleted`, id).Scan(&email)
		return email, err == nil
	}
}

// TestOutOfOrderDelivery replays late-arriving webhooks: the update event
// arrives before the create event. The mirror must converge to the newest
// state in both modes.
func TestOutOfOrderDelivery(t *testing.T) {
	for name, extraEnv := range map[string][]string{
		"fetch mode":   nil,
		"payload mode": {"DRIFTLESS_APPLY_PAYLOAD_MODE_TYPES=customer"},
	} {
		t.Run(name, func(t *testing.T) {
			binary := buildBinary(t)
			pool, connString := testpg.StartWithURL(t)
			fs := fakestripe.New(t, e2eSecret)

			created := fs.Put("customer", "cus_ooo", map[string]any{"email": "v1@x.y"}, "customer.created")
			updated := fs.Put("customer", "cus_ooo", map[string]any{"email": "v2@x.y"}, "customer.updated")

			proc := startServe(t, binary, connString, fs.URL(), "", extraEnv...)

			// newest first, then the older create
			fs.Deliver(t, proc.IngestURL, updated.ID)
			fs.Deliver(t, proc.IngestURL, created.ID)

			read := customerEmail(t, pool, "cus_ooo")
			waitFor(t, 15*time.Second, "customer to mirror", func() bool {
				_, ok := read()
				return ok
			})
			// allow the second (stale) delivery to be processed too
			waitForDrain(t, pool)

			if email, _ := read(); email != "v2@x.y" {
				t.Errorf("email = %q, want the newer v2@x.y", email)
			}
		})
	}
}

// TestSameTimestampOrdering pins the same-second tie: two subscription
// events share one created second and arrive in generation order. The
// event with the higher id must win under payload mode; a strictly-less
// guard drops it, which was a real sync-engine bug.
func TestSameTimestampOrdering(t *testing.T) {
	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)
	fs := fakestripe.New(t, e2eSecret)

	noItems := map[string]any{"data": []any{}, "has_more": false}
	first := fs.Put("subscription", "sub_tie", map[string]any{
		"customer": "cus_1", "status": "active", "items": noItems,
	}, "customer.subscription.updated")
	second := fs.PutTied("subscription", "sub_tie", map[string]any{
		"customer": "cus_1", "status": "trialing", "items": noItems,
	}, "customer.subscription.trial_will_end")

	if !first.Created.Equal(second.Created) {
		t.Fatalf("test setup: events must share a created second, got %v and %v", first.Created, second.Created)
	}

	proc := startServe(t, binary, connString, fs.URL(), "",
		"DRIFTLESS_APPLY_PAYLOAD_MODE_TYPES=subscription")

	fs.Deliver(t, proc.IngestURL, first.ID)
	fs.Deliver(t, proc.IngestURL, second.ID)

	ctx := context.Background()
	waitFor(t, 15*time.Second, "subscription to mirror", func() bool {
		return countRow(t, pool, `SELECT count(*) FROM stripe.subscriptions WHERE id = 'sub_tie'`) == 1
	})
	waitForDrain(t, pool)

	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM stripe.subscriptions WHERE id = 'sub_tie'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	// the second event has the higher id: its data must win the tie
	if status != "trialing" {
		t.Errorf("status = %q, want trialing from the higher-id event", status)
	}
}
