package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/quyumkehinde/driftless/internal/fakestripe"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

// TestSecretRotation walks the documented rotation path: move the old
// secret to secondary, put the new secret in primary, and deliveries
// signed with either are accepted while everything else stays rejected.
func TestSecretRotation(t *testing.T) {
	const oldSecret = "whsec_e2e_old"
	const newSecret = "whsec_e2e_new"

	binary := buildBinary(t)
	pool, connString := testpg.StartWithURL(t)

	// three signers: Stripe still using the old secret mid-rotation,
	// Stripe on the new secret, and an attacker with neither
	fsOld := fakestripe.New(t, oldSecret)
	fsNew := fakestripe.New(t, newSecret)
	fsWrong := fakestripe.New(t, "whsec_attacker")

	proc := startServe(t, binary, connString, fsNew.URL(), "",
		"DRIFTLESS_STRIPE_WEBHOOK_SECRET="+newSecret,
		"DRIFTLESS_STRIPE_WEBHOOK_SECRET_SECONDARY="+oldSecret,
	)

	deliver := func(fs *fakestripe.Server, id string) int {
		event := fs.Put("customer", id, nil, "customer.created")
		status, err := fs.TryDeliver(proc.IngestURL, event.ID)
		if err != nil {
			t.Fatalf("deliver %s: %v", id, err)
		}
		return status
	}

	if status := deliver(fsOld, "cus_old"); status != http.StatusOK {
		t.Errorf("old-secret delivery = %d, want 200: rotation must not drop events", status)
	}
	if status := deliver(fsNew, "cus_new"); status != http.StatusOK {
		t.Errorf("new-secret delivery = %d, want 200", status)
	}
	if status := deliver(fsWrong, "cus_evil"); status != http.StatusBadRequest {
		t.Errorf("wrong-secret delivery = %d, want 400", status)
	}

	waitFor(t, 10*time.Second, "both legitimate events recorded", func() bool {
		return countRow(t, pool, `SELECT count(*) FROM driftless.events`) == 2
	})
	if n := countRow(t, pool, `SELECT count(*) FROM driftless.events WHERE event_id LIKE '%evil%'`); n != 0 {
		t.Errorf("attacker event rows = %d, want 0", n)
	}
}
