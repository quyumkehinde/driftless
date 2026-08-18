package cli

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/testpg"
)

func TestNextAutoVerify(t *testing.T) {
	loc := time.FixedZone("test", 3600)
	tests := []struct {
		name string
		now  time.Time
		at   string
		want time.Time
	}{
		{
			"later today",
			time.Date(2026, 8, 18, 1, 30, 0, 0, loc), "03:00",
			time.Date(2026, 8, 18, 3, 0, 0, 0, loc),
		},
		{
			"already passed rolls to tomorrow",
			time.Date(2026, 8, 18, 3, 0, 1, 0, loc), "03:00",
			time.Date(2026, 8, 19, 3, 0, 0, 0, loc),
		},
		{
			"exactly now rolls to tomorrow",
			time.Date(2026, 8, 18, 3, 0, 0, 0, loc), "03:00",
			time.Date(2026, 8, 19, 3, 0, 0, 0, loc),
		},
		{
			"month boundary",
			time.Date(2026, 8, 31, 23, 59, 0, 0, loc), "00:30",
			time.Date(2026, 9, 1, 0, 30, 0, 0, loc),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextAutoVerify(tt.now, tt.at); !got.Equal(tt.want) {
				t.Errorf("nextAutoVerify(%v, %s) = %v, want %v", tt.now, tt.at, got, tt.want)
			}
		})
	}
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, id string, age time.Duration, processed bool) {
	t.Helper()
	var processedAt *time.Time
	if processed {
		at := time.Now().Add(-age)
		processedAt = &at
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.events (event_id, type, created, received_at, source, payload, livemode, processed_at)
		VALUES ($1, 'customer.created', now() - $2::interval, now() - $2::interval, 'webhook', '{}', false, $3)`,
		id, age.String(), processedAt); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionPurgesOnlyOldProcessedEvents(t *testing.T) {
	pool := testpg.Start(t)

	insertEvent(t, pool, "evt_old_done", 100*24*time.Hour, true)
	insertEvent(t, pool, "evt_old_pending", 100*24*time.Hour, false)
	insertEvent(t, pool, "evt_fresh_done", 24*time.Hour, true)

	// the purged event carries a gap audit row, which must go with it
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO driftless.gaps (event_id, sweep_id, event_created, lag)
		VALUES ('evt_old_done', 1, now() - interval '100 days', interval '1 hour')`); err != nil {
		t.Fatal(err)
	}

	purged, err := db.New(pool).PurgeOldEvents(t.Context(), time.Now().AddDate(0, 0, -90))
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Errorf("purged = %d, want exactly the old processed event", purged)
	}

	var remaining []string
	rows, err := pool.Query(t.Context(), `SELECT event_id FROM driftless.events ORDER BY event_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		remaining = append(remaining, id)
	}
	want := []string{"evt_fresh_done", "evt_old_pending"}
	if len(remaining) != 2 || remaining[0] != want[0] || remaining[1] != want[1] {
		t.Errorf("remaining events = %v, want %v: unprocessed events outlive retention until applied", remaining, want)
	}

	var gaps int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM driftless.gaps`).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps != 0 {
		t.Errorf("gap rows = %d, want 0 after their event was purged", gaps)
	}
}
