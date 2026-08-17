package apply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/quyumkehinde/driftless/internal/mirror"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// applyPayload materializes the event's own data.object instead of
// fetching. Payloads can be stale snapshots delivered in any order, so the
// write is guarded, inside the advisory lock, by the strict (created, id)
// ordering against object_state. A stale event is skipped entirely but
// still marked processed.
func (e *Engine) applyPayload(ctx context.Context, tx pgx.Tx, job queue.Job, payload []byte) error {
	q := db.New(tx)

	state, err := q.GetObjectState(ctx, db.GetObjectStateParams{
		ObjectType: job.ObjectType, ObjectID: job.ObjectID,
	})
	haveState := true
	if errors.Is(err, pgx.ErrNoRows) {
		haveState = false
	} else if err != nil {
		return err
	}

	if haveState && !eventNewer(job.LatestEventCreated, job.LatestEventID, state.LastEventCreated, state.LastEventID) {
		// A newer event already applied: this payload is a stale snapshot.
		if job.LatestEventCreated != nil {
			return q.MarkEventsProcessedForObject(ctx, db.MarkEventsProcessedForObjectParams{
				ObjectID: job.ObjectID,
				Created:  *job.LatestEventCreated,
			})
		}
		return nil
	}

	object, err := extractDataObject(payload)
	if err != nil {
		return fmt.Errorf("apply: event %v for %s %s: %w",
			deref(job.LatestEventID), job.ObjectType, job.ObjectID, err)
	}

	if job.ObjectType == stripeapi.ObjectSubscription {
		if err := e.upsertSubscription(ctx, tx, job.ObjectID, object); err != nil {
			return err
		}
	} else if err := mirror.UpsertObject(ctx, tx, job.ObjectType, job.ObjectID, object); err != nil {
		return err
	}
	return e.finishApplied(ctx, tx, job, mirror.SyncSourcePayload)
}

// eventNewer implements the payload-mode guard: strictly newer created
// wins; equal created falls back to the deterministic id tie-break. An
// event without a recorded predecessor is always newer.
func eventNewer(evCreated *time.Time, evID *string, stateCreated *time.Time, stateID *string) bool {
	if evCreated == nil {
		return false
	}
	if stateCreated == nil {
		return true
	}
	if evCreated.After(*stateCreated) {
		return true
	}
	if !evCreated.Equal(*stateCreated) {
		return false
	}
	if evID == nil {
		return false
	}
	if stateID == nil {
		return true
	}
	return *evID > *stateID
}

// extractDataObject returns the raw bytes of the event's data.object.
func extractDataObject(payload []byte) (json.RawMessage, error) {
	var envelope struct {
		Data struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	if len(envelope.Data.Object) == 0 {
		return nil, fmt.Errorf("payload has no data.object")
	}
	return envelope.Data.Object, nil
}

func deref(s *string) string {
	if s == nil {
		return "<none>"
	}
	return *s
}
