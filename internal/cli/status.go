package cli

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
)

func newStatusCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show sync health at a glance: events, queue, sweeps, backfill, drift",
		Long: "Status prints a one-screen summary of the mirror: event totals and\n" +
			"freshness, queue depth by state, apply lag, the last sweep and any gaps it\n" +
			"found, the latest backfill run, the latest verification, and stored event\n" +
			"types that map to no known object.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()
			q := queue.New(pool, cfg.Workers.VisibilityTimeout.Std(), cfg.Workers.MaxAttempts)
			return printStatus(cmd, pool, q)
		},
	}
}

func printStatus(cmd *cobra.Command, pool *pgxpool.Pool, jobs *queue.Queue) error {
	ctx := cmd.Context()
	q := db.New(pool)

	events, err := q.StatusEvents(ctx)
	if err != nil {
		return err
	}
	cmd.Printf("events      total=%d  unprocessed=%d  last received %s\n",
		events.Total, events.Unprocessed, ago(events.LastReceived))

	counts, err := jobs.CountByStatus(ctx)
	if err != nil {
		return err
	}
	cmd.Printf("queue       pending=%d running=%d dead=%d\n",
		counts[queue.StatusPending], counts[queue.StatusRunning], counts[queue.StatusDead])
	oldest, err := q.StatusOldestLiveJob(ctx)
	if err != nil {
		return err
	}
	if !isNever(oldest) {
		cmd.Printf("lag         oldest live job event %s\n", ago(oldest))
	} else {
		cmd.Println("lag         none: no live jobs")
	}

	sweep, err := q.StatusLastSweep(ctx)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		cmd.Println("sweeps      none yet")
	case err != nil:
		return err
	default:
		finished := sweep.WindowTo
		if sweep.FinishedAt != nil {
			finished = *sweep.FinishedAt
		}
		gaps24h, err := q.StatusGapsLastDay(ctx)
		if err != nil {
			return err
		}
		cmd.Printf("sweeps      last done %s  events_seen=%d  gaps_found=%d  gaps_24h=%d\n",
			ago(finished), sweep.EventsSeen, sweep.GapsFound, gaps24h)
	}

	run, err := q.StatusLatestBackfillRun(ctx)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		cmd.Println("backfill    none yet")
	case err != nil:
		return err
	default:
		when := run.StartedAt
		if run.FinishedAt != nil {
			when = *run.FinishedAt
		}
		cmd.Printf("backfill    run %d %s %s  tasks=%d/%d  objects=%d\n",
			run.ID, run.Status, ago(when), run.TasksDone, run.TasksTotal, run.ObjectsDone)
	}

	verification, err := q.StatusLatestVerification(ctx)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		cmd.Println("verify      none yet")
	case err != nil:
		return err
	default:
		when := verification.StartedAt
		if verification.FinishedAt != nil {
			when = *verification.FinishedAt
		}
		cmd.Printf("verify      last %s %s  checked=%d drifted=%d repaired=%d\n",
			verification.Mode, ago(when), verification.Checked, verification.Drifted, verification.Repaired)
	}

	return printUnhandled(ctx, cmd, q)
}

// printUnhandled lists stored-but-unmappable event types, the signal that
// the mapping table needs a new entry.
func printUnhandled(ctx context.Context, cmd *cobra.Command, q *db.Queries) error {
	types, err := q.StatusUnprocessedEventTypes(ctx)
	if err != nil {
		return err
	}
	var unhandled []db.StatusUnprocessedEventTypesRow
	for _, row := range types {
		if _, known := apply.ResolveType(row.Type); !known {
			unhandled = append(unhandled, row)
		}
	}
	if len(unhandled) == 0 {
		cmd.Println("unhandled   none")
		return nil
	}
	cmd.Println("unhandled   stored events with no object mapping:")
	for _, row := range unhandled {
		cmd.Printf("            %s: %d\n", row.Type, row.Count)
	}
	return nil
}

func ago(t time.Time) string {
	if isNever(t) {
		return "never"
	}
	return time.Since(t).Round(time.Second).String() + " ago"
}

// epoch marks a coalesced "never" timestamp from the status queries.
func isNever(t time.Time) bool {
	return !t.After(time.Unix(0, 0))
}
