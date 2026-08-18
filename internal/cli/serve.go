package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/backfill"
	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/ingest"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/store/migrations"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/sweep"
	"github.com/quyumkehinde/driftless/internal/verify"
)

const shutdownTimeout = 10 * time.Second

// workerPollInterval is the idle sleep between claim attempts when the
// queue is empty; claims themselves are immediate.
const workerPollInterval = 250 * time.Millisecond

func newServeCmd(flags *rootFlags) *cobra.Command {
	var forceAccount bool
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the webhook receiver and metrics listeners",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, pool, err := openPool(cmd, flags, config.ScopeServe)
			if err != nil {
				return err
			}
			defer pool.Close()

			if err := refuseIfMigrationsPending(cmd.Context(), cfg); err != nil {
				return err
			}
			return runServe(cmd, cfg, pool, forceAccount)
		},
	}
	cmd.Flags().BoolVar(&forceAccount, "force-account", false,
		"overwrite the recorded Stripe account instead of refusing on mismatch")
	return cmd
}

// refuseIfMigrationsPending makes serve exit with the migrations-pending
// code instead of running against an outdated schema.
func refuseIfMigrationsPending(ctx context.Context, cfg *config.Config) error {
	sdb, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = sdb.Close() }()
	pending, err := migrations.Pending(ctx, sdb)
	if err != nil {
		return fmt.Errorf("check migrations: %w", err)
	}
	if pending > 0 {
		return &MigrationsPendingError{Pending: pending}
	}
	return nil
}

func runServe(cmd *cobra.Command, cfg *config.Config, pool *pgxpool.Pool, forceAccount bool) error {
	logger, err := obs.NewLogger(cmd.ErrOrStderr(), cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}

	registry := obs.NewRegistry()
	verifier := ingest.NewVerifier(
		cfg.Stripe.WebhookSecret,
		cfg.Stripe.WebhookSecretSecondary,
		cfg.Stripe.SignatureTolerance.Std(),
	)
	q := queue.New(pool, cfg.Workers.VisibilityTimeout.Std(), cfg.Workers.MaxAttempts)
	ingestServer := ingest.NewServer(pool, q, verifier, logger, ingest.NewMetrics(registry))

	limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
	defer limiter.Stop()
	client := stripeapi.New(cfg.Stripe.APIBaseURL, cfg.Stripe.APIKey, limiter, stripeapi.NewMetrics(registry, limiter))
	if err := ensureAccount(cmd.Context(), pool, client, forceAccount, logger); err != nil {
		return err
	}
	engine := apply.NewEngine(pool, client, cfg.Apply.PayloadModeTypes, logger, apply.NewMetrics(registry))
	queueMetrics := queue.NewMetrics(registry)
	workers := queue.NewWorkerPool(q, engine, cfg.Workers.Count,
		workerPollInterval, logger, queueMetrics)

	latestMigration, err := migrations.LatestVersion()
	if err != nil {
		return err
	}
	readyz := obs.Readyz(
		obs.Check{Name: "postgres", Run: pool.Ping},
		obs.Check{Name: "migrations", Run: func(ctx context.Context) error {
			var applied int64
			err := pool.QueryRow(ctx,
				`SELECT coalesce(max(version_id), 0) FROM `+migrations.VersionTable).Scan(&applied)
			if err != nil {
				return err
			}
			if applied < latestMigration {
				return fmt.Errorf("schema at version %d, want %d", applied, latestMigration)
			}
			return nil
		}},
	)

	ingestSrv := &http.Server{Addr: cfg.Server.Listen, Handler: ingestServer.Handler()}
	metricsSrv := obs.NewMetricsServer(cfg.Server.MetricsListen, registry, readyz, cfg.Server.EnablePprof)

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workersDone := make(chan struct{})
	go func() {
		workers.Run(ctx)
		close(workersDone)
	}()
	go runReaper(ctx, q, queueMetrics, cfg.Workers.VisibilityTimeout.Std(), logger)
	sweeper := sweep.New(pool, client, q, logger, sweep.NewMetrics(registry),
		cfg.Sweep.Overlap.Std(), cfg.Sweep.FirstRunLookback.Std())
	go runSweeper(ctx, pool, sweeper, cfg.Sweep.Interval.Std(), logger)
	if cfg.Backfill.AutoResume {
		runner := backfill.NewRunner(pool, client, logger, backfill.NewMetrics(registry), nil)
		go resumeInterruptedBackfills(ctx, pool, runner, logger)
	}
	if cfg.Verify.Auto {
		verifier := verify.NewRunner(pool, client, logger, nil)
		go runAutoVerify(ctx, pool, verifier, cfg.Verify.AutoTime, logger)
	}
	if cfg.Retention.EventsDays > 0 {
		go runRetention(ctx, pool, cfg.Retention.EventsDays, logger)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- serveListener("ingest", ingestSrv) }()
	go func() { errCh <- serveListener("metrics", metricsSrv) }()
	logger.Info("serve started",
		"ingest_listen", cfg.Server.Listen,
		"metrics_listen", cfg.Server.MetricsListen,
		"workers", cfg.Workers.Count)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		errShutdown := errors.Join(ingestSrv.Shutdown(shutdownCtx), metricsSrv.Shutdown(shutdownCtx))
		<-errCh
		<-errCh
		<-workersDone
		return errShutdown
	case err := <-errCh:
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = ingestSrv.Shutdown(shutdownCtx)
		_ = metricsSrv.Shutdown(shutdownCtx)
		<-errCh
		<-workersDone
		return err
	}
}

// resumeInterruptedBackfills continues any backfill run a crash left in
// progress, sequentially and at backfill priority, so a restart never
// silently abandons an import.
func resumeInterruptedBackfills(ctx context.Context, pool *pgxpool.Pool, runner *backfill.Runner, logger *slog.Logger) {
	runs, err := db.New(pool).ListResumableRuns(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error("listing resumable backfill runs failed", "error", err)
		}
		return
	}
	for _, run := range runs {
		if ctx.Err() != nil {
			return
		}
		logger.Info("auto-resuming backfill run", "run_id", run.ID)
		if err := runner.Resume(ctx, run.ID); err != nil {
			if ctx.Err() == nil {
				// one held or failing run must not abandon the others
				logger.Error("backfill auto-resume failed", "run_id", run.ID, "error", err)
			}
			continue
		}
	}
}

// sweeperLockKey serializes sweep passes across every serve replica, so
// running more than one replica never double-sweeps.
const sweeperLockKey = `hashtextextended('driftless:sweeper', 0)`

// runSweeper runs one sweep pass immediately, then one per interval. Each
// pass elects a leader with a try-advisory lock, so extra replicas skip
// the pass instead of duplicating it, and take over when the leader dies.
func runSweeper(ctx context.Context, pool *pgxpool.Pool, s *sweep.Sweeper, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		sweepOnce(ctx, pool, s, logger)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sweepOnce runs a single sweep pass if this replica wins the lock.
func sweepOnce(ctx context.Context, pool *pgxpool.Pool, s *sweep.Sweeper, logger *slog.Logger) {
	if ctx.Err() != nil {
		return
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error("sweep lock connection failed", "error", err)
		}
		return
	}
	defer conn.Release()

	var leader bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(`+sweeperLockKey+`)`).Scan(&leader); err != nil {
		if ctx.Err() == nil {
			logger.Error("sweep leader election failed", "error", err)
		}
		return
	}
	if !leader {
		return // another replica is sweeping this pass
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(`+sweeperLockKey+`)`)
	}()

	result, err := s.RunOnce(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error("sweep failed", "error", err)
		}
		return
	}
	if result.GapsFound > 0 {
		logger.Warn("sweep found undelivered events",
			"events_seen", result.EventsSeen, "gaps_found", result.GapsFound)
	}
}

// autoVerifyLockKey serializes scheduled verifications across replicas.
const autoVerifyLockKey = `hashtextextended('driftless:verify-auto', 0)`

// nextAutoVerify returns the next occurrence of the HH:MM wall-clock time
// strictly after now, in now's location.
func nextAutoVerify(now time.Time, autoTime string) time.Time {
	at, err := time.Parse("15:04", autoTime)
	if err != nil {
		// config validation rejects unparseable times before serve starts
		panic("unvalidated verify.auto_time: " + autoTime)
	}
	next := time.Date(now.Year(), now.Month(), now.Day(), at.Hour(), at.Minute(), 0, 0, now.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// runAutoVerify runs a quick verification at the configured wall-clock
// time each day, on whichever replica wins the lock. Drift never stops
// serve; it is recorded, logged, and left for operators and verify runs.
func runAutoVerify(ctx context.Context, pool *pgxpool.Pool, r *verify.Runner, autoTime string, logger *slog.Logger) {
	for {
		wait := time.Until(nextAutoVerify(time.Now(), autoTime))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		conn, err := pool.Acquire(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("auto verify lock connection failed", "error", err)
			}
			continue
		}
		var leader bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(`+autoVerifyLockKey+`)`).Scan(&leader); err != nil {
			conn.Release()
			if ctx.Err() == nil {
				logger.Error("auto verify leader election failed", "error", err)
			}
			continue
		}
		if !leader {
			conn.Release()
			continue
		}
		report, err := r.Run(ctx, verify.Options{})
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(`+autoVerifyLockKey+`)`)
		conn.Release()
		switch {
		case err != nil:
			if ctx.Err() == nil {
				logger.Error("auto verify failed", "error", err)
			}
		case report.Drifted > 0:
			logger.Warn("nightly verify found drift",
				"checked", report.Checked, "drifted", report.Drifted)
		default:
			logger.Info("nightly verify clean", "checked", report.Checked)
		}
	}
}

// retentionInterval is how often the retention loop enforces the event
// window. Purging hourly instead of nightly keeps each delete small and
// makes the loop timezone-indifferent; deletion is idempotent.
const retentionInterval = time.Hour

// runRetention deletes processed events older than the configured window,
// along with their gap audit rows.
func runRetention(ctx context.Context, pool *pgxpool.Pool, days int, logger *slog.Logger) {
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()
	for {
		cutoff := time.Now().AddDate(0, 0, -days)
		purged, err := db.New(pool).PurgeOldEvents(ctx, cutoff)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				logger.Error("event retention purge failed", "error", err)
			}
		case purged > 0:
			logger.Info("event retention purged old events", "count", purged, "older_than", cutoff.Format(time.DateOnly))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runReaper periodically resurrects jobs whose worker died holding a
// claim, and refreshes the queue-depth gauge on the same cadence. It
// ticks at a quarter of the visibility timeout so a crashed claim is
// noticed soon after it expires.
func runReaper(ctx context.Context, q *queue.Queue, metrics *queue.Metrics, visibilityTimeout time.Duration, logger *slog.Logger) {
	tick := min(max(visibilityTimeout/4, time.Second), 30*time.Second)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resurrected, dead, err := q.Reap(ctx)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("reaper failed", "error", err)
				}
				continue
			}
			if resurrected > 0 {
				logger.Warn("resurrected expired job claims", "count", resurrected)
			}
			if dead > 0 {
				logger.Error("dead-lettered jobs with expired claims and exhausted budgets", "count", dead)
			}
			if err := metrics.SampleJobs(ctx, q); err != nil && ctx.Err() == nil {
				logger.Error("sampling jobs gauge failed", "error", err)
			}
		}
	}
}

// serveListener normalizes the graceful-close error to nil.
func serveListener(name string, srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}
