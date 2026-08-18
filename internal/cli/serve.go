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
)

const shutdownTimeout = 10 * time.Second

// workerPollInterval is the idle sleep between claim attempts when the
// queue is empty; claims themselves are immediate.
const workerPollInterval = 250 * time.Millisecond

func newServeCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
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
			return runServe(cmd, cfg, pool)
		},
	}
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

func runServe(cmd *cobra.Command, cfg *config.Config, pool *pgxpool.Pool) error {
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
	if cfg.Backfill.AutoResume {
		runner := backfill.NewRunner(pool, client, logger, backfill.NewMetrics(registry), nil)
		go resumeInterruptedBackfills(ctx, pool, runner, logger)
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
