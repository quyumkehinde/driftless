package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/apply"
	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/ingest"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/queue"
	"github.com/quyumkehinde/driftless/internal/store/migrations"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

const shutdownTimeout = 10 * time.Second

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
	q := queue.New(pool, cfg.Workers.VisibilityTimeout.Std())
	ingestServer := ingest.NewServer(pool, q, verifier, logger, ingest.NewMetrics(registry))

	limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
	defer limiter.Stop()
	client := stripeapi.New(cfg.Stripe.APIBaseURL, cfg.Stripe.APIKey, limiter, stripeapi.NewMetrics(registry, limiter))
	engine := apply.NewEngine(pool, client, logger, apply.NewMetrics(registry))
	workers := queue.NewWorkerPool(q, engine, cfg.Workers.Count,
		250*time.Millisecond, logger, queue.NewMetrics(registry))

	latestMigration, err := migrations.LatestVersion()
	if err != nil {
		return err
	}
	readyz := obs.Readyz(
		obs.Check{Name: "postgres", Run: pool.Ping},
		obs.Check{Name: "migrations", Run: func(ctx context.Context) error {
			var applied int64
			err := pool.QueryRow(ctx,
				`SELECT coalesce(max(version_id), 0) FROM public.goose_db_version`).Scan(&applied)
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

// serveListener normalizes the graceful-close error to nil.
func serveListener(name string, srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}
