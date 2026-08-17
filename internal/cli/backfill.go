package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/backfill"
	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/obs"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

func newBackfillCmd(flags *rootFlags) *cobra.Command {
	var (
		full   bool
		since  string
		types  []string
		resume int64
		dryRun bool
	)
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Import history from the Stripe API into the mirror",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts, err := backfillOptions(full, since, types, resume, dryRun)
			if err != nil {
				return err
			}

			if dryRun {
				planned, err := backfill.Plan(opts.Types)
				if err != nil {
					return &usageError{err: err}
				}
				cmd.Println("backfill plan (dry run, nothing fetched):")
				for i, objectType := range planned {
					cmd.Printf("  %2d. %s\n", i+1, objectType)
				}
				if opts.Since != nil {
					cmd.Printf("  filter: created >= %s\n", opts.Since.Format(time.RFC3339))
				} else {
					cmd.Println("  filter: full history")
				}
				cmd.Println("  pages of 100 per call; call count depends on account size")
				return nil
			}

			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()

			limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
			defer limiter.Stop()
			client := stripeapi.New(cfg.Stripe.APIBaseURL, cfg.Stripe.APIKey, limiter, nil)
			logger, err := buildLogger(cmd, cfg)
			if err != nil {
				return err
			}
			runner := backfill.NewRunner(pool, client, logger, nil,
				func(objectType string, pages, objects int64) {
					cmd.Printf("%-18s pages=%-5d objects=%d\n", objectType, pages, objects)
				})

			// ctrl-c is a deliberate stop: mark the run cancelled so serve's
			// auto_resume leaves it alone; only an explicit --resume revives it
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			var runID int64
			if resume > 0 {
				runID = resume
				err = runner.Resume(ctx, resume)
			} else {
				runID, err = runner.Start(ctx, *opts)
			}
			if err == nil {
				cmd.Printf("backfill run %d complete\n", runID)
				return nil
			}
			if ctx.Err() != nil && runID > 0 {
				cancelled, cancelErr := runner.Cancel(context.WithoutCancel(ctx), runID)
				if cancelErr != nil {
					return fmt.Errorf("backfill run %d interrupted, and marking it cancelled failed: %w", runID, cancelErr)
				}
				if cancelled {
					return fmt.Errorf("backfill run %d cancelled; resume with: driftless backfill --resume %d", runID, runID)
				}
			}
			return fmt.Errorf("backfill run %d: %w (resume with: driftless backfill --resume %d)", runID, err, runID)
		},
	}
	cmd.Flags().BoolVar(&full, "full", false, "all history (default when no --since)")
	cmd.Flags().StringVar(&since, "since", "", "only objects created on or after this date (2024-01-31 or RFC3339)")
	cmd.Flags().StringSliceVar(&types, "type", nil, "subset of object types, comma separated")
	cmd.Flags().Int64Var(&resume, "resume", 0, "resume an interrupted run by id")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the plan without fetching")
	return cmd
}

// backfillOptions validates the flag combination into runner options.
func backfillOptions(full bool, since string, types []string, resume int64, dryRun bool) (*backfill.Options, error) {
	if resume > 0 && (full || since != "" || len(types) > 0 || dryRun) {
		return nil, &usageError{err: fmt.Errorf("--resume cannot be combined with other backfill flags")}
	}
	if full && since != "" {
		return nil, &usageError{err: fmt.Errorf("--full and --since are mutually exclusive")}
	}

	opts := &backfill.Options{RequestedBy: "cli"}
	if since != "" {
		parsed, err := parseSince(since)
		if err != nil {
			return nil, &usageError{err: err}
		}
		opts.Since = &parsed
	}
	for _, raw := range types {
		normalized, err := normalizeObjectType(raw)
		if err != nil {
			return nil, &usageError{err: err}
		}
		opts.Types = append(opts.Types, normalized)
	}
	// surface unlistable types (like subscription_item, which arrives via
	// its parent) as flag errors rather than mid-run failures
	if _, err := backfill.Plan(opts.Types); err != nil {
		return nil, &usageError{err: err}
	}
	return opts, nil
}

func parseSince(raw string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02", time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid --since %q: use 2024-01-31 or RFC3339", raw)
}

// normalizeObjectType accepts the documented plural spellings alongside the
// canonical singular ones.
func normalizeObjectType(raw string) (string, error) {
	candidate := strings.TrimSpace(raw)
	for _, objectType := range stripeapi.AllObjectTypes {
		if candidate == objectType || candidate == objectType+"s" {
			return objectType, nil
		}
	}
	return "", fmt.Errorf("unknown object type %q", raw)
}

// buildLogger builds the command's logger from the effective log config.
func buildLogger(cmd *cobra.Command, cfg *config.Config) (*slog.Logger, error) {
	return obs.NewLogger(cmd.ErrOrStderr(), cfg.Log.Level, cfg.Log.Format)
}
