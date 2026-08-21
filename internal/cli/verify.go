package cli

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
	"github.com/quyumkehinde/driftless/internal/verify"
)

func newVerifyCmd(flags *rootFlags) *cobra.Command {
	var (
		quick  bool
		full   bool
		types  []string
		since  string
		repair bool
		format string
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Reconcile the mirror against Stripe and report drift",
		Long: "Verify re-reads objects from Stripe and compares them against the mirror,\n" +
			"reporting every divergence: objects Stripe has that the mirror lacks,\n" +
			"objects whose stored data differs, and live mirror rows Stripe no longer\n" +
			"has. Full mode walks everything; quick mode walks the last day and\n" +
			"spot-checks a random sample of older history.\n" +
			"\n" +
			"Exit codes are the CI contract: 0 means no drift, 3 means drift was found\n" +
			"(even if --repair fixed it), 4 means migrations are pending. Every run is\n" +
			"recorded in the verifications history table.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts, err := verifyOptions(quick, full, types, since, repair)
			if err != nil {
				return err
			}
			if format != "table" && format != "json" {
				return &usageError{err: fmt.Errorf("invalid --format %q: use table or json", format)}
			}

			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := refuseIfMigrationsPending(cmd.Context(), cfg); err != nil {
				return err
			}

			limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
			defer limiter.Stop()
			client := newStripeClient(cfg, limiter)
			logger, err := buildLogger(cmd, cfg)
			if err != nil {
				return err
			}

			var progress verify.Progress
			if format == "table" {
				progress = func(objectType stripeapi.ObjectType, checked, drifted int) {
					cmd.Printf("%-18s checked=%-6d drifted=%d\n", objectType, checked, drifted)
				}
			}
			report, err := verify.NewRunner(pool, client, logger, progress).Run(cmd.Context(), *opts)
			if err != nil {
				return err
			}

			if format == "json" {
				encoded, err := json.MarshalIndent(report, "", "  ")
				if err != nil {
					return err
				}
				cmd.Println(string(encoded))
			} else {
				printReport(cmd, report)
			}
			if report.Drifted > 0 {
				return &DriftError{Drifted: report.Drifted, Repaired: report.Repaired}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&full, "full", false, "exhaustive walk comparing every object (default)")
	cmd.Flags().BoolVar(&quick, "quick", false, "recent-window walk plus random spot-checks per type")
	cmd.Flags().StringSliceVar(&types, "type", nil, "subset of object types, comma separated")
	cmd.Flags().StringVar(&since, "since", "", "only objects created on or after this date (2024-01-31 or RFC3339)")
	cmd.Flags().BoolVar(&repair, "repair", false, "re-fetch and fix drifted objects")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table|json")
	return cmd
}

// verifyOptions validates the flag combination into runner options.
func verifyOptions(quick, full bool, types []string, since string, repair bool) (*verify.Options, error) {
	if quick && full {
		return nil, &usageError{err: fmt.Errorf("--quick and --full are mutually exclusive")}
	}
	// full is the default: exhaustive is the mode whose clean exit proves
	// the mirror; quick is the cheap opt-in for schedules
	opts := &verify.Options{Full: full || !quick, Repair: repair}
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
	// surface unverifiable types as flag errors rather than mid-run failures
	if _, err := verify.Plan(opts.Types); err != nil {
		return nil, &usageError{err: err}
	}
	return opts, nil
}

// printReport renders the human-readable summary.
func printReport(cmd *cobra.Command, report *verify.Report) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TYPE\tCHECKED\tDRIFTED\tREPAIRED")
	for _, tr := range report.Types {
		_, _ = fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", tr.ObjectType, tr.Checked, tr.Drifted, tr.Repaired)
	}
	_, _ = fmt.Fprintf(w, "total\t%d\t%d\t%d\n", report.Checked, report.Drifted, report.Repaired)
	_ = w.Flush()

	if len(report.Drifts) > 0 {
		cmd.Println("\ndrifted objects:")
		for _, d := range report.Drifts {
			suffix := ""
			if d.Repaired {
				suffix = " (repaired)"
			}
			cmd.Printf("  %s %s: %s%s\n", d.ObjectType, d.ObjectID, d.Kind, suffix)
		}
	}
	if report.Drifted == 0 {
		cmd.Println("no drift found")
	}
}
