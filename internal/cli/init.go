package cli

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/backfill"
	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/store/migrations"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

func newInitCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "First-run setup: check the environment, migrate, offer a backfill",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()
			cmd.Println("checks:")
			cmd.Println("  database: reachable")

			limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
			defer limiter.Stop()
			client := newStripeClient(cfg, limiter)
			account, err := client.GetAccount(cmd.Context(), stripeapi.PriorityWebhook)
			if err != nil {
				return fmt.Errorf("stripe api key check failed: %w", err)
			}
			var acct struct {
				ID       string `json:"id"`
				Livemode bool   `json:"livemode"`
			}
			if err := json.Unmarshal(account, &acct); err != nil {
				return fmt.Errorf("stripe account response: %w", err)
			}
			cmd.Printf("  stripe: key works, account %s (livemode=%v)\n", acct.ID, acct.Livemode)
			if cfg.Stripe.WebhookSecret == "" {
				cmd.Println("  warning: stripe.webhook_secret is not set; serve will refuse to start without it")
			}

			sdb, err := sql.Open("pgx", cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer func() { _ = sdb.Close() }()
			applied, err := migrations.Up(cmd.Context(), sdb)
			if err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			if len(applied) == 0 {
				cmd.Println("  migrations: up to date")
			} else {
				cmd.Printf("  migrations: applied %d\n", len(applied))
			}

			cmd.Print("run a full backfill now? [y/N]: ")
			answer, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
			if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
				cmd.Println("skipped; run it later with: driftless backfill --full")
				return nil
			}

			runner, err := newBackfillRunner(cmd, cfg, pool, client)
			if err != nil {
				return err
			}
			runID, err := runner.Start(cmd.Context(), backfill.Options{RequestedBy: "auto-init"})
			if err != nil {
				return resumeHint(runID, err)
			}
			cmd.Printf("backfill run %d complete; driftless is ready\n", runID)
			return nil
		},
	}
}
