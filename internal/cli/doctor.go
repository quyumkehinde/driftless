package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/store/db"
	"github.com/quyumkehinde/driftless/internal/store/migrations"
	"github.com/quyumkehinde/driftless/internal/stripeapi"
)

// restrictedKeyScopes is what doctor tells users to click when they run
// with a full-access key: read-only on exactly the object families the
// mirror consumes.
var restrictedKeyScopes = []string{
	"Customers", "Subscriptions", "Products", "Prices", "Invoices",
	"Charges", "PaymentIntents", "SetupIntents", "PaymentMethods",
	"Refunds", "Disputes", "Checkout Sessions", "Events",
}

// checkStatus is a doctor check's outcome.
type checkStatus string

const (
	checkOK   checkStatus = "ok"
	checkWarn checkStatus = "warn"
	checkFail checkStatus = "fail"
)

// checkResult is one doctor line: outcome plus detail.
type checkResult struct {
	name   string
	status checkStatus
	detail string
}

func newDoctorCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check the environment: database, migrations, Stripe key, webhook setup",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()

			results := runChecks(cmd.Context(), cfg, db.New(pool))
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			failed := false
			for _, r := range results {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", r.name, r.status, r.detail)
				if r.status == checkFail {
					failed = true
				}
			}
			if err := w.Flush(); err != nil {
				return err
			}
			if failed {
				return errors.New("doctor found problems")
			}
			return nil
		},
	}
}

// runChecks executes every doctor probe in dependency order and never
// stops early: the point is the complete picture.
func runChecks(ctx context.Context, cfg *config.Config, q *db.Queries) []checkResult {
	var results []checkResult
	check := func(name string, status checkStatus, detail string) {
		results = append(results, checkResult{name: name, status: status, detail: detail})
	}

	check(databaseCheck(ctx, cfg))
	check(migrationsCheck(ctx, cfg))

	accountID, livemode, apiErr := fetchAccount(ctx, cfg)
	if apiErr != nil {
		check("stripe api", checkFail, apiErr.Error())
	} else {
		mode := "test"
		if livemode {
			mode = "live"
		}
		check("stripe api", checkOK, fmt.Sprintf("key works, account %s (%s mode)", accountID, mode))
		check(accountGuardCheck(ctx, q, accountID, livemode))
	}

	if cfg.Stripe.WebhookSecret == "" {
		check("webhook secret", checkWarn, "not configured; serve will refuse to start")
	} else {
		check("webhook secret", checkOK, "configured")
	}

	if strings.HasPrefix(cfg.Stripe.APIKey, "sk_") {
		check("key scope", checkWarn,
			"full-access key; create a restricted read-only key (rk_...) with: "+strings.Join(restrictedKeyScopes, ", "))
	} else {
		check("key scope", checkOK, "restricted key")
	}

	check(webhookTrafficCheck(ctx, q))
	return results
}

func databaseCheck(ctx context.Context, cfg *config.Config) (string, checkStatus, string) {
	start := time.Now()
	sdb, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return "database", checkFail, err.Error()
	}
	defer func() { _ = sdb.Close() }()
	if err := sdb.PingContext(ctx); err != nil {
		return "database", checkFail, err.Error()
	}
	return "database", checkOK, fmt.Sprintf("connected in %s", time.Since(start).Round(time.Millisecond))
}

func migrationsCheck(ctx context.Context, cfg *config.Config) (string, checkStatus, string) {
	sdb, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return "migrations", checkFail, err.Error()
	}
	defer func() { _ = sdb.Close() }()
	pending, err := migrations.Pending(ctx, sdb)
	if err != nil {
		return "migrations", checkFail, err.Error()
	}
	if pending > 0 {
		return "migrations", checkFail, fmt.Sprintf("%d pending; run 'driftless migrate up'", pending)
	}
	return "migrations", checkOK, "schema current"
}

func fetchAccount(ctx context.Context, cfg *config.Config) (accountID string, livemode bool, err error) {
	limiter := stripeapi.NewLimiter(cfg.Stripe.APIRPS)
	defer limiter.Stop()
	client := newStripeClient(cfg, limiter)
	raw, err := client.GetAccount(ctx, stripeapi.PriorityWebhook)
	if err != nil {
		return "", false, err
	}
	var account struct {
		ID       string `json:"id"`
		Livemode bool   `json:"livemode"`
	}
	if err := json.Unmarshal(raw, &account); err != nil {
		return "", false, fmt.Errorf("account response: %w", err)
	}
	return account.ID, account.Livemode, nil
}

func accountGuardCheck(ctx context.Context, q *db.Queries, accountID string, livemode bool) (string, checkStatus, string) {
	meta, err := q.GetMeta(ctx)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "account guard", checkOK, "not initialized yet; the first serve or backfill records the account"
	case err != nil:
		return "account guard", checkFail, err.Error()
	}
	recorded := ""
	if meta.StripeAccountID != nil {
		recorded = *meta.StripeAccountID
	}
	if recorded != accountID || (meta.Livemode != nil && *meta.Livemode != livemode) {
		return "account guard", checkFail, fmt.Sprintf(
			"key is for %s (livemode=%v) but this database mirrors %s (livemode=%v)",
			accountID, livemode, recorded, meta.Livemode != nil && *meta.Livemode)
	}
	return "account guard", checkOK, fmt.Sprintf("matches %s", recorded)
}

// webhookTrafficCheck detects the classic wrong-URL misconfiguration:
// sweeps keep finding events Stripe generated, yet not one has ever
// arrived as a webhook.
func webhookTrafficCheck(ctx context.Context, q *db.Queries) (string, checkStatus, string) {
	counts, err := q.CountEventsBySource(ctx)
	if err != nil {
		return "webhook traffic", checkFail, err.Error()
	}
	if counts.Webhook == 0 && counts.Sweep > 0 {
		return "webhook traffic", checkWarn, fmt.Sprintf(
			"sweeps recovered %d events but no webhook has ever arrived; check the endpoint URL in the Stripe dashboard", counts.Sweep)
	}
	if counts.Webhook == 0 {
		return "webhook traffic", checkOK, "no events yet"
	}
	return "webhook traffic", checkOK, fmt.Sprintf("%d webhook events received", counts.Webhook)
}
