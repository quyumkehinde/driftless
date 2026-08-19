package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/quyumkehinde/driftless/internal/config"
	"github.com/quyumkehinde/driftless/internal/store/db"
)

func newEventsCmd(flags *rootFlags) *cobra.Command {
	eventsCmd := &cobra.Command{
		Use:   "events",
		Short: "Inspect stored events",
	}
	eventsCmd.AddCommand(newEventsShowCmd(flags))
	return eventsCmd
}

func newEventsShowCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "show <event-id>",
		Short: "Dump one stored event: metadata and raw payload",
		Long: "Prints the stored event's metadata (type, timestamps, source, processing\n" +
			"state) followed by its raw payload, pretty-printed. This is the intended\n" +
			"way to inspect payloads; log output never contains them.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, pool, err := openPool(cmd, flags, config.ScopeDefault)
			if err != nil {
				return err
			}
			defer pool.Close()

			event, err := db.New(pool).GetEventByID(cmd.Context(), args[0])
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("no stored event %q", args[0])
			}
			if err != nil {
				return err
			}

			cmd.Printf("event_id      %s\n", event.EventID)
			cmd.Printf("type          %s\n", event.Type)
			if event.ApiVersion != nil {
				cmd.Printf("api_version   %s\n", *event.ApiVersion)
			}
			cmd.Printf("created       %s\n", event.Created.UTC().Format(time.RFC3339))
			cmd.Printf("received_at   %s\n", event.ReceivedAt.UTC().Format(time.RFC3339))
			cmd.Printf("source        %s\n", event.Source)
			cmd.Printf("livemode      %v\n", event.Livemode)
			if event.ProcessedAt != nil {
				cmd.Printf("processed_at  %s\n", event.ProcessedAt.UTC().Format(time.RFC3339))
			} else {
				cmd.Println("processed_at  pending")
			}

			var pretty bytes.Buffer
			if err := json.Indent(&pretty, event.Payload, "", "  "); err != nil {
				// dump verbatim rather than hide a payload that will not indent
				cmd.Println(string(event.Payload))
				return nil
			}
			cmd.Println(pretty.String())
			return nil
		},
	}
}
