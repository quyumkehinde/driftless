package cli

import (
	"fmt"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func newJobsCmd(flags *rootFlags) *cobra.Command {
	jobsCmd := &cobra.Command{
		Use:   "jobs",
		Short: "Inspect and retry queue jobs",
	}
	jobsCmd.AddCommand(newJobsListCmd(flags), newJobsRetryCmd(flags))
	return jobsCmd
}

func newJobsListCmd(flags *rootFlags) *cobra.Command {
	var status string
	var limit int32
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List queue jobs by status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pool, q, err := openQueue(cmd, flags)
			if err != nil {
				return err
			}
			defer pool.Close()

			jobs, err := q.List(cmd.Context(), status, limit)
			if err != nil {
				return fmt.Errorf("list jobs: %w", err)
			}
			if len(jobs) == 0 {
				cmd.Printf("no %s jobs\n", status)
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "ID\tOBJECT\tATTEMPTS\tRUN AFTER\tLAST ERROR")
			for _, j := range jobs {
				lastError := ""
				if j.LastError != nil {
					lastError = truncate(*j.LastError, 60)
				}
				_, _ = fmt.Fprintf(w, "%d\t%s:%s\t%d/%d\t%s\t%s\n",
					j.ID, j.ObjectType, j.ObjectID, j.Attempts, j.MaxAttempts,
					j.RunAfter.UTC().Format(time.RFC3339), lastError)
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&status, "status", "dead", "job status to list: pending|running|done|dead")
	cmd.Flags().Int32Var(&limit, "limit", 50, "maximum rows to show")
	return cmd
}

func newJobsRetryCmd(flags *rootFlags) *cobra.Command {
	var allDead bool
	cmd := &cobra.Command{
		Use:   "retry [job-id]",
		Short: "Requeue dead jobs with a fresh attempt budget",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if allDead == (len(args) == 1) {
				return &usageError{err: fmt.Errorf("pass exactly one of --all-dead or a job id")}
			}

			pool, q, err := openQueue(cmd, flags)
			if err != nil {
				return err
			}
			defer pool.Close()

			if allDead {
				n, err := q.RetryAllDead(cmd.Context())
				if err != nil {
					return fmt.Errorf("retry dead jobs: %w", err)
				}
				cmd.Printf("requeued %d dead job(s)\n", n)
				return nil
			}

			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return &usageError{err: fmt.Errorf("invalid job id %q", args[0])}
			}
			retried, err := q.RetryOne(cmd.Context(), id)
			if err != nil {
				return fmt.Errorf("retry job %d: %w", id, err)
			}
			if !retried {
				return fmt.Errorf("job %d is not dead or does not exist", id)
			}
			cmd.Printf("requeued job %d\n", id)
			return nil
		},
	}
	cmd.Flags().BoolVar(&allDead, "all-dead", false, "requeue every dead job")
	return cmd
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
