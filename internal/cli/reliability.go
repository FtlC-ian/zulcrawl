package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/lock"
	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/spf13/cobra"
)

func excludedStream(cfg *config.Config, name string) bool {
	if name == "direct-messages" {
		return false
	}
	for _, s := range cfg.Sync.ExcludeStreams {
		if strings.EqualFold(strings.TrimSpace(s), name) {
			return true
		}
	}
	if len(cfg.Sync.Streams) > 0 {
		for _, s := range cfg.Sync.Streams {
			if strings.EqualFold(strings.TrimSpace(s), name) {
				return false
			}
		}
		return true
	}
	return false
}

func healthReport(ctx context.Context, st *store.Store, cfg *config.Config) (store.HealthReport, error) {
	maxAge, err := cfg.SyncStaleAfter()
	if err != nil {
		return store.HealthReport{}, err
	}
	report, err := st.HealthWithMaxAge(ctx, maxAge)
	for i := range report.Streams {
		report.Streams[i].Excluded = excludedStream(cfg, report.Streams[i].Name)
	}
	return report, err
}

func printHealth(cmd *cobra.Command, report store.HealthReport, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
	}
	for _, h := range report.Streams {
		state := "fresh"
		if h.Quiet {
			state = "fresh (quiet)"
		}
		if h.Stale {
			state = "stale"
		}
		if h.Excluded {
			state = "excluded"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s: %s, %d messages, newest=%s, last_success=%s, last_attempt=%s, last_error=%q, quiet=%t, checkpoint_missing=%t, repair_anchor=%s, repair_completed_at=%s\n", h.Name, state, h.MessageCount, h.NewestMessageAt, h.LastSuccess, h.LastAttempt, h.LastError, h.Quiet, h.CheckpointMissing, h.RepairAnchor, h.RepairCompletedAt)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Recent sync failures: %d (maximum 200)\n", len(report.Failures))
	return nil
}

func statusCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{Use: "status", Short: "Show local sync freshness, coverage and recent failures", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := loadCfg()
		if err != nil {
			return err
		}
		l, err := lock.Acquire(cfg.DBPath() + ".lock")
		if err != nil {
			return err
		}
		defer l.Release()
		st, err := store.Open(cfg.DBPath())
		if err != nil {
			return err
		}
		defer st.Close()
		if err = st.InitSchema(cmd.Context()); err != nil {
			return err
		}
		report, err := healthReport(cmd.Context(), st, cfg)
		if err != nil {
			return err
		}
		return printHealth(cmd, report, asJSON)
	}}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit machine-readable sync health")
	return cmd
}

func explainEmpty(cmd *cobra.Command, cfg *config.Config, st *store.Store, stream string) error {
	if stream != "" && excludedStream(cfg, stream) {
		fmt.Fprintln(cmd.ErrOrStderr(), "No results: stream is excluded by sync config; stored data may be incomplete.")
		return nil
	}
	report, err := healthReport(cmd.Context(), st, cfg)
	if err != nil {
		return err
	}
	var count int64
	var stale bool
	var found bool
	for _, h := range report.Streams {
		if stream == "" || strings.EqualFold(h.Name, stream) {
			count += h.MessageCount
			stale = stale || h.Stale
			found = true
		}
	}
	switch {
	case !found || count == 0:
		fmt.Fprintln(cmd.ErrOrStderr(), "No results: no local messages for this scope; run sync and check status for coverage.")
	case stale:
		fmt.Fprintln(cmd.ErrOrStderr(), "No results: archive is stale or a sync failed; run sync and check status. No local FTS match for the query and filters.")
	default:
		fmt.Fprintln(cmd.ErrOrStderr(), "No results: no FTS match for the query and filters in the local archive.")
	}
	return nil
}
