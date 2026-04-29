package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/lock"
	"github.com/FtlC-ian/zulcrawl/internal/search"
	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/syncer"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

func NewRootCmd() *cobra.Command {
	var cfgPath string
	root := &cobra.Command{
		Use:   "zulcrawl",
		Short: "Mirror Zulip data into local SQLite with FTS5 search",
	}
	root.PersistentFlags().StringVar(&cfgPath, "config", "", "config file path (default ~/.zulcrawl/config.toml)")

	loadCfg := func() (*config.Config, error) {
		return config.Load(cfgPath)
	}

	root.AddCommand(initCmd(&cfgPath, loadCfg))
	root.AddCommand(doctorCmd(loadCfg))
	root.AddCommand(syncCmd(loadCfg))
	root.AddCommand(searchCmd(loadCfg))
	root.AddCommand(topicsCmd(loadCfg))
	root.AddCommand(statsCmd(loadCfg))
	root.AddCommand(sqlCmd(loadCfg))
	root.AddCommand(messagesCmd(loadCfg))
	return root
}

func initCmd(cfgPath *string, loadCfg func() (*config.Config, error)) *cobra.Command {
	var urlOverride string
	var fromOpenClaw string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize config and database",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if fromOpenClaw != "" {
				if err := applyOpenClawConfig(fromOpenClaw, cfg); err != nil {
					return err
				}
			}
			if urlOverride != "" {
				cfg.Zulip.URL = urlOverride
			}
			cfg.Normalize()
			if err := cfg.ValidateAuth(); err != nil {
				return err
			}

			api := zulip.NewClient(cfg.Zulip.URL, cfg.Zulip.Email, cfg.Zulip.APIKey)
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			settings, err := api.ServerSettings(ctx)
			if err != nil {
				return err
			}

			dbPath := cfg.DBPath()
			if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
				return err
			}
			st, err := store.Open(dbPath)
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}
			if err := st.UpsertOrganization(ctx, 1, cfg.Zulip.URL, settings.RealmName); err != nil {
				return err
			}
			if err := cfg.Save(*cfgPath); err != nil {
				return err
			}
			fmt.Printf("Initialized %s\n", config.ExpandPath(config.DefaultPath()))
			fmt.Printf("DB: %s\n", dbPath)
			fmt.Printf("Org: %s (%s)\n", settings.RealmName, settings.RealmURI)
			return nil
		},
	}
	cmd.Flags().StringVar(&urlOverride, "url", "", "zulip base URL override")
	cmd.Flags().StringVar(&fromOpenClaw, "from-openclaw", "", "import auth from openclaw json")
	return cmd
}

func doctorCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Validate config, auth, and DB",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if err := cfg.ValidateAuth(); err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 40*time.Second)
			defer cancel()
			api := zulip.NewClient(cfg.Zulip.URL, cfg.Zulip.Email, cfg.Zulip.APIKey)
			streams, err := api.Streams(ctx)
			if err != nil {
				return fmt.Errorf("zulip auth/api check failed: %w", err)
			}

			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}
			if err := st.Ping(ctx); err != nil {
				return err
			}
			fmt.Println("doctor: OK")
			fmt.Printf("  URL: %s\n", cfg.Zulip.URL)
			fmt.Printf("  Streams visible: %d\n", len(streams))
			fmt.Printf("  DB: %s\n", cfg.DBPath())
			return nil
		},
	}
}

func syncCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var full bool
	var since string
	var streams string
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync streams/messages from Zulip",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if err := cfg.ValidateAuth(); err != nil {
				return err
			}
			ctx := cmd.Context()
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}

			// Acquire an exclusive lock so two concurrent sync processes do not
			// write the same archive simultaneously.
			lockPath := cfg.DBPath() + ".lock"
			l, err := lock.Acquire(lockPath)
			if err != nil {
				return err
			}
			defer l.Release()

			api := zulip.NewClient(cfg.Zulip.URL, cfg.Zulip.Email, cfg.Zulip.APIKey)
			sy := syncer.New(cfg, api, st)
			var include []string
			if streams != "" {
				for _, s := range strings.Split(streams, ",") {
					s = strings.TrimSpace(s)
					if s != "" {
						include = append(include, s)
					}
				}
			}
			if since != "" {
				if _, err := time.Parse("2006-01-02", since); err != nil {
					return fmt.Errorf("--since must be YYYY-MM-DD")
				}
				since = since + "T00:00:00Z"
			}

			start := time.Now()
			if err := sy.Sync(ctx, syncer.Options{Full: full, Streams: include, Since: since}); err != nil {
				return err
			}
			fmt.Printf("sync complete in %s\n", time.Since(start).Round(time.Millisecond))
			return nil
		},
	}
	cmd.Flags().BoolVar(&full, "full", false, "full backfill")
	cmd.Flags().StringVar(&streams, "streams", "", "comma-separated stream names")
	cmd.Flags().StringVar(&since, "since", "", "only include messages since date (YYYY-MM-DD)")
	return cmd
}

func searchCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var stream string
	var resolved bool
	var limit int
	cmd := &cobra.Command{
		Use:   "search [query]",
		Short: "FTS search over mirrored messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			hits, err := st.Search(cmd.Context(), args[0], stream, resolved, limit)
			if err != nil {
				return err
			}
			fmt.Println(search.FormatHits(hits))
			return nil
		},
	}
	cmd.Flags().StringVar(&stream, "stream", "", "stream filter")
	cmd.Flags().BoolVar(&resolved, "resolved", false, "only resolved topics")
	cmd.Flags().IntVar(&limit, "limit", 20, "result limit")
	return cmd
}

func topicsCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var stream string
	var unresolved bool
	var limit int
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics with stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			rows, err := st.ListTopics(cmd.Context(), stream, unresolved, limit)
			if err != nil {
				return err
			}
			for _, r := range rows {
				res := ""
				if r.Resolved {
					res = " [resolved]"
				}
				fmt.Printf("#%s > %s%s (%d msgs, last %s)\n", r.StreamName, r.TopicName, res, r.MessageCount, r.LastMessageAt)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&stream, "stream", "", "stream name")
	cmd.Flags().BoolVar(&unresolved, "unresolved", false, "only unresolved topics")
	cmd.Flags().IntVar(&limit, "limit", 100, "row limit")
	return cmd
}

func statsCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Database stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			x, err := st.Stats(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("streams: %d\n", x.Streams)
			fmt.Printf("topics: %d\n", x.Topics)
			fmt.Printf("users: %d\n", x.Users)
			fmt.Printf("messages: %d\n", x.Messages)
			fmt.Printf("db_size_bytes: %d\n", x.DBSize)
			return nil
		},
	}
}

func sqlCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "sql [query]",
		Short: "Run raw SQL query",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			rows, err := st.Query(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			fmt.Println(strings.Join(cols, "\t"))
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					return err
				}
				out := make([]string, len(cols))
				for i, v := range vals {
					out[i] = fmt.Sprint(v)
				}
				fmt.Println(strings.Join(out, "\t"))
			}
			return rows.Err()
		},
	}
}

func messagesCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var stream, topic, sender, since, until string
	var days, hours, last, limit int
	var all bool

	cmd := &cobra.Command{
		Use:   "messages",
		Short: "Query archived messages from the local SQLite DB",
		Long: `Return an exact archive slice from the local SQLite database.

At least one narrowing filter is required to avoid accidental full-archive
dumps. Use --stream, --topic, --sender, --days, --hours, --since, or --until.
The default safety limit is 200 messages; use --limit or --all to override.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()

			// Require at least one narrowing filter.
			if stream == "" && topic == "" && sender == "" &&
				since == "" && until == "" && days == 0 && hours == 0 &&
				last == 0 && !all {
				return fmt.Errorf(
					"at least one narrowing filter is required\n" +
						"  e.g. --stream general --days 7\n" +
						"  Use --all to fetch the entire archive (may be very large)")
			}

			if days < 0 {
				return fmt.Errorf("--days must be positive")
			}
			if hours < 0 {
				return fmt.Errorf("--hours must be positive")
			}
			if last < 0 {
				return fmt.Errorf("--last must be positive")
			}
			if limit <= 0 && !all {
				return fmt.Errorf("--limit must be positive unless --all is set")
			}

			// Validate --since / --until formats and normalize all values to the
			// UTC RFC3339 strings stored in SQLite. Lexicographic comparisons only
			// work correctly when equivalent instants use the same zone.
			for flagName, val := range map[string]string{"since": since, "until": until} {
				if val == "" {
					continue
				}
				if t, err := time.Parse(time.RFC3339, val); err == nil {
					if flagName == "since" {
						since = t.UTC().Format(time.RFC3339)
					} else {
						until = t.UTC().Format(time.RFC3339)
					}
					continue
				}
				if _, err := time.Parse("2006-01-02", val); err != nil {
					return fmt.Errorf("--%s must be RFC3339 (2006-01-02T15:04:05Z) or YYYY-MM-DD, got %q", flagName, val)
				}
				if flagName == "since" {
					since = val + "T00:00:00Z"
				} else {
					until = val + "T23:59:59Z"
				}
			}

			f := store.MessagesFilter{
				Stream: stream,
				Topic:  topic,
				Sender: sender,
				Since:  since,
				Until:  until,
				Days:   days,
				Hours:  hours,
				Last:   last,
				Limit:  limit,
			}
			if all {
				f.Limit = -1 // signal to store: no cap
			}

			msgs, err := st.QueryMessages(cmd.Context(), f)
			if err != nil {
				return err
			}
			if len(msgs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No messages found.")
				return nil
			}
			for _, m := range msgs {
				timeText := m.Timestamp
				if t, err2 := time.Parse(time.RFC3339, m.Timestamp); err2 == nil {
					timeText = t.Format("2006-01-02 15:04:05")
				}
				fmt.Fprintf(cmd.OutOrStdout(), "[%s] #%s > %s | %s\n  %s\n\n",
					timeText, m.StreamName, m.TopicName, m.SenderName, m.Content)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "(%d messages)\n", len(msgs))
			return nil
		},
	}

	cmd.Flags().StringVar(&stream, "stream", "", "filter by stream name")
	cmd.Flags().StringVar(&topic, "topic", "", "filter by topic name (exact match)")
	cmd.Flags().StringVar(&sender, "sender", "", "filter by sender full name (substring match)")
	cmd.Flags().StringVar(&since, "since", "", "only messages at or after this time (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().StringVar(&until, "until", "", "only messages at or before this time (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().IntVar(&days, "days", 0, "only messages from the last N days")
	cmd.Flags().IntVar(&hours, "hours", 0, "only messages from the last N hours")
	cmd.Flags().IntVar(&last, "last", 0, "return the N most recent messages (oldest-first output)")
	cmd.Flags().IntVar(&limit, "limit", 200, "maximum messages to return (default 200)")
	cmd.Flags().BoolVar(&all, "all", false, "remove safety limit and return all matching messages")
	return cmd
}

func applyOpenClawConfig(path string, cfg *config.Config) error {
	b, err := os.ReadFile(config.ExpandPath(path))
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	// Look specifically at channels.zulip in OpenClaw config structure.
	zulipCfg := nestedMap(m, "channels", "zulip")
	if zulipCfg == nil {
		// Fallback: try flat keys (non-OpenClaw config files).
		zulipCfg = m
	}
	if cfg.Zulip.URL == "" {
		cfg.Zulip.URL = flatString(zulipCfg, "url")
	}
	if cfg.Zulip.Email == "" {
		cfg.Zulip.Email = flatString(zulipCfg, "email")
	}
	if cfg.Zulip.APIKey == "" {
		cfg.Zulip.APIKey = flatString(zulipCfg, "apiKey", "api_key")
	}
	return nil
}

func nestedMap(m map[string]any, keys ...string) map[string]any {
	cur := m
	for _, k := range keys {
		v, ok := cur[k]
		if !ok {
			return nil
		}
		sub, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		cur = sub
	}
	return cur
}

func flatString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}
