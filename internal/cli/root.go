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
	"github.com/FtlC-ian/zulcrawl/internal/embed"
	"github.com/FtlC-ian/zulcrawl/internal/embeddings"
	"github.com/FtlC-ian/zulcrawl/internal/lock"
	"github.com/FtlC-ian/zulcrawl/internal/media"
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
	root.AddCommand(attachmentsCmd(loadCfg))
	root.AddCommand(backfillIndexesCmd(loadCfg))
	root.AddCommand(embeddingsCmd(loadCfg))
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
	var quiet bool
	var withMedia bool
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

			// Acquire the exclusive lock immediately after auth validation and
			// before any DB open / migration work, so two concurrent sync
			// invocations cannot race on schema migration, reindex, or setup.
			lockPath := cfg.DBPath() + ".lock"
			l, err := lock.Acquire(lockPath)
			if err != nil {
				return err
			}
			defer l.Release()

			ctx := cmd.Context()
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}

			api := zulip.NewClient(cfg.Zulip.URL, cfg.Zulip.Email, cfg.Zulip.APIKey)
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

			var sy *syncer.Syncer
			if quiet {
				sy = syncer.NewWithLogger(cfg, api, st, nil)
			} else {
				sy = syncer.New(cfg, api, st)
			}

			start := time.Now()
			if err := sy.Sync(ctx, syncer.Options{Full: full, Streams: include, Since: since}); err != nil {
				return err
			}
			if withMedia {
				res, err := media.FetchPending(ctx, st, api, media.FetchOptions{CacheDir: cfg.MediaCacheDir()})
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "media fetch: %d fetched, %d skipped, %d failed\n", res.Fetched, res.Skipped, res.Failed)
			}
			fmt.Printf("sync complete in %s\n", time.Since(start).Round(time.Millisecond))
			return nil
		},
	}
	cmd.Flags().BoolVar(&full, "full", false, "full backfill")
	cmd.Flags().StringVar(&streams, "streams", "", "comma-separated stream names")
	cmd.Flags().StringVar(&since, "since", "", "only include messages since date (YYYY-MM-DD)")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "suppress progress output (only print final summary or errors)")
	cmd.Flags().BoolVar(&withMedia, "with-media", false, "download indexed Zulip attachment media into the local cache after sync")
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
		Short: "List topics or search topics by content",
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
	cmd.AddCommand(topicsSearchCmd(loadCfg))
	return cmd
}

func topicsSearchCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var stream string
	var unresolved bool
	var limit int
	var semantic bool
	cmd := &cobra.Command{
		Use:   "search [query]",
		Short: "Hybrid topic-level search (FTS on topic names + message content)",
		Long: `Search topics by name and by the content of their messages.

Results are ranked by:
  - FTS relevance (BM25) on topic name and/or message text
  - Activity bonus: more messages → slightly higher rank
  - Recency: recently-active topics rank higher
  - Resolved bonus: resolved topics get a small lift

This is a purely local FTS/hybrid search. No remote embedding APIs are called.

When --semantic is set, semantic/vector scoring participates alongside FTS.
This requires embeddings to be enabled and backfilled:
  1. Set [embeddings] enabled = true in config
  2. ollama pull nomic-embed-text-v2-moe
  3. zulcrawl embeddings backfill

FTS-only behaviour is the default and remains unchanged.`,
		Args: cobra.ExactArgs(1),
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

			// --- Semantic mode ---------------------------------------------------
			if semantic {
				if !cfg.Embeddings.Enabled {
					return fmt.Errorf(
						"semantic search requires embeddings to be enabled.\n"+
							"Set [embeddings] enabled = true in config, then run:\n"+
							"  ollama pull %s\n"+
							"  zulcrawl embeddings backfill",
						cfg.Embeddings.Model)
				}
				if err := cfg.ValidateEmbeddings(); err != nil {
					return err
				}
				client := embed.NewOllamaClient(
					cfg.Embeddings.OllamaBase,
					cfg.Embeddings.Model,
					cfg.Embeddings.BatchSize,
				)
				ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
				defer cancel()
				if err := client.CheckAvailable(ctx); err != nil {
					return err
				}
				hits, err := embeddings.SemanticSearch(ctx, st, client, embeddings.SemanticSearchOptions{
					Query:          args[0],
					Stream:         stream,
					Limit:          limit,
					OnlyUnresolved: unresolved,
				})
				if err != nil {
					return err
				}
				if len(hits) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "No topics found.")
					return nil
				}
				for _, h := range hits {
					res := ""
					if h.Resolved {
						res = " [resolved]"
					}
					lastAt := h.LastMessageAt
					if t, err2 := time.Parse(time.RFC3339, h.LastMessageAt); err2 == nil {
						lastAt = t.Format("2006-01-02")
					}
					fmt.Fprintf(cmd.OutOrStdout(), "#%s > %s%s (%d msgs, last %s, score %.3f)\n",
						h.StreamName, h.TopicName, res, h.MessageCount, lastAt, h.Score)
				}
				return nil
			}

			// --- FTS mode (default) ---------------------------------------------
			hits, err := st.SearchTopics(cmd.Context(), store.TopicSearchOptions{
				Query:          args[0],
				Stream:         stream,
				Limit:          limit,
				OnlyUnresolved: unresolved,
			})
			if err != nil {
				return err
			}
			if len(hits) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No topics found.")
				return nil
			}
			for _, h := range hits {
				res := ""
				if h.Resolved {
					res = " [resolved]"
				}
				lastAt := h.LastMessageAt
				if t, err2 := time.Parse(time.RFC3339, h.LastMessageAt); err2 == nil {
					lastAt = t.Format("2006-01-02")
				}
				fmt.Fprintf(cmd.OutOrStdout(), "#%s > %s%s (%d msgs, last %s)\n",
					h.StreamName, h.TopicName, res, h.MessageCount, lastAt)
				if h.BestSnippet != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", h.BestSnippet)
				}
				fmt.Fprintln(cmd.OutOrStdout())
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&stream, "stream", "", "filter by stream name")
	cmd.Flags().BoolVar(&unresolved, "unresolved", false, "only unresolved topics")
	cmd.Flags().IntVar(&limit, "limit", 20, "max topics to return")
	cmd.Flags().BoolVar(&semantic, "semantic", false, "use semantic/vector search (requires embeddings enabled + backfilled)")
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

func attachmentsCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var stream string
	var topic string
	var status string
	var limit int
	cmd := &cobra.Command{
		Use:   "attachments",
		Short: "List indexed Zulip attachments and local media cache status",
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
			if err := st.InitSchema(cmd.Context()); err != nil {
				return err
			}
			rows, err := st.ListAttachments(cmd.Context(), store.AttachmentFilter{Stream: stream, Topic: topic, Status: status, Limit: limit})
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No attachments found.")
				return nil
			}
			for _, a := range rows {
				stText := a.MediaStatus
				if stText == "" {
					stText = "pending"
				}
				name := a.FileName
				if name == "" {
					name = a.Title
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%d\tmsg:%d\t%s\t#%s > %s\t%s\t%s\n", a.ID, a.MessageID, stText, a.StreamName, a.TopicName, name, a.URL)
				if a.MediaPath != "" || a.MediaError != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "\tpath:%s\tbytes:%d\terror:%s\n", a.MediaPath, a.MediaBytes, a.MediaError)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&stream, "stream", "", "filter by stream name")
	cmd.Flags().StringVar(&topic, "topic", "", "filter by topic name")
	cmd.Flags().StringVar(&status, "status", "", "filter by media status: pending, fetched, error, all")
	cmd.Flags().IntVar(&limit, "limit", 100, "row limit")
	cmd.AddCommand(attachmentsFetchCmd(loadCfg))
	return cmd
}

func attachmentsFetchCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var limit int
	var force bool
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Download indexed Zulip /user_uploads/ attachments into the local cache",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if err := cfg.ValidateAuth(); err != nil {
				return err
			}
			lockPath := cfg.DBPath() + ".lock"
			l, err := lock.Acquire(lockPath)
			if err != nil {
				return err
			}
			defer l.Release()
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.InitSchema(cmd.Context()); err != nil {
				return err
			}
			api := zulip.NewClient(cfg.Zulip.URL, cfg.Zulip.Email, cfg.Zulip.APIKey)
			res, err := media.FetchPending(cmd.Context(), st, api, media.FetchOptions{CacheDir: cfg.MediaCacheDir(), Limit: limit, Force: force})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "media fetch: %d fetched, %d skipped, %d failed\n", res.Fetched, res.Skipped, res.Failed)
			fmt.Fprintf(cmd.OutOrStdout(), "cache: %s\n", cfg.MediaCacheDir())
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "maximum attachments to fetch (default all pending/error)")
	cmd.Flags().BoolVar(&force, "force", false, "refetch attachments even if already fetched")
	return cmd
}

func backfillIndexesCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var batchSize int
	cmd := &cobra.Command{
		Use:   "backfill-indexes",
		Short: "Rebuild mention and attachment indexes for existing messages",
		Long: `Reparse stored rendered HTML in messages.content to rebuild:

  • message_mentions  – @-mentions extracted from each message
  • message_attachments – /user_uploads/ links and any inline text preview
  • messages_fts.attachment_text – so attachment content participates in search

Useful after a schema migration that introduced these tables when the archive
already contained messages. Runs in batches (default 200 per transaction) and
prints progress to stderr. Idempotent: running twice leaves the same result.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if batchSize <= 0 {
				return fmt.Errorf("--batch must be positive")
			}
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			lockPath := cfg.DBPath() + ".lock"
			l, err := lock.Acquire(lockPath)
			if err != nil {
				return err
			}
			defer l.Release()

			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			ctx := cmd.Context()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}
			return syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{
				BatchSize: batchSize,
			}, os.Stderr)
		},
	}
	cmd.Flags().IntVar(&batchSize, "batch", 200, "number of messages per transaction")
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

// embeddingsCmd is the parent command for embedding management.
func embeddingsCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "embeddings",
		Short: "Manage topic embeddings (Ollama, disabled by default)",
		Long: `Manage local topic embeddings for semantic/vector search.

Embeddings are DISABLED by default. No calls are made to Ollama or any remote
service unless you explicitly enable them in config.

To enable:
  1. Edit ~/.zulcrawl/config.toml and add:
       [embeddings]
       enabled = true
       model   = "nomic-embed-text-v2-moe"
  2. Start Ollama:   ollama serve
  3. Pull the model: ollama pull nomic-embed-text-v2-moe
  4. Run backfill:   zulcrawl embeddings backfill
  5. Search:         zulcrawl topics search --semantic "your query"

First-run latency depends on the model and corpus size. Subsequent incremental
backfills only embed newly-added topics.`,
	}
	cmd.AddCommand(embeddingsBackfillCmd(loadCfg))
	cmd.AddCommand(embeddingsStatusCmd(loadCfg))
	return cmd
}

func embeddingsBackfillCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	var batchSize int
	var limit int
	var force bool
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Build or update topic embeddings",
		Long: `Embed all topics that do not yet have a stored vector for the configured model.

Requires:
  - [embeddings] enabled = true in config
  - Ollama running: ollama serve
  - Model pulled:   ollama pull <model>

Backfill is incremental: only topics without an existing embedding are processed.
Use --force to re-embed all topics (e.g. after changing the model).

Note: on first run with a large archive the process may take several minutes.
Progress is printed as batches complete.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if !cfg.Embeddings.Enabled {
				return fmt.Errorf(
					"embeddings are disabled.\n"+
						"Set [embeddings] enabled = true in config, then:\n"+
						"  ollama pull %s",
					cfg.Embeddings.Model)
			}
			if err := cfg.ValidateEmbeddings(); err != nil {
				return err
			}

			lockPath := cfg.DBPath() + ".lock"
			l, err := lock.Acquire(lockPath)
			if err != nil {
				return err
			}
			defer l.Release()

			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			ctx := cmd.Context()
			if err := st.InitSchema(ctx); err != nil {
				return err
			}

			if batchSize > 0 {
				cfg.Embeddings.BatchSize = batchSize
			}

			client := embed.NewOllamaClient(
				cfg.Embeddings.OllamaBase,
				cfg.Embeddings.Model,
				cfg.Embeddings.BatchSize,
			)

			// Verify Ollama is reachable and model is present before doing any work.
			checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := client.CheckAvailable(checkCtx); err != nil {
				return err
			}

			if force {
				// Drop all stored embeddings for this model so backfill re-embeds everything.
				fmt.Fprintf(os.Stderr, "--force: clearing existing embeddings for model %q\n", cfg.Embeddings.Model)
				if err := st.DeleteEmbeddingsByModel(ctx, cfg.Embeddings.Model); err != nil {
					return fmt.Errorf("clear embeddings: %w", err)
				}
			}

			_, err = embeddings.Backfill(ctx, cfg, st, client, embeddings.BackfillOptions{
				BatchSize: batchSize,
				Limit:     limit,
			}, os.Stdout)
			return err
		},
	}
	cmd.Flags().IntVar(&batchSize, "batch", 0, "override batch size from config")
	cmd.Flags().IntVar(&limit, "limit", 0, "only embed this many topics (0 = all)")
	cmd.Flags().BoolVar(&force, "force", false, "re-embed all topics, even if already embedded")
	return cmd
}

func embeddingsStatusCmd(loadCfg func() (*config.Config, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show embedding coverage for the configured model",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if !cfg.Embeddings.Enabled {
				fmt.Println("embeddings: disabled (set [embeddings] enabled = true to activate)")
				return nil
			}
			if err := cfg.ValidateEmbeddings(); err != nil {
				return err
			}
			st, err := store.Open(cfg.DBPath())
			if err != nil {
				return err
			}
			defer st.Close()
			ctx := cmd.Context()

			info, err := st.EmbeddingStats(ctx, cfg.Embeddings.Model)
			if err != nil {
				return err
			}
			stats, err := st.Stats(ctx)
			if err != nil {
				return err
			}

			fmt.Printf("embeddings status\n")
			fmt.Printf("  enabled:  %v\n", cfg.Embeddings.Enabled)
			fmt.Printf("  model:    %s\n", cfg.Embeddings.Model)
			fmt.Printf("  provider: %s\n", cfg.Embeddings.Provider)
			fmt.Printf("  base:     %s\n", cfg.Embeddings.OllamaBase)
			fmt.Printf("  topics total: %d\n", stats.Topics)
			if info != nil {
				fmt.Printf("  embedded:     %d\n", info.Count)
				fmt.Printf("  dimension:    %d\n", info.Dim)
				missing := stats.Topics - info.Count
				fmt.Printf("  missing:      %d\n", missing)
				if missing > 0 {
					fmt.Println("  hint: run `zulcrawl embeddings backfill` to embed missing topics")
				}
			} else {
				fmt.Println("  embedded:     0 (no embeddings yet)")
				fmt.Println("  hint: run `zulcrawl embeddings backfill` to get started")
			}
			return nil
		},
	}
}
