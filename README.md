# zulcrawl

`zulcrawl` mirrors Zulip org data into local SQLite and provides fast FTS5 search.

## Features (current pass)

- TOML config (`~/.zulcrawl/config.toml`) + env overrides (`ZULIP_URL`, `ZULIP_EMAIL`, `ZULIP_API_KEY`)
- Zulip API client (auth, streams, users, messages + narrow filters + pagination + rate-limit retries)
- SQLite schema from `SPEC.md` (including FTS5 tables + sync state)
- Cobra CLI commands:
  - `init`
  - `doctor`
  - `sync --full --streams --since [--quiet]`
  - `search`
  - `topics` — list topics or topic-level hybrid search
  - `topics search` — hybrid search: FTS on topic names + message content (see below)
  - `stats`
  - `sql`
  - `messages` — direct archive slice queries (see below)
  - `backfill-indexes` — rebuild mention/attachment indexes for existing messages
- Syncer with parallel stream workers and incremental sync (`sync_state`)
- HTML-to-text conversion for indexing
- Search ranking with FTS BM25, resolved-topic boost, and recency boost
- Topic-level hybrid search: two-leg FTS (topic names + message content) with activity/recency scoring
- Mention and attachment indexes parsed from rendered HTML

## Attachment indexing — current scope

Attachment indexing covers **text already present in the Zulip-rendered HTML** of
each message — inline previews and short snippets that Zulip embeds in the
rendered `<div class="message_inline_ref">` block.  It does **not** fetch or
parse the actual uploaded file contents (PDFs, images, Office documents, etc.).
For text-heavy attachments the best coverage comes from users quoting or
summarising content in the message body itself.

Full file-content extraction (downloading from `/user_uploads/…` and parsing
the file bytes) is a planned future enhancement and is **not implemented**.

## Build

```bash
go build ./cmd/zulcrawl
```

## Config

`~/.zulcrawl/config.toml`

```toml
[zulip]
url = "https://your-org.zulipchat.com"
email = "bot@your-org.zulipchat.com"
api_key = ""

[database]
path = "~/.zulcrawl/zulcrawl.db"

[sync]
concurrency = 4
streams = []
exclude_streams = ["social", "random"]
```

Environment variables override file values:

- `ZULIP_URL`
- `ZULIP_EMAIL`
- `ZULIP_API_KEY`

## Usage

```bash
# Initialize config + DB
zulcrawl init --url https://your-org.zulipchat.com

# Health checks
zulcrawl doctor

# Full sync of all selected streams
zulcrawl sync --full

# Sync only specific streams
zulcrawl sync --streams general,engineering

# Sync only messages from date onward (YYYY-MM-DD)
zulcrawl sync --since 2026-01-01

# Sync silently (suppress progress lines, show only final summary or errors)
zulcrawl sync --quiet

# Search
zulcrawl search "database migration"
zulcrawl search --stream engineering --resolved "deploy script"

# Topic listing
zulcrawl topics --stream engineering
zulcrawl topics --unresolved

# Topic-level hybrid search (FTS on topic names + message content)
# Finds topics whose name or messages match the query
zulcrawl topics search "database migration"
zulcrawl topics search --stream engineering "deploy script"
zulcrawl topics search --unresolved --limit 10 "onboarding"

# Stats
zulcrawl stats

# Raw SQL
zulcrawl sql "SELECT stream_id, COUNT(*) FROM messages GROUP BY stream_id ORDER BY 2 DESC LIMIT 10"
```

## topics search command

`zulcrawl topics search <query>` performs a hybrid topic-level search over the
local archive. It runs two legs:

1. **Topic-name FTS** (`topics_fts`) — finds topics whose name matches the query.
2. **Message-content FTS** (`messages_fts`) aggregated by topic — finds topics
   that contain relevant messages.

Results from both legs are merged and de-duplicated by topic ID, then scored:

| Signal | Weight |
|--------|--------|
| BM25 relevance (best of name/message leg) | base |
| Activity bonus: log₂(1 + message_count) × 0.1 | up to ~0.7 for large topics |
| Recency decay: 0.5 / (1 + days_since_last_msg / 30) | 0–0.5 |
| Resolved bonus | +0.15 |

**No remote embedding APIs are called.** This is a local FTS/hybrid approach.
Vector-embedding provider integration is deferred to a future chunk (issue #9).

**Flags**

| Flag | Description |
|------|-------------|
| `--stream NAME` | Filter by stream name |
| `--unresolved` | Exclude resolved topics |
| `--limit N` | Maximum topics to return (default 20) |

**Output format**

```
#stream > topic name [resolved] (N msgs, last YYYY-MM-DD)
  …best matching message snippet…
```

## messages command

`zulcrawl messages` queries the local archive without hitting the Zulip API.
At least one narrowing filter is required to prevent accidental full-archive dumps.

**Flags**

| Flag | Description |
|------|-------------|
| `--stream NAME` | Filter by stream name (exact match) |
| `--topic NAME` | Filter by topic name (exact match) |
| `--sender NAME` | Filter by sender full name (substring match) |
| `--since DATE` | Only messages at or after this time (RFC3339 or `YYYY-MM-DD`) |
| `--until DATE` | Only messages at or before this time (RFC3339 or `YYYY-MM-DD`) |
| `--days N` | Only messages from the last N days |
| `--hours N` | Only messages from the last N hours |
| `--last N` | Return the N most recent messages (oldest-first output) |
| `--limit N` | Maximum messages to return (default 200) |
| `--all` | Remove safety limit and return all matching messages |

**Examples**

```bash
# Last 7 days in the #general stream
zulcrawl messages --stream general --days 7

# All messages in a specific topic
zulcrawl messages --stream engineering --topic "Q2 roadmap"

# Messages from a specific sender since a date
zulcrawl messages --sender "Alice" --since 2026-01-01

# Last 50 messages across the entire archive
zulcrawl messages --last 50

# Messages in the last 4 hours across all streams
zulcrawl messages --hours 4

# Combined: sender + date range, higher limit
zulcrawl messages --stream dev --sender "Bob" --since 2026-03-01 --until 2026-03-31 --limit 500

# Remove the safety cap entirely (use with care on large archives)
zulcrawl messages --stream general --days 365 --all
```

Output format per message:
```
[YYYY-MM-DD HH:MM:SS] #stream > topic | Sender Name
  message content text...

```

## Notes

- Uses `modernc.org/sqlite` (pure Go, no CGO)
- Current implementation focuses on core mirror/search flow
- Planned later (per spec): tail/event queue, MCP server, vector-embedding semantic search (pluggable provider, off by default), summarization, Q&A extraction
