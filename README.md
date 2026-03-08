# zulcrawl

`zulcrawl` mirrors Zulip org data into local SQLite and provides fast FTS5 search.

## Features (current pass)

- TOML config (`~/.zulcrawl/config.toml`) + env overrides (`ZULIP_URL`, `ZULIP_EMAIL`, `ZULIP_API_KEY`)
- Zulip API client (auth, streams, users, messages + narrow filters + pagination + rate-limit retries)
- SQLite schema from `SPEC.md` (including FTS5 tables + sync state)
- Cobra CLI commands:
  - `init`
  - `doctor`
  - `sync --full --streams --since`
  - `search`
  - `topics`
  - `stats`
  - `sql`
- Syncer with parallel stream workers and incremental sync (`sync_state`)
- HTML-to-text conversion for indexing
- Search ranking with FTS BM25, resolved-topic boost, and recency boost

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

# Search
zulcrawl search "database migration"
zulcrawl search --stream engineering --resolved "deploy script"

# Topic listing
zulcrawl topics --stream engineering
zulcrawl topics --unresolved

# Stats
zulcrawl stats

# Raw SQL
zulcrawl sql "SELECT stream_id, COUNT(*) FROM messages GROUP BY stream_id ORDER BY 2 DESC LIMIT 10"
```

## Notes

- Uses `modernc.org/sqlite` (pure Go, no CGO)
- Current implementation focuses on core mirror/search flow
- Planned later (per spec): tail/event queue, MCP server, semantic search, summarization, Q&A extraction
