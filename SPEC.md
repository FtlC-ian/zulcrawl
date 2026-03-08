# zulcrawl — Zulip Archive & Search CLI

Local SQLite mirror of Zulip organization data with full-text search, topic-aware indexing, and AI-ready query interface.

Inspired by [discrawl](https://github.com/steipete/discrawl) (SQLite mirror pattern) and [AnswerOverflow](https://github.com/AnswerOverflow/AnswerOverflow) (hybrid search, AI context, resolution detection).

## Why

Zulip's built-in search works but has limits: no offline access, no SQL queries, no way to feed conversation context to external AI agents, and no analytics over message history. zulcrawl puts everything in a local SQLite database you own.

## Goals

- Mirror all accessible Zulip streams, topics, and messages into SQLite
- FTS5 full-text search with topic-aware ranking
- Live event tailing via Zulip's event queue API
- MCP server for AI agent access to the archive
- Single binary, zero external dependencies beyond SQLite

## Non-Goals

- Web publishing / SEO pages (not needed)
- Replacing Zulip's UI
- Write operations back to Zulip

## Architecture

```
zulcrawl
├── cmd/zulcrawl/         # CLI entrypoint
├── internal/
│   ├── cli/              # Command definitions (cobra)
│   ├── config/           # Config loading (TOML)
│   ├── zulip/            # Zulip API client
│   ├── store/            # SQLite + FTS5 storage layer
│   ├── syncer/           # Backfill + incremental sync
│   ├── tailer/           # Live event queue consumer
│   ├── search/           # Search engine (FTS5 + topic ranking)
│   └── mcp/              # MCP server (optional)
└── go.mod
```

Language: Go. Same reasoning as discrawl — single binary, good concurrency, mature SQLite bindings (modernc.org/sqlite or mattn/go-sqlite3).

## Data Model

### SQLite Schema

```sql
-- Organization metadata
CREATE TABLE organizations (
    id              INTEGER PRIMARY KEY,
    url             TEXT NOT NULL,          -- https://your-org.zulipchat.com
    name            TEXT,
    synced_at       TEXT                    -- ISO 8601
);

-- Streams (channels)
CREATE TABLE streams (
    id              INTEGER PRIMARY KEY,    -- Zulip stream_id
    org_id          INTEGER NOT NULL REFERENCES organizations(id),
    name            TEXT NOT NULL,
    description     TEXT,
    is_web_public   INTEGER DEFAULT 0,
    invite_only     INTEGER DEFAULT 0,
    synced_at       TEXT
);

-- Topics (conversations within streams)
CREATE TABLE topics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    org_id          INTEGER NOT NULL REFERENCES organizations(id),
    stream_id       INTEGER NOT NULL REFERENCES streams(id),
    name            TEXT NOT NULL,
    resolved        INTEGER DEFAULT 0,      -- ✔ prefix detected
    message_count   INTEGER DEFAULT 0,
    first_message_id INTEGER,
    last_message_id  INTEGER,
    first_message_at TEXT,
    last_message_at  TEXT,
    synced_at       TEXT,
    UNIQUE(stream_id, name)
);

-- Users
CREATE TABLE users (
    id              INTEGER PRIMARY KEY,    -- Zulip user_id
    org_id          INTEGER NOT NULL REFERENCES organizations(id),
    email           TEXT,
    full_name       TEXT,
    is_bot          INTEGER DEFAULT 0,
    avatar_url      TEXT,
    synced_at       TEXT
);

-- Messages
CREATE TABLE messages (
    id              INTEGER PRIMARY KEY,    -- Zulip message_id
    org_id          INTEGER NOT NULL REFERENCES organizations(id),
    stream_id       INTEGER NOT NULL REFERENCES streams(id),
    topic_id        INTEGER NOT NULL REFERENCES topics(id),
    sender_id       INTEGER NOT NULL REFERENCES users(id),
    content         TEXT NOT NULL,           -- rendered HTML
    content_text    TEXT NOT NULL,           -- plain text (stripped)
    timestamp       TEXT NOT NULL,           -- ISO 8601
    edit_timestamp  TEXT,
    has_attachment   INTEGER DEFAULT 0,
    has_image        INTEGER DEFAULT 0,
    has_link         INTEGER DEFAULT 0,
    reactions        TEXT,                   -- JSON array
    is_me_message    INTEGER DEFAULT 0
);

-- FTS5 index
CREATE VIRTUAL TABLE messages_fts USING fts5(
    content_text,
    topic_name,
    sender_name,
    content=messages,
    content_rowid=id,
    tokenize='porter unicode61'
);

-- Topic-level FTS (search by topic name/description)
CREATE VIRTUAL TABLE topics_fts USING fts5(
    name,
    content=topics,
    content_rowid=id,
    tokenize='porter unicode61'
);

-- Sync state tracking
CREATE TABLE sync_state (
    org_id          INTEGER NOT NULL,
    stream_id       INTEGER NOT NULL,
    last_message_id INTEGER,                -- anchor for incremental sync
    last_event_id   INTEGER,                -- event queue bookmark
    synced_at       TEXT,
    PRIMARY KEY(org_id, stream_id)
);

-- Indexes
CREATE INDEX idx_messages_stream_topic ON messages(stream_id, topic_id);
CREATE INDEX idx_messages_sender ON messages(sender_id);
CREATE INDEX idx_messages_timestamp ON messages(timestamp);
CREATE INDEX idx_topics_stream ON topics(stream_id);
CREATE INDEX idx_topics_resolved ON topics(resolved);
```

### Key design decisions

**Topic as first-class entity.** Zulip's killer feature is topics — they're the natural unit of a "conversation" (unlike Discord where threads are bolted on). The schema makes topics queryable, countable, and searchable independently.

**Plain text extraction.** Zulip messages come as rendered HTML. We store both the HTML (for fidelity) and a stripped plain-text version (for FTS). This avoids indexing HTML tags.

**Resolved topic detection.** Zulip marks resolved topics with a ✔ prefix. We track this as a boolean for easy filtering ("show me all resolved questions about X").

**Reactions as JSON.** Lightweight, rarely queried individually. If analytics demand it later, a separate reactions table is easy to add.

## CLI Commands

```
zulcrawl init                              # Discover org, write config
zulcrawl init --from-openclaw ~/.openclaw/openclaw.json
zulcrawl doctor                            # Verify config, auth, DB health
zulcrawl sync --full                       # Full backfill of all streams
zulcrawl sync --streams general,engineering
zulcrawl sync --since 2026-01-01
zulcrawl tail                              # Live event queue consumer
zulcrawl tail --repair-every 30m           # With periodic consistency repair
zulcrawl search "database migration"       # FTS search
zulcrawl search --stream engineering "auth bug"
zulcrawl search --resolved "how to deploy" # Only resolved topics
zulcrawl topics --stream general           # List topics with stats
zulcrawl topics --unresolved               # Unresolved topics across all streams
zulcrawl stats                             # DB stats (messages, topics, users, size)
zulcrawl export --topic "auth refactor" --format markdown
zulcrawl serve --mcp                       # Start MCP server
zulcrawl sql "SELECT ..."                  # Raw SQL access
```

### init

- Reads Zulip API key from env (`ZULIP_EMAIL` + `ZULIP_API_KEY`), config file, or OpenClaw config
- Calls `/api/v1/server_settings` to discover the org
- Calls `/api/v1/streams` to list accessible streams
- Writes `~/.zulcrawl/config.toml`
- Creates SQLite DB at `~/.zulcrawl/zulcrawl.db`

### sync

- Calls `/api/v1/messages` with `anchor=oldest` (or last known ID for incremental)
- Paginates with `num_before=0, num_after=1000` (Zulip max is 5000, but 1000 is safer)
- Strips HTML to plain text using a lightweight HTML-to-text converter
- Upserts into messages table, updates FTS index
- Tracks per-stream sync state for efficient incremental syncs
- Parallel stream workers (configurable concurrency, default 4 — Zulip rate limits are tighter than Discord)

### tail

- Registers an event queue via `/api/v1/register` with `event_types=["message", "update_message", "delete_message", "stream", "subscription"]`
- Long-polls `/api/v1/events` in a loop
- Applies events to SQLite in real time
- Handles topic moves (Zulip allows moving messages between topics)
- Periodic repair sync catches anything the event queue missed

### search

Output format:

```
#engineering > database migration (2026-02-15)
  Ian F: We should run the migration with --dry-run first
  Ian F: Applied migration 093 to staging, all constraints verified

#general > deploy checklist (2026-02-10) [resolved]
  Debbie: The deploy script needs the --yes flag for non-interactive runs
```

Search ranking factors:
- FTS5 BM25 score (base relevance)
- Topic resolution boost (resolved topics rank slightly higher — the answer exists)
- Recency decay (newer messages get a mild boost)
- Topic coherence bonus (if multiple messages in the same topic match, boost that topic)

## Zulip API Integration

**Authentication:** Email + API key (bot or user). Zulip doesn't use OAuth tokens for API access — it's simpler than Discord.

**Rate limits:** Zulip rate limits are per-user, typically 200 requests per minute. The syncer should respect `X-RateLimit-Remaining` headers and back off automatically.

**Key endpoints:**
- `GET /api/v1/streams` — list streams
- `GET /api/v1/users` — list users
- `GET /api/v1/messages` — fetch messages (with narrow filters)
- `POST /api/v1/register` — register event queue
- `GET /api/v1/events` — long-poll for events
- `GET /api/v1/users/{user_id}/topics` — list topics for a stream (via `stream_id` param)

**Narrow filters** are Zulip's query language. For syncing a specific stream: `[{"operator": "stream", "operand": "engineering"}]`. These map cleanly to our per-stream sync approach.

## Ideas from AnswerOverflow

### Hybrid search (FTS + semantic)

AnswerOverflow uses both text search and vector embeddings for context retrieval. For zulcrawl, this could be a `--semantic` flag on search:

- Generate embeddings for each topic (not per-message — topics are the natural unit)
- Store in a separate `topic_embeddings` table (blob column for the vector)
- Use sqlite-vec or a simple cosine similarity function
- Useful for "find conversations similar to X" where keyword matching fails

This is an optional layer. FTS5 handles 90% of use cases. Semantic search is the 10% where someone asks "that discussion about making the API faster" and the actual topic was called "response time optimization."

### Resolution detection and Q&A extraction

AnswerOverflow detects resolved threads and extracts question/answer pairs. Zulip makes this easier because resolved topics already have a convention (✔ prefix).

Beyond just tracking the boolean, we could:
- Extract the "question" (first message or first message ending with ?) 
- Extract the "answer" (message that preceded the topic being marked resolved, or the last message from a different user)
- Store as structured Q&A pairs in a `topic_qa` table
- Makes the MCP server much more useful: "What was the answer to X?" → direct hit

### AI-powered topic summarization

On-demand (not during sync — that'd be expensive):

```
zulcrawl summarize --topic "auth refactor"
zulcrawl summarize --stream engineering --since 2026-03-01
```

Uses a local or remote LLM to produce a one-paragraph summary of a topic or a digest of recent activity. Results cached in a `summaries` table.

### Mention and cross-reference tracking

AnswerOverflow tracks structured mentions. For Zulip:
- Parse @-mentions and store in a `mentions` table
- Parse stream/topic links (Zulip's `#**stream>topic**` syntax) and store as cross-references
- Enables queries like "all topics where I was mentioned" or "topics that reference this topic"

## MCP Server

The MCP server exposes the archive to AI agents as tools:

**Tools:**
- `search(query, stream?, resolved?)` — FTS search over messages
- `get_topic(stream, topic)` — full message history for a topic
- `list_topics(stream, since?, unresolved?)` — topic listing with stats
- `get_summary(stream, topic)` — AI-generated summary (cached)
- `find_similar(text)` — semantic search (if embeddings enabled)

**Resources:**
- `zulip://streams` — list of all streams
- `zulip://stream/{name}/topics` — topics in a stream
- `zulip://stream/{name}/topic/{topic}` — messages in a topic

This lets any MCP-compatible agent (Claude, OpenClaw agents, etc.) query Zulip history as context.

## Config

`~/.zulcrawl/config.toml`:

```toml
[zulip]
url = "https://your-org.zulipchat.com"
email = "bot@your-org.zulipchat.com"
# api_key read from ZULIP_API_KEY env var or keychain

[database]
path = "~/.zulcrawl/zulcrawl.db"

[sync]
concurrency = 4
streams = []              # empty = all accessible streams
exclude_streams = ["social", "random"]

[tail]
repair_interval = "30m"

[search]
semantic_enabled = false
embedding_model = ""      # e.g., "ollama/nomic-embed-text" or "openai/text-embedding-3-small"

[mcp]
enabled = false
port = 8849
```

## Runtime Paths

```
~/.zulcrawl/
├── config.toml
├── zulcrawl.db           # SQLite database
├── cache/                # Embedding cache, summaries
└── logs/                 # Sync logs
```

## Build & Install

```bash
git clone https://github.com/<org>/zulcrawl.git
cd zulcrawl
go build -o bin/zulcrawl ./cmd/zulcrawl
```

## Open Questions

1. **Attachment handling** — Download and store locally, or just index metadata? Discrawl indexes small text attachments into FTS. We could do the same for code snippets, but skip images/binaries.

2. **Private streams** — Sync everything the bot/user can see, or require explicit opt-in per stream? Leaning toward "sync all, filter at query time" with an exclude list in config.

3. **Message edits** — Zulip supports message editing. Store edit history or just latest version? Latest-only is simpler and covers 99% of use cases.

4. **Multi-org** — Support multiple Zulip organizations in one DB? Discrawl does multi-guild. Probably worth building in from the start even if we only use one org.

5. **Embedding model** — Local (ollama) vs remote (OpenAI)? Config-driven, user's choice. Topic-level embeddings keep the volume manageable.
