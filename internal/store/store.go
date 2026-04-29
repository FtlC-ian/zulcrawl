package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) InitSchema(ctx context.Context) error {
	ftsMigrated, err := s.prepareFTSMigration(ctx)
	if err != nil {
		return err
	}
	schema := `
CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    name TEXT,
    synced_at TEXT
);
CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    name TEXT NOT NULL,
    description TEXT,
    is_web_public INTEGER DEFAULT 0,
    invite_only INTEGER DEFAULT 0,
    synced_at TEXT
);
CREATE TABLE IF NOT EXISTS topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    stream_id INTEGER NOT NULL REFERENCES streams(id),
    name TEXT NOT NULL,
    resolved INTEGER DEFAULT 0,
    message_count INTEGER DEFAULT 0,
    first_message_id INTEGER,
    last_message_id INTEGER,
    first_message_at TEXT,
    last_message_at TEXT,
    synced_at TEXT,
    UNIQUE(stream_id, name)
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    email TEXT,
    full_name TEXT,
    is_bot INTEGER DEFAULT 0,
    avatar_url TEXT,
    synced_at TEXT
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    stream_id INTEGER NOT NULL REFERENCES streams(id),
    topic_id INTEGER NOT NULL REFERENCES topics(id),
    sender_id INTEGER NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    content_text TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    edit_timestamp TEXT,
    has_attachment INTEGER DEFAULT 0,
    has_image INTEGER DEFAULT 0,
    has_link INTEGER DEFAULT 0,
    reactions TEXT,
    is_me_message INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS message_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    stream_id INTEGER NOT NULL REFERENCES streams(id),
    topic_id INTEGER NOT NULL REFERENCES topics(id),
    mentioned_user_id INTEGER,
    mentioned_name TEXT NOT NULL,
    mention_kind TEXT NOT NULL DEFAULT 'user',
    timestamp TEXT NOT NULL,
    UNIQUE(message_id, mentioned_user_id, mentioned_name, mention_kind)
);
CREATE TABLE IF NOT EXISTS message_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    org_id INTEGER NOT NULL REFERENCES organizations(id),
    stream_id INTEGER NOT NULL REFERENCES streams(id),
    topic_id INTEGER NOT NULL REFERENCES topics(id),
    url TEXT NOT NULL,
    file_name TEXT,
    title TEXT,
    content_type TEXT,
    text_content TEXT,
    indexed INTEGER DEFAULT 0,
    timestamp TEXT NOT NULL,
    UNIQUE(message_id, url)
);

-- Standalone FTS5 tables (no content= option).
-- content= was intentionally removed because messages_fts declares extra columns
-- (topic_name, sender_name) that do not exist in the messages table. SQLite FTS5
-- queries ALL declared columns from the content table when evaluating auxiliary
-- functions like snippet(), which would fail at runtime.  Instead we maintain
-- both FTS tables via explicit triggers that set rowid = message/topic id, so
-- JOIN ON messages_fts.rowid = messages.id continues to work.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    content_text,
    topic_name,
    sender_name,
    attachment_text,
    tokenize='porter unicode61'
);
CREATE VIRTUAL TABLE IF NOT EXISTS topics_fts USING fts5(
    name,
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS sync_state (
    org_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    last_message_id INTEGER,
    last_event_id INTEGER,
    synced_at TEXT,
    PRIMARY KEY(org_id, stream_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_stream_topic ON messages(stream_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_mentions_message ON message_mentions(message_id);
CREATE INDEX IF NOT EXISTS idx_mentions_user ON message_mentions(mentioned_user_id);
CREATE INDEX IF NOT EXISTS idx_mentions_name ON message_mentions(mentioned_name);
CREATE INDEX IF NOT EXISTS idx_mentions_stream_topic ON message_mentions(stream_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_mentions_timestamp ON message_mentions(timestamp);
CREATE INDEX IF NOT EXISTS idx_attachments_message ON message_attachments(message_id);
CREATE INDEX IF NOT EXISTS idx_attachments_stream_topic ON message_attachments(stream_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_attachments_timestamp ON message_attachments(timestamp);
CREATE INDEX IF NOT EXISTS idx_topics_stream ON topics(stream_id);
CREATE INDEX IF NOT EXISTS idx_topics_resolved ON topics(resolved);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, content_text, topic_name, sender_name, attachment_text)
  VALUES (new.id, new.content_text,
    COALESCE((SELECT name FROM topics WHERE id = new.topic_id),''),
    COALESCE((SELECT full_name FROM users WHERE id = new.sender_id),''),
    COALESCE((SELECT group_concat(text_content, ' ') FROM message_attachments WHERE message_id = new.id AND indexed = 1),''));
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  DELETE FROM messages_fts WHERE rowid = old.id;
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
  DELETE FROM messages_fts WHERE rowid = old.id;
  INSERT INTO messages_fts(rowid, content_text, topic_name, sender_name, attachment_text)
  VALUES (new.id, new.content_text,
    COALESCE((SELECT name FROM topics WHERE id = new.topic_id),''),
    COALESCE((SELECT full_name FROM users WHERE id = new.sender_id),''),
    COALESCE((SELECT group_concat(text_content, ' ') FROM message_attachments WHERE message_id = new.id AND indexed = 1),''));
END;

CREATE TRIGGER IF NOT EXISTS topics_ai AFTER INSERT ON topics BEGIN
  INSERT INTO topics_fts(rowid, name) VALUES(new.id, new.name);
END;
CREATE TRIGGER IF NOT EXISTS topics_ad AFTER DELETE ON topics BEGIN
  DELETE FROM topics_fts WHERE rowid = old.id;
END;
CREATE TRIGGER IF NOT EXISTS topics_au AFTER UPDATE ON topics BEGIN
  DELETE FROM topics_fts WHERE rowid = old.id;
  INSERT INTO topics_fts(rowid, name) VALUES(new.id, new.name);
END;
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	if ftsMigrated {
		return s.ReindexFTS(ctx)
	}
	return nil
}

func (s *Store) prepareFTSMigration(ctx context.Context) (bool, error) {
	if _, err := s.db.ExecContext(ctx, `
DROP TRIGGER IF EXISTS messages_ai;
DROP TRIGGER IF EXISTS messages_ad;
DROP TRIGGER IF EXISTS messages_au;
`); err != nil {
		return false, err
	}
	var name string
	if err := s.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='messages_fts'`).Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(messages_fts)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	hasAttachmentText := false
	for rows.Next() {
		var cid int
		var colName, typ string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &colName, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if colName == "attachment_text" {
			hasAttachmentText = true
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if hasAttachmentText {
		return false, nil
	}
	if _, err := s.db.ExecContext(ctx, `DROP TABLE messages_fts`); err != nil {
		return false, err
	}
	return true, nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

func (s *Store) UpsertOrganization(ctx context.Context, id int64, url, name string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO organizations(id, url, name, synced_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET url=excluded.url, name=excluded.name, synced_at=excluded.synced_at
`, id, url, name, now())
	return err
}

type Stream struct {
	ID          int64
	OrgID       int64
	Name        string
	Description string
	InviteOnly  bool
	IsWebPublic bool
}

func (s *Store) UpsertStream(ctx context.Context, st Stream) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO streams(id, org_id, name, description, is_web_public, invite_only, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
name=excluded.name, description=excluded.description,
is_web_public=excluded.is_web_public, invite_only=excluded.invite_only, synced_at=excluded.synced_at
`, st.ID, st.OrgID, st.Name, st.Description, boolToInt(st.IsWebPublic), boolToInt(st.InviteOnly), now())
	return err
}

type User struct {
	ID        int64
	OrgID     int64
	Email     string
	FullName  string
	IsBot     bool
	AvatarURL string
}

func (s *Store) UpsertUser(ctx context.Context, u User) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO users(id, org_id, email, full_name, is_bot, avatar_url, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
email=excluded.email, full_name=excluded.full_name, is_bot=excluded.is_bot, avatar_url=excluded.avatar_url, synced_at=excluded.synced_at
`, u.ID, u.OrgID, u.Email, u.FullName, boolToInt(u.IsBot), u.AvatarURL, now())
	return err
}

// EnsureMessageSender upserts a user record derived from message metadata
// (sender_id + sender_full_name). Unlike UpsertUser it preserves existing
// non-empty fields (email, avatar_url, is_bot) so that a full-roster sync
// does not get overwritten by the sparse data carried in message headers.
// Only full_name is always refreshed because it is the only field reliably
// present in every message.
func (s *Store) EnsureMessageSender(ctx context.Context, u User) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO users(id, org_id, email, full_name, is_bot, avatar_url, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  full_name = excluded.full_name,
  email     = CASE WHEN excluded.email     != '' THEN excluded.email     ELSE email     END,
  avatar_url= CASE WHEN excluded.avatar_url!= '' THEN excluded.avatar_url ELSE avatar_url END,
  is_bot    = CASE WHEN excluded.is_bot    != 0  THEN excluded.is_bot    ELSE is_bot    END,
  synced_at = excluded.synced_at
`, u.ID, u.OrgID, u.Email, u.FullName, boolToInt(u.IsBot), u.AvatarURL, now())
	return err
}

func topicResolved(name string) bool {
	t := strings.TrimSpace(name)
	return strings.HasPrefix(t, "✔") || strings.HasPrefix(strings.ToLower(t), "[resolved]")
}

func (s *Store) GetOrCreateTopic(ctx context.Context, orgID, streamID int64, name string) (int64, error) {
	if name == "" {
		name = "(no topic)"
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO topics(org_id, stream_id, name, resolved, synced_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(stream_id, name) DO UPDATE SET
resolved=excluded.resolved,
synced_at=excluded.synced_at
`, orgID, streamID, name, boolToInt(topicResolved(name)), now())
	if err != nil {
		return 0, err
	}
	var id int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM topics WHERE stream_id=? AND name=?`, streamID, name).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

type Mention struct {
	MessageID int64
	OrgID     int64
	StreamID  int64
	TopicID   int64
	UserID    int64
	Name      string
	Kind      string
	Timestamp string
}

type Attachment struct {
	MessageID   int64
	OrgID       int64
	StreamID    int64
	TopicID     int64
	URL         string
	FileName    string
	Title       string
	ContentType string
	Text        string
	Indexed     bool
	Timestamp   string
}

type Message struct {
	ID            int64
	OrgID         int64
	StreamID      int64
	TopicID       int64
	SenderID      int64
	Content       string
	ContentText   string
	Timestamp     string
	EditTimestamp string
	HasAttachment bool
	HasImage      bool
	HasLink       bool
	Reactions     json.RawMessage
	IsMeMessage   bool
	Mentions      []Mention
	Attachments   []Attachment
}

func (s *Store) UpsertMessage(ctx context.Context, m Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := s.upsertMessageTx(ctx, tx, m); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// upsertMessageTx is the shared implementation used by both UpsertMessage and
// UpsertMessageBatch. It accepts anything that implements ExecContext so it can
// work with either *sql.DB or *sql.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *Store) upsertMessageTx(ctx context.Context, ex execer, m Message) error {
	reactions := string(m.Reactions)
	if reactions == "" {
		reactions = "[]"
	}
	if _, err := ex.ExecContext(ctx, `DELETE FROM message_mentions WHERE message_id = ?`, m.ID); err != nil {
		return err
	}
	if _, err := ex.ExecContext(ctx, `DELETE FROM message_attachments WHERE message_id = ?`, m.ID); err != nil {
		return err
	}
	_, err := ex.ExecContext(ctx, `
INSERT INTO messages(
id, org_id, stream_id, topic_id, sender_id, content, content_text, timestamp, edit_timestamp,
has_attachment, has_image, has_link, reactions, is_me_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
stream_id=excluded.stream_id,
topic_id=excluded.topic_id,
sender_id=excluded.sender_id,
content=excluded.content,
content_text=excluded.content_text,
timestamp=excluded.timestamp,
edit_timestamp=excluded.edit_timestamp,
has_attachment=excluded.has_attachment,
has_image=excluded.has_image,
has_link=excluded.has_link,
reactions=excluded.reactions,
is_me_message=excluded.is_me_message
`,
		m.ID, m.OrgID, m.StreamID, m.TopicID, m.SenderID,
		m.Content, m.ContentText, m.Timestamp, nullIfEmpty(m.EditTimestamp),
		boolToInt(m.HasAttachment), boolToInt(m.HasImage), boolToInt(m.HasLink), reactions, boolToInt(m.IsMeMessage),
	)
	if err != nil {
		return err
	}
	for _, mention := range m.Mentions {
		if mention.Kind == "" {
			mention.Kind = "user"
		}
		if mention.Timestamp == "" {
			mention.Timestamp = m.Timestamp
		}
		if _, err := ex.ExecContext(ctx, `
INSERT OR IGNORE INTO message_mentions(
message_id, org_id, stream_id, topic_id, mentioned_user_id, mentioned_name, mention_kind, timestamp
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			m.ID, m.OrgID, m.StreamID, m.TopicID, nullInt64IfZero(mention.UserID), mention.Name, mention.Kind, mention.Timestamp,
		); err != nil {
			return err
		}
	}
	for _, attachment := range m.Attachments {
		if attachment.Timestamp == "" {
			attachment.Timestamp = m.Timestamp
		}
		if _, err := ex.ExecContext(ctx, `
INSERT OR IGNORE INTO message_attachments(
message_id, org_id, stream_id, topic_id, url, file_name, title, content_type, text_content, indexed, timestamp
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			m.ID, m.OrgID, m.StreamID, m.TopicID, attachment.URL, attachment.FileName, attachment.Title,
			attachment.ContentType, nullIfEmpty(attachment.Text), boolToInt(attachment.Indexed), attachment.Timestamp,
		); err != nil {
			return err
		}
	}
	if len(m.Attachments) > 0 {
		if _, err := ex.ExecContext(ctx, `DELETE FROM messages_fts WHERE rowid = ?`, m.ID); err != nil {
			return err
		}
		_, err = ex.ExecContext(ctx, `
INSERT INTO messages_fts(rowid, content_text, topic_name, sender_name, attachment_text)
VALUES (?, ?,
  COALESCE((SELECT name FROM topics WHERE id = ?),''),
  COALESCE((SELECT full_name FROM users WHERE id = ?),''),
  COALESCE((SELECT group_concat(text_content, ' ') FROM message_attachments WHERE message_id = ? AND indexed = 1),''));`,
			m.ID, m.ContentText, m.TopicID, m.SenderID, m.ID)
	}
	return err
}

// UpsertMessageBatch inserts/updates a slice of messages inside a single
// transaction for efficiency. Each page of the syncer should call this instead
// of calling UpsertMessage in a loop.
func (s *Store) UpsertMessageBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for i, m := range msgs {
		if err := s.upsertMessageTx(ctx, tx, m); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("message %d (id=%d, sender=%d, topic=%d, stream=%d): %w", i, m.ID, m.SenderID, m.TopicID, m.StreamID, err)
		}
	}
	return tx.Commit()
}

func (s *Store) RecomputeTopicStats(ctx context.Context, topicID int64) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE topics
SET
message_count = COALESCE((SELECT COUNT(*) FROM messages m WHERE m.topic_id = topics.id), 0),
first_message_id = (SELECT id FROM messages m WHERE m.topic_id = topics.id ORDER BY m.id ASC LIMIT 1),
last_message_id = (SELECT id FROM messages m WHERE m.topic_id = topics.id ORDER BY m.id DESC LIMIT 1),
first_message_at = (SELECT timestamp FROM messages m WHERE m.topic_id = topics.id ORDER BY m.timestamp ASC LIMIT 1),
last_message_at = (SELECT timestamp FROM messages m WHERE m.topic_id = topics.id ORDER BY m.timestamp DESC LIMIT 1),
synced_at = ?
WHERE id = ?
`, now(), topicID)
	return err
}

func (s *Store) LastMessageID(ctx context.Context, orgID, streamID int64) (int64, error) {
	var id sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT last_message_id FROM sync_state WHERE org_id=? AND stream_id=?`, orgID, streamID).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (s *Store) UpdateSyncState(ctx context.Context, orgID, streamID, lastMessageID int64) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO sync_state(org_id, stream_id, last_message_id, synced_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(org_id, stream_id) DO UPDATE SET
last_message_id=excluded.last_message_id,
synced_at=excluded.synced_at
`, orgID, streamID, lastMessageID, now())
	return err
}

type TopicRow struct {
	StreamName     string
	TopicName      string
	Resolved       bool
	MessageCount   int64
	LastMessageAt  string
	FirstMessageAt string
}

func (s *Store) ListTopics(ctx context.Context, stream string, unresolved bool, limit int) ([]TopicRow, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `
SELECT s.name, t.name, t.resolved, t.message_count, COALESCE(t.last_message_at,''), COALESCE(t.first_message_at,'')
FROM topics t
JOIN streams s ON s.id=t.stream_id
WHERE 1=1`
	args := []any{}
	if stream != "" {
		q += " AND s.name = ?"
		args = append(args, stream)
	}
	if unresolved {
		q += " AND t.resolved = 0"
	}
	q += " ORDER BY t.last_message_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TopicRow{}
	for rows.Next() {
		var r TopicRow
		var resolved int
		if err := rows.Scan(&r.StreamName, &r.TopicName, &resolved, &r.MessageCount, &r.LastMessageAt, &r.FirstMessageAt); err != nil {
			return nil, err
		}
		r.Resolved = resolved == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

type SearchHit struct {
	MessageID  int64
	StreamName string
	TopicName  string
	Resolved   bool
	Timestamp  string
	SenderName string
	Snippet    string
	Rank       float64
}

func (s *Store) Search(ctx context.Context, query, stream string, resolvedOnly bool, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		limit = 20
	}
	// messages_fts is a standalone FTS5 table (no content= backing).
	// snippet() reads from the FTS table's own stored content, so no extra
	// JOIN to messages is needed for the snippet itself.
	// The outer JOIN to messages is for metadata (timestamp, stream, sender).
	base := `
SELECT m.id, st.name, t.name, t.resolved, m.timestamp, u.full_name,
       snippet(messages_fts, 0, '', '', ' … ', 20),
       ((-bm25(messages_fts))
        + CASE WHEN t.resolved=1 THEN 0.20 ELSE 0 END
        + (1.0/(1.0 + ((julianday('now') - julianday(m.timestamp))/30.0)))) AS rank
FROM messages_fts
JOIN messages m ON m.id = messages_fts.rowid
JOIN topics t ON t.id = m.topic_id
JOIN streams st ON st.id = m.stream_id
JOIN users u ON u.id = m.sender_id
WHERE messages_fts MATCH ?`
	args := []any{query}
	if stream != "" {
		base += " AND st.name = ?"
		args = append(args, stream)
	}
	if resolvedOnly {
		base += " AND t.resolved = 1"
	}
	base += " ORDER BY rank DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SearchHit{}
	for rows.Next() {
		var h SearchHit
		var resolved int
		if err := rows.Scan(&h.MessageID, &h.StreamName, &h.TopicName, &resolved, &h.Timestamp, &h.SenderName, &h.Snippet, &h.Rank); err != nil {
			return nil, err
		}
		h.Resolved = resolved == 1
		out = append(out, h)
	}
	return out, rows.Err()
}

type Stats struct {
	Streams  int64
	Topics   int64
	Users    int64
	Messages int64
	DBSize   int64
}

func (s *Store) Stats(ctx context.Context) (*Stats, error) {
	st := &Stats{}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM streams`).Scan(&st.Streams); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM topics`).Scan(&st.Topics); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&st.Users); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&st.Messages); err != nil {
		return nil, err
	}
	_ = s.db.QueryRowContext(ctx, `SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()`).Scan(&st.DBSize)
	return st, nil
}

func (s *Store) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query)
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullInt64IfZero(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// ReindexFTS repopulates both FTS tables from scratch.
// Unlike content= tables, standalone FTS5 tables do not support the 'rebuild'
// command, so we delete all rows and reinsert from the source tables.
func (s *Store) ReindexFTS(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmts := []string{
		// Clear FTS tables.
		`DELETE FROM messages_fts`,
		`DELETE FROM topics_fts`,
		// Repopulate messages_fts.
		`INSERT INTO messages_fts(rowid, content_text, topic_name, sender_name, attachment_text)
         SELECT m.id, m.content_text,
                COALESCE(t.name, ''),
                COALESCE(u.full_name, ''),
                COALESCE((SELECT group_concat(a.text_content, ' ')
                          FROM message_attachments a
                          WHERE a.message_id = m.id AND a.indexed = 1), '')
         FROM messages m
         LEFT JOIN topics t ON t.id = m.topic_id
         LEFT JOIN users u ON u.id = m.sender_id`,
		// Repopulate topics_fts.
		`INSERT INTO topics_fts(rowid, name) SELECT id, name FROM topics`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("reindex fts: %w", err)
		}
	}
	return tx.Commit()
}

// MessageRow is a single message returned by QueryMessages.
type MessageRow struct {
	ID         int64
	StreamName string
	TopicName  string
	SenderName string
	Timestamp  string
	Content    string
}

// MessagesFilter specifies which messages to return. At least one narrowing
// criterion must be set by the caller; the store itself does not enforce the
// requirement — that is done in the CLI layer.
type MessagesFilter struct {
	Stream string // stream name (exact match)
	Topic  string // topic name  (exact match)
	Sender string // sender full_name LIKE search

	// Time window — RFC3339 timestamps, both optional.
	Since string
	Until string

	// Convenience windows — applied as "now - duration".
	// Days and Hours are cumulative (whichever is non-zero is added).
	Days  int
	Hours int

	// Last N (sorted DESC inside, reversed before return).
	Last int

	// Hard cap; 0 means use default.
	Limit int
}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// QueryMessages returns messages matching f, always ordered oldest-first.
func (s *Store) QueryMessages(ctx context.Context, f MessagesFilter) ([]MessageRow, error) {
	// Determine the effective limit.
	// -1 means "no cap" (--all flag), 0 means "use default 200", >0 is explicit.
	noLimit := f.Limit == -1
	limit := f.Limit
	if limit <= 0 && !noLimit {
		limit = 200
	}

	q := `
SELECT m.id, st.name, t.name, COALESCE(u.full_name,''), m.timestamp, m.content_text
FROM messages m
JOIN streams st ON st.id = m.stream_id
JOIN topics t ON t.id = m.topic_id
JOIN users u ON u.id = m.sender_id
WHERE 1=1`
	var args []any

	if f.Stream != "" {
		q += " AND st.name = ?"
		args = append(args, f.Stream)
	}
	if f.Topic != "" {
		q += " AND t.name = ?"
		args = append(args, f.Topic)
	}
	if f.Sender != "" {
		q += ` AND u.full_name LIKE ? ESCAPE '\'`
		args = append(args, "%"+escapeLike(f.Sender)+"%")
	}
	if f.Since != "" {
		q += " AND m.timestamp >= ?"
		args = append(args, f.Since)
	}
	if f.Until != "" {
		q += " AND m.timestamp <= ?"
		args = append(args, f.Until)
	}
	if f.Days > 0 || f.Hours > 0 {
		window := time.Now().UTC().
			Add(-time.Duration(f.Days) * 24 * time.Hour).
			Add(-time.Duration(f.Hours) * time.Hour).
			Format(time.RFC3339)
		q += " AND m.timestamp >= ?"
		args = append(args, window)
	}

	if f.Last > 0 {
		// Pull the newest N then reverse them to oldest-first.
		q += " ORDER BY m.timestamp DESC LIMIT ?"
		args = append(args, f.Last)
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []MessageRow
		for rows.Next() {
			var r MessageRow
			if err := rows.Scan(&r.ID, &r.StreamName, &r.TopicName, &r.SenderName, &r.Timestamp, &r.Content); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		// Reverse to oldest-first.
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out, nil
	}

	if noLimit {
		q += " ORDER BY m.timestamp ASC"
	} else {
		q += " ORDER BY m.timestamp ASC LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MessageRow
	for rows.Next() {
		var r MessageRow
		if err := rows.Scan(&r.ID, &r.StreamName, &r.TopicName, &r.SenderName, &r.Timestamp, &r.Content); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) EnsureSystemUser(ctx context.Context, orgID int64, id int64, name string) error {
	if id == 0 {
		return nil
	}
	return s.UpsertUser(ctx, User{ID: id, OrgID: orgID, FullName: name})
}

func (s *Store) EnsureStreamByName(ctx context.Context, orgID int64, streamID int64, name string) error {
	if streamID == 0 {
		return fmt.Errorf("stream id missing")
	}
	return s.UpsertStream(ctx, Stream{ID: streamID, OrgID: orgID, Name: name})
}
