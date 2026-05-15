package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func setupTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if err := st.UpsertOrganization(ctx, 1, "https://example.com", "Example"); err != nil {
		t.Fatalf("UpsertOrganization: %v", err)
	}
	if err := st.UpsertStream(ctx, Stream{ID: 10, OrgID: 1, Name: "general"}); err != nil {
		t.Fatalf("UpsertStream: %v", err)
	}
	if err := st.UpsertUser(ctx, User{ID: 20, OrgID: 1, FullName: "Sender"}); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	return st, ctx
}

func TestUpsertMessageStoresMentionsAndAttachments(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	msg := Message{
		ID:          100,
		OrgID:       1,
		StreamID:    10,
		TopicID:     topicID,
		SenderID:    20,
		Content:     "hello",
		ContentText: "hello",
		Timestamp:   "2026-04-29T17:00:00Z",
		Mentions: []Mention{{
			UserID: 42,
			Name:   "Alice Example",
			Kind:   "user",
		}},
		Attachments: []Attachment{{
			URL:         "/user_uploads/1/ab/todo.txt",
			FileName:    "todo.txt",
			Title:       "todo.txt",
			ContentType: "text/plain",
			Text:        "needle attachment body",
			Indexed:     true,
		}},
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var mentionCount int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id=100 AND mentioned_user_id=42 AND mentioned_name='Alice Example'`).Scan(&mentionCount); err != nil {
		t.Fatalf("query mentions: %v", err)
	}
	if mentionCount != 1 {
		t.Fatalf("mention count = %d, want 1", mentionCount)
	}

	var attachmentCount int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM message_attachments WHERE message_id=100 AND file_name='todo.txt' AND indexed=1`).Scan(&attachmentCount); err != nil {
		t.Fatalf("query attachments: %v", err)
	}
	if attachmentCount != 1 {
		t.Fatalf("attachment count = %d, want 1", attachmentCount)
	}

	hits, err := st.Search(ctx, "needle", "", false, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].MessageID != 100 {
		t.Fatalf("attachment text search hits = %#v, want message 100", hits)
	}
}

func TestUpsertMessageReplacesMentionAndAttachmentRows(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	base := Message{ID: 101, OrgID: 1, StreamID: 10, TopicID: topicID, SenderID: 20, Content: "hello", ContentText: "hello", Timestamp: "2026-04-29T17:00:00Z"}
	base.Mentions = []Mention{{UserID: 1, Name: "Old", Kind: "user"}}
	base.Attachments = []Attachment{{URL: "/user_uploads/old.txt", FileName: "old.txt", Text: "oldneedle", Indexed: true}}
	if err := st.UpsertMessage(ctx, base); err != nil {
		t.Fatalf("first UpsertMessage: %v", err)
	}
	base.Mentions = []Mention{{UserID: 2, Name: "New", Kind: "user"}}
	base.Attachments = []Attachment{{URL: "/user_uploads/new.txt", FileName: "new.txt", Text: "newneedle", Indexed: true}}
	if err := st.UpsertMessage(ctx, base); err != nil {
		t.Fatalf("second UpsertMessage: %v", err)
	}

	var oldRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id=101 AND mentioned_name='Old'`).Scan(&oldRows); err != nil {
		t.Fatalf("query old mentions: %v", err)
	}
	if oldRows != 0 {
		t.Fatalf("old mention rows = %d, want 0", oldRows)
	}
	hits, err := st.Search(ctx, "newneedle", "", false, 10)
	if err != nil {
		t.Fatalf("Search newneedle: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("new attachment text hits = %d, want 1", len(hits))
	}
}

func TestEnsureMessageSenderPreservesExistingFields(t *testing.T) {
	st, ctx := setupTestStore(t)
	// First insert a full user record via UpsertUser (as the roster sync would).
	if err := st.UpsertUser(ctx, User{
		ID:        50,
		OrgID:     1,
		Email:     "alice@example.com",
		FullName:  "Alice Roster",
		IsBot:     false,
		AvatarURL: "https://example.com/avatar.png",
	}); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}

	// Now simulate the message-sender auto-upsert with sparse data (no email/avatar).
	if err := st.EnsureMessageSender(ctx, User{
		ID:       50,
		OrgID:    1,
		FullName: "Alice Updated Name",
		// Email, AvatarURL, IsBot are zero — should NOT overwrite existing values.
	}); err != nil {
		t.Fatalf("EnsureMessageSender: %v", err)
	}

	var email, avatar, fullName string
	var isBot int
	if err := st.db.QueryRowContext(ctx, `SELECT email, full_name, is_bot, avatar_url FROM users WHERE id=50`).Scan(&email, &fullName, &isBot, &avatar); err != nil {
		t.Fatalf("query user: %v", err)
	}
	if fullName != "Alice Updated Name" {
		t.Errorf("full_name = %q, want %q", fullName, "Alice Updated Name")
	}
	if email != "alice@example.com" {
		t.Errorf("email = %q, want preserved %q", email, "alice@example.com")
	}
	if avatar != "https://example.com/avatar.png" {
		t.Errorf("avatar_url = %q, want preserved", avatar)
	}
	if isBot != 0 {
		t.Errorf("is_bot = %d, want 0 (preserved)", isBot)
	}
}

func TestEnsureMessageSenderInsertsNewUser(t *testing.T) {
	st, ctx := setupTestStore(t)
	// A user not in the roster yet — only message metadata available.
	if err := st.EnsureMessageSender(ctx, User{
		ID:       99,
		OrgID:    1,
		FullName: "Ghost Bot",
	}); err != nil {
		t.Fatalf("EnsureMessageSender: %v", err)
	}

	var fullName string
	if err := st.db.QueryRowContext(ctx, `SELECT full_name FROM users WHERE id=99`).Scan(&fullName); err != nil {
		t.Fatalf("query user: %v", err)
	}
	if fullName != "Ghost Bot" {
		t.Errorf("full_name = %q, want %q", fullName, "Ghost Bot")
	}
}

func TestInitSchemaMigratesAttachmentMediaColumnsBeforeIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE message_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    org_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    file_name TEXT,
    title TEXT,
    content_type TEXT,
    text_content TEXT,
    indexed INTEGER DEFAULT 0,
    timestamp TEXT NOT NULL,
    UNIQUE(message_id, url)
);
`)
	if err != nil {
		_ = db.Close()
		t.Fatalf("create old attachment table: %v", err)
	}
	_ = db.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema should migrate old attachment schema before creating media index: %v", err)
	}
	var indexed string
	if err := st.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='index' AND name='idx_attachments_media_status'`).Scan(&indexed); err != nil {
		t.Fatalf("media status index missing after migration: %v", err)
	}
}

func TestAttachmentMediaMetadata(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	msg := Message{
		ID:            200,
		OrgID:         1,
		StreamID:      10,
		TopicID:       topicID,
		SenderID:      20,
		Content:       "file",
		ContentText:   "file",
		Timestamp:     "2026-01-02T03:04:05Z",
		HasAttachment: true,
		Attachments: []Attachment{{
			URL:      "/user_uploads/a/report.txt",
			FileName: "report.txt",
		}},
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	rows, err := st.ListAttachments(ctx, AttachmentFilter{Status: "pending"})
	if err != nil {
		t.Fatalf("ListAttachments: %v", err)
	}
	if len(rows) != 1 || rows[0].MediaStatus != "" {
		t.Fatalf("pending rows = %#v", rows)
	}
	if err := st.UpdateAttachmentMedia(ctx, rows[0].ID, AttachmentMediaUpdate{
		Path:        "/tmp/report.txt",
		Status:      "fetched",
		FetchedAt:   "2026-01-02T04:00:00Z",
		ContentType: "text/plain",
		Bytes:       12,
	}); err != nil {
		t.Fatalf("UpdateAttachmentMedia: %v", err)
	}
	rows, err = st.ListAttachments(ctx, AttachmentFilter{Status: "fetched"})
	if err != nil {
		t.Fatalf("ListAttachments fetched: %v", err)
	}
	if len(rows) != 1 || rows[0].MediaPath != "/tmp/report.txt" || rows[0].MediaBytes != 12 {
		t.Fatalf("fetched rows = %#v", rows)
	}
}

func TestListAttachmentsLimitSemantics(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	for i := int64(1); i <= 105; i++ {
		msg := Message{
			ID:            1000 + i,
			OrgID:         1,
			StreamID:      10,
			TopicID:       topicID,
			SenderID:      20,
			Content:       "file",
			ContentText:   "file",
			Timestamp:     "2026-01-02T03:04:05Z",
			HasAttachment: true,
			Attachments: []Attachment{{
				URL:      fmt.Sprintf("/user_uploads/a/file%03d.txt", i),
				FileName: "file.txt",
			}},
		}
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage %d: %v", i, err)
		}
	}

	rows, err := st.ListAttachments(ctx, AttachmentFilter{Status: "pending"})
	if err != nil {
		t.Fatalf("ListAttachments unlimited: %v", err)
	}
	if len(rows) != 105 {
		t.Fatalf("unlimited ListAttachments returned %d rows, want 105", len(rows))
	}

	rows, err = st.ListAttachments(ctx, AttachmentFilter{Status: "pending", Limit: 100})
	if err != nil {
		t.Fatalf("ListAttachments limited: %v", err)
	}
	if len(rows) != 100 {
		t.Fatalf("limited ListAttachments returned %d rows, want 100", len(rows))
	}
}
