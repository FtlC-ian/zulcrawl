package store

import (
	"context"
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
