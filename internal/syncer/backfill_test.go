package syncer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/syncer"
)

// openTestStore opens a fresh SQLite DB in a temp dir with schema applied.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if err := st.UpsertOrganization(ctx, 1, "https://z.example.com", "TestOrg"); err != nil {
		t.Fatalf("UpsertOrganization: %v", err)
	}
	if err := st.UpsertStream(ctx, store.Stream{ID: 10, OrgID: 1, Name: "general"}); err != nil {
		t.Fatalf("UpsertStream: %v", err)
	}
	if err := st.UpsertUser(ctx, store.User{ID: 20, OrgID: 1, FullName: "Alice"}); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	return st
}

// queryCount runs a COUNT(*) SQL query via the store's raw Query method.
func queryCount(t *testing.T, st *store.Store, ctx context.Context, query string) int {
	t.Helper()
	rows, err := st.Query(ctx, query)
	if err != nil {
		t.Fatalf("queryCount(%q): %v", query, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("queryCount(%q): no rows returned", query)
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("queryCount(%q) scan: %v", query, err)
	}
	return n
}

// seedOldMessage inserts a message without any derived indexes (simulates
// a pre-Phase-2 archive row).
func seedOldMessage(t *testing.T, st *store.Store, id int64, html string) {
	t.Helper()
	ctx := context.Background()
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	if err := st.UpsertMessage(ctx, store.Message{
		ID:          id,
		OrgID:       1,
		StreamID:    10,
		TopicID:     topicID,
		SenderID:    20,
		Content:     html,
		ContentText: "content text",
		Timestamp:   "2026-01-01T00:00:00Z",
		// Intentionally NO Mentions or Attachments (old-style row).
	}); err != nil {
		t.Fatalf("UpsertMessage %d: %v", id, err)
	}
}

// TestBackfillIndexes_OldStyleDB confirms that BackfillIndexes populates
// message_mentions and message_attachments for messages that were inserted
// before the indexing logic existed.
func TestBackfillIndexes_OldStyleDB(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// An old message that contains a @-mention and an attachment link in HTML.
	const msgHTML = `<p>Hey <span class="user-mention" data-user-id="42">@Bob</span>, see <a class="message_inline_ref" href="/user_uploads/1/ab/notes.txt">notes.txt</a></p>`
	seedOldMessage(t, st, 1001, msgHTML)

	// Verify no derived rows exist before backfill.
	mentionsBefore := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id=1001`)
	attachmentsBefore := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_attachments WHERE message_id=1001`)
	if mentionsBefore != 0 {
		t.Fatalf("expected 0 mentions before backfill, got %d", mentionsBefore)
	}
	if attachmentsBefore != 0 {
		t.Fatalf("expected 0 attachments before backfill, got %d", attachmentsBefore)
	}

	// Run backfill.
	if err := syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{BatchSize: 50}, os.Stderr); err != nil {
		t.Fatalf("BackfillIndexes: %v", err)
	}

	// Verify derived rows now exist.
	mentionsAfter := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id=1001 AND mentioned_user_id=42`)
	attachmentsAfter := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_attachments WHERE message_id=1001 AND file_name='notes.txt'`)
	if mentionsAfter != 1 {
		t.Fatalf("expected 1 mention after backfill, got %d", mentionsAfter)
	}
	if attachmentsAfter != 1 {
		t.Fatalf("expected 1 attachment after backfill, got %d", attachmentsAfter)
	}
}

// TestBackfillIndexes_Idempotent confirms that running BackfillIndexes twice
// does not duplicate rows.
func TestBackfillIndexes_Idempotent(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const msgHTML = `<p><span class="user-mention" data-user-id="99">@Carol</span></p>`
	seedOldMessage(t, st, 2001, msgHTML)

	for i := 0; i < 2; i++ {
		if err := syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{BatchSize: 50}, os.Stderr); err != nil {
			t.Fatalf("BackfillIndexes (run %d): %v", i+1, err)
		}
	}

	count := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id=2001`)
	if count != 1 {
		t.Fatalf("expected exactly 1 mention after 2 backfill runs (idempotence), got %d", count)
	}
}

// TestBackfillIndexes_AttachmentFTSSearch confirms that after backfill, a
// search for text that appears only in attachment_text returns the right message.
func TestBackfillIndexes_AttachmentFTSSearch(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// The message body itself does not contain "secretword"; it only appears
	// in the inline text near an attachment link.
	const msgHTML = `<p>See attachment</p><p><a class="message_inline_ref" href="/user_uploads/1/xy/report.txt">secretword report text</a></p>`
	seedOldMessage(t, st, 3001, msgHTML)

	// Before backfill, attachment_text in FTS is empty – search returns nothing.
	hitsBefore, err := st.Search(ctx, "secretword", "", false, 10)
	if err != nil {
		t.Fatalf("Search before backfill: %v", err)
	}
	if len(hitsBefore) != 0 {
		t.Fatalf("expected 0 hits before backfill, got %d", len(hitsBefore))
	}

	// Run backfill.
	if err := syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{BatchSize: 50}, os.Stderr); err != nil {
		t.Fatalf("BackfillIndexes: %v", err)
	}

	// After backfill, the attachment text should be searchable.
	hitsAfter, err := st.Search(ctx, "secretword", "", false, 10)
	if err != nil {
		t.Fatalf("Search after backfill: %v", err)
	}
	found := false
	for _, h := range hitsAfter {
		if strings.Contains(h.Snippet, "secretword") || h.MessageID == 3001 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to find message 3001 in search results after backfill, hits=%+v", hitsAfter)
	}
}

// TestBackfillIndexes_EmptyDB confirms backfill on an empty archive does not error.
func TestBackfillIndexes_EmptyDB(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{BatchSize: 50}, os.Stderr); err != nil {
		t.Fatalf("BackfillIndexes on empty DB: %v", err)
	}
}

// TestBackfillIndexes_MultipleMessages confirms batching works across several messages.
func TestBackfillIndexes_MultipleMessages(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// Seed 5 messages – use a small batch size of 2 to exercise pagination.
	for i := int64(1); i <= 5; i++ {
		html := `<p><span class="user-mention" data-user-id="99">@Carol</span></p>`
		seedOldMessage(t, st, 4000+i, html)
	}

	if err := syncer.BackfillIndexes(ctx, st, syncer.BackfillOptions{BatchSize: 2}, os.Stderr); err != nil {
		t.Fatalf("BackfillIndexes: %v", err)
	}

	// Confirm no messages were lost.
	total, err := st.CountMessages(ctx)
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected 5 messages after backfill, got %d", total)
	}

	// Each message should have exactly 1 mention.
	totalMentions := queryCount(t, st, ctx, `SELECT COUNT(*) FROM message_mentions WHERE message_id BETWEEN 4001 AND 4005`)
	if totalMentions != 5 {
		t.Fatalf("expected 5 total mentions (1 per message), got %d", totalMentions)
	}
}
