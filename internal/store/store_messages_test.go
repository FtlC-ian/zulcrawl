package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/store"
)

// openTestStore opens a fresh in-memory-style SQLite DB in a temp dir.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return st
}

// seedDB inserts the minimum org/stream/user/topic/message fixtures needed
// by the query-messages tests. Returns stream IDs keyed by name.
func seedDB(t *testing.T, st *store.Store) map[string]int64 {
	t.Helper()
	ctx := context.Background()

	if err := st.UpsertOrganization(ctx, 1, "https://z.example.com", "TestOrg"); err != nil {
		t.Fatal(err)
	}

	streams := map[string]int64{"general": 1, "dev": 2}
	for name, id := range streams {
		if err := st.UpsertStream(ctx, store.Stream{ID: id, OrgID: 1, Name: name}); err != nil {
			t.Fatalf("UpsertStream %s: %v", name, err)
		}
	}

	users := []store.User{
		{ID: 10, OrgID: 1, FullName: "Alice"},
		{ID: 11, OrgID: 1, FullName: "Bob"},
	}
	for _, u := range users {
		if err := st.UpsertUser(ctx, u); err != nil {
			t.Fatalf("UpsertUser %s: %v", u.FullName, err)
		}
	}

	// topic IDs are autoincrement; seed order matters.
	topicFixtures := []struct {
		streamID int64
		name     string
	}{
		{1, "deploys"},
		{1, "random"},
		{2, "bugs"},
	}
	topicIDs := map[string]int64{}
	for _, tf := range topicFixtures {
		id, err := st.GetOrCreateTopic(ctx, 1, tf.streamID, tf.name)
		if err != nil {
			t.Fatalf("GetOrCreateTopic %s: %v", tf.name, err)
		}
		topicIDs[tf.name] = id
	}

	now := time.Now().UTC()
	msgs := []struct {
		id        int64
		streamID  int64
		topicName string
		senderID  int64
		ts        time.Time
		content   string
	}{
		// general/deploys – 3 messages
		{1001, 1, "deploys", 10, now.Add(-8 * 24 * time.Hour), "deployed v1"},
		{1002, 1, "deploys", 11, now.Add(-5 * 24 * time.Hour), "deployed v2"},
		{1003, 1, "deploys", 10, now.Add(-1 * time.Hour), "deployed v3"},
		// general/random – 2 messages
		{2001, 1, "random", 11, now.Add(-2 * 24 * time.Hour), "hello world"},
		{2002, 1, "random", 10, now.Add(-30 * time.Minute), "bye world"},
		// dev/bugs – 1 message
		{3001, 2, "bugs", 11, now.Add(-3 * 24 * time.Hour), "nil pointer"},
	}
	for _, m := range msgs {
		if err := st.UpsertMessage(ctx, store.Message{
			ID:          m.id,
			OrgID:       1,
			StreamID:    m.streamID,
			TopicID:     topicIDs[m.topicName],
			SenderID:    m.senderID,
			Content:     m.content,
			ContentText: m.content,
			Timestamp:   m.ts.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("UpsertMessage %d: %v", m.id, err)
		}
	}
	return streams
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestQueryMessages_ByStream(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Stream: "general",
		Limit:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 messages in 'general', got %d", len(rows))
	}
	// Verify oldest-first order.
	for i := 1; i < len(rows); i++ {
		if rows[i].Timestamp < rows[i-1].Timestamp {
			t.Errorf("messages not in ascending order at index %d", i)
		}
	}
}

func TestQueryMessages_ByStreamAndTopic(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Stream: "general",
		Topic:  "deploys",
		Limit:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 messages in deploys, got %d", len(rows))
	}
}

func TestQueryMessages_BySender(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Sender: "Alice",
		Limit:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Alice sent: 1001, 1003, 2002 = 3
	if len(rows) != 3 {
		t.Fatalf("expected 3 messages from Alice, got %d", len(rows))
	}
	for _, r := range rows {
		if r.SenderName != "Alice" {
			t.Errorf("expected sender Alice, got %s", r.SenderName)
		}
	}
}

func TestQueryMessages_ByDays(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	// Only messages from last 3 days: 1003 (1h ago), 2001 (2d ago), 2002 (30m ago), 3001 (3d ago)
	// Boundary is fuzzy; use 4 days to be safe about rounding.
	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Days:  4,
		Limit: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("expected some messages from last 4 days")
	}
	// None should be older than 4 days.
	cutoff := time.Now().UTC().Add(-4 * 24 * time.Hour)
	for _, r := range rows {
		ts, err := time.Parse(time.RFC3339, r.Timestamp)
		if err != nil {
			t.Errorf("bad timestamp %q: %v", r.Timestamp, err)
			continue
		}
		if ts.Before(cutoff) {
			t.Errorf("message %d is older than 4 days: %s", r.ID, r.Timestamp)
		}
	}
}

func TestQueryMessages_ByHours(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Hours: 2,
		Limit: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Expect: 1003 (1h ago), 2002 (30m ago) = 2
	if len(rows) != 2 {
		t.Fatalf("expected 2 messages in last 2 hours, got %d", len(rows))
	}
}

func TestQueryMessages_BySince(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	since := time.Now().UTC().Add(-6 * 24 * time.Hour).Format(time.RFC3339)
	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Since: since,
		Limit: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 1002 (5d), 1003 (1h), 2001 (2d), 2002 (30m), 3001 (3d) = 5
	if len(rows) != 5 {
		t.Fatalf("expected 5 messages since 6 days ago, got %d", len(rows))
	}
}

func TestQueryMessages_LastN(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Sender: "Bob", // Bob: 1002, 2001, 3001
		Last:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 messages (last 2 from Bob), got %d", len(rows))
	}
	// Must be oldest-first (ascending).
	if rows[0].Timestamp > rows[1].Timestamp {
		t.Errorf("last N result is not oldest-first: %s > %s", rows[0].Timestamp, rows[1].Timestamp)
	}
	// The 2 newest from Bob should be 2001 and 3001 (relative to the 3 Bob msgs).
	// Order: 2001 then 3001 (ascending).
}

func TestQueryMessages_DefaultLimit(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	// No explicit limit (0 = default 200).
	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Stream: "general",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Should get all 5 general messages (well under default 200).
	if len(rows) != 5 {
		t.Fatalf("expected 5, got %d", len(rows))
	}
}

func TestQueryMessages_AllFlag(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	// -1 = no cap
	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Stream: "general",
		Limit:  -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 with --all, got %d", len(rows))
	}
}

func TestQueryMessages_DevStream(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	rows, err := st.QueryMessages(context.Background(), store.MessagesFilter{
		Stream: "dev",
		Limit:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 message in dev, got %d", len(rows))
	}
	if rows[0].StreamName != "dev" {
		t.Errorf("expected stream dev, got %s", rows[0].StreamName)
	}
}

// Verify that the os package import is not needed beyond tempdir.
var _ = os.DevNull

func TestQueryMessages_SenderEscapesLikeWildcards(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	seedDB(t, st)

	ctx := context.Background()
	if err := st.UpsertUser(ctx, store.User{ID: 12, OrgID: 1, FullName: "A_lice"}); err != nil {
		t.Fatal(err)
	}
	topicID, err := st.GetOrCreateTopic(ctx, 1, 1, "deploys")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMessage(ctx, store.Message{
		ID:          4001,
		OrgID:       1,
		StreamID:    1,
		TopicID:     topicID,
		SenderID:    12,
		Content:     "literal underscore sender",
		ContentText: "literal underscore sender",
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := st.QueryMessages(ctx, store.MessagesFilter{Sender: "A_", Limit: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SenderName != "A_lice" {
		t.Fatalf("expected only literal underscore sender, got %#v", rows)
	}
}
