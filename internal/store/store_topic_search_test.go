package store

import (
	"context"
	"testing"
)

// seedTopicSearchDB creates a small fixture DB for topic-search tests.
// Returns (store, ctx).
//
// Schema:
//
//	stream "general" (id=10)
//	  topic "deployment pipeline" (resolved)     — messages about deploy/release
//	  topic "onboarding checklist" (unresolved)  — messages about onboarding setup
//	  topic "random banter" (unresolved)         — off-topic messages
//	stream "dev" (id=20)
//	  topic "bug in database layer" (unresolved) — messages about database bug
func seedTopicSearchDB(t *testing.T) (*Store, context.Context) {
	t.Helper()
	st, ctx := setupTestStore(t) // reuse helper from store_test.go (same package)

	// Extra stream
	if err := st.UpsertStream(ctx, Stream{ID: 20, OrgID: 1, Name: "dev"}); err != nil {
		t.Fatalf("UpsertStream dev: %v", err)
	}
	// Extra user
	if err := st.UpsertUser(ctx, User{ID: 21, OrgID: 1, FullName: "Alice"}); err != nil {
		t.Fatalf("UpsertUser Alice: %v", err)
	}

	topics := []struct {
		streamID int64
		name     string
	}{
		{10, "deployment pipeline"},
		{10, "onboarding checklist"},
		{10, "random banter"},
		{20, "bug in database layer"},
	}
	topicIDs := map[string]int64{}
	for _, tp := range topics {
		id, err := st.GetOrCreateTopic(ctx, 1, tp.streamID, tp.name)
		if err != nil {
			t.Fatalf("GetOrCreateTopic %q: %v", tp.name, err)
		}
		topicIDs[tp.name] = id
	}

	// Mark "deployment pipeline" resolved by updating the row directly.
	if _, err := st.db.ExecContext(ctx, `UPDATE topics SET resolved=1 WHERE id=?`, topicIDs["deployment pipeline"]); err != nil {
		t.Fatalf("mark resolved: %v", err)
	}

	type msgFixture struct {
		id        int64
		streamID  int64
		topicName string
		senderID  int64
		ts        string
		content   string
	}
	msgs := []msgFixture{
		// deployment pipeline — recent
		{1001, 10, "deployment pipeline", 20, "2026-04-28T10:00:00Z", "Released v2.5 to production. Deploy went smoothly."},
		{1002, 10, "deployment pipeline", 21, "2026-04-28T11:00:00Z", "CI pipeline passed all checks before the deploy."},
		// onboarding checklist
		{2001, 10, "onboarding checklist", 20, "2026-04-01T09:00:00Z", "New engineer onboarding setup steps: install Go, clone repo."},
		{2002, 10, "onboarding checklist", 21, "2026-04-02T10:00:00Z", "Onboarding doc updated with Docker setup instructions."},
		// random banter — old
		{3001, 10, "random banter", 20, "2025-01-10T08:00:00Z", "Did you see the game last night?"},
		// bug in database layer
		{4001, 20, "bug in database layer", 20, "2026-04-25T14:00:00Z", "Found a nil pointer dereference in the database query function."},
		{4002, 20, "bug in database layer", 21, "2026-04-26T15:00:00Z", "Fixed the database bug. Patch pushed."},
	}
	for _, m := range msgs {
		if err := st.UpsertMessage(ctx, Message{
			ID:          m.id,
			OrgID:       1,
			StreamID:    m.streamID,
			TopicID:     topicIDs[m.topicName],
			SenderID:    m.senderID,
			Content:     m.content,
			ContentText: m.content,
			Timestamp:   m.ts,
		}); err != nil {
			t.Fatalf("UpsertMessage %d: %v", m.id, err)
		}
	}
	return st, ctx
}

// TestSearchTopics_ByTopicName verifies that topics whose name matches the query
// are returned (leg 1: topics_fts).
func TestSearchTopics_ByTopicName(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "onboarding", Limit: 10})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("expected at least one hit for 'onboarding', got none")
	}
	found := false
	for _, h := range hits {
		if h.TopicName == "onboarding checklist" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'onboarding checklist' in results; got %+v", hits)
	}
}

// TestSearchTopics_ByMessageContent verifies that topics are found via their
// message content even when the query term is not in the topic name (leg 2:
// messages_fts).
func TestSearchTopics_ByMessageContent(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	// "nil pointer" is only in message content for "bug in database layer"
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "pointer", Limit: 10})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("expected hit for 'pointer' via message content, got none")
	}
	found := false
	for _, h := range hits {
		if h.TopicName == "bug in database layer" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'bug in database layer' in results; got %+v", hits)
	}
}

// TestSearchTopics_HybridBothLegs verifies that a query can match via both leg 1
// (topic name) and leg 2 (message content) and that the result is not duplicated.
func TestSearchTopics_HybridBothLegs(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	// "database" appears in both topic name ("bug in database layer") and message
	// content for that same topic.
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "database", Limit: 10})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	// Count how many times we see "bug in database layer" — should be exactly 1.
	count := 0
	for _, h := range hits {
		if h.TopicName == "bug in database layer" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected topic 'bug in database layer' exactly once, got %d times; results: %+v", count, hits)
	}
}

// TestSearchTopics_StreamFilter verifies the --stream filter is honored.
func TestSearchTopics_StreamFilter(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	// "deploy" appears in general/deployment pipeline; filtering to dev should yield 0.
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{
		Query:  "deploy",
		Stream: "dev",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	for _, h := range hits {
		if h.StreamName != "dev" {
			t.Errorf("expected only dev stream results, got stream %q", h.StreamName)
		}
	}
}

// TestSearchTopics_OnlyUnresolved verifies that resolved topics are excluded
// when OnlyUnresolved is set.
func TestSearchTopics_OnlyUnresolved(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	// "deploy" matches "deployment pipeline" which is resolved.
	// With OnlyUnresolved=true it should be excluded.
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{
		Query:          "deploy",
		OnlyUnresolved: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	for _, h := range hits {
		if h.Resolved {
			t.Errorf("OnlyUnresolved=true but got resolved topic %q", h.TopicName)
		}
	}
}

// TestSearchTopics_EmptyQuery verifies that an empty query returns an error.
func TestSearchTopics_EmptyQuery(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)
	_, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "", Limit: 10})
	if err == nil {
		t.Fatal("expected error for empty query, got nil")
	}
}

// TestSearchTopics_NoResults verifies that a query matching nothing returns
// an empty slice (not an error).
func TestSearchTopics_NoResults(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "xyzzy_nonexistent", Limit: 10})
	if err != nil {
		t.Fatalf("unexpected error for no-match query: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("expected 0 results, got %d", len(hits))
	}
}

// TestSearchTopics_LimitHonored verifies that the Limit option is respected.
func TestSearchTopics_LimitHonored(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	// "a" matches pretty much everything via FTS porter stemmer.
	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "deploy", Limit: 1})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	if len(hits) > 1 {
		t.Fatalf("expected at most 1 result with Limit=1, got %d", len(hits))
	}
}

// TestSearchTopics_SnippetPopulated verifies that BestSnippet is non-empty for
// results that matched via message content.
func TestSearchTopics_SnippetPopulated(t *testing.T) {
	st, ctx := seedTopicSearchDB(t)

	hits, err := st.SearchTopics(ctx, TopicSearchOptions{Query: "pipeline", Limit: 10})
	if err != nil {
		t.Fatalf("SearchTopics: %v", err)
	}
	for _, h := range hits {
		if h.TopicName == "deployment pipeline" && h.BestSnippet == "" {
			t.Errorf("expected BestSnippet to be populated for 'deployment pipeline'")
		}
	}
}
