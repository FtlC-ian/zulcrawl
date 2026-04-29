package embeddings

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/embed"
	"github.com/FtlC-ian/zulcrawl/internal/store"
)

func setupEmbedTest(t *testing.T) (*store.Store, *config.Config, context.Context) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
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
	if err := st.UpsertStream(ctx, store.Stream{ID: 10, OrgID: 1, Name: "general"}); err != nil {
		t.Fatalf("UpsertStream: %v", err)
	}
	if err := st.UpsertUser(ctx, store.User{ID: 20, OrgID: 1, FullName: "Alice"}); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	cfg := config.Default()
	cfg.Embeddings.Enabled = true
	cfg.Embeddings.Model = "fake-model"
	cfg.Embeddings.BatchSize = 2
	return st, cfg, ctx
}

func seedTopics(t *testing.T, st *store.Store, ctx context.Context, names ...string) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(names))
	for i, name := range names {
		id, err := st.GetOrCreateTopic(ctx, 1, 10, name)
		if err != nil {
			t.Fatalf("GetOrCreateTopic %q: %v", name, err)
		}
		// Insert one message so TopicText returns something.
		if err := st.UpsertMessage(ctx, store.Message{
			ID: int64(5000 + i), OrgID: 1, StreamID: 10, TopicID: id,
			SenderID: 20, Content: "msg " + name, ContentText: "msg " + name,
			Timestamp: "2026-04-29T10:00:00Z",
		}); err != nil {
			t.Fatalf("UpsertMessage for %q: %v", name, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestBackfill_AllTopicsEmbedded(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	_ = seedTopics(t, st, ctx, "topic A", "topic B", "topic C")

	client := embed.NewFakeClient("fake-model", 8)
	result, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if result.Embedded != 3 {
		t.Errorf("expected Embedded=3, got %d", result.Embedded)
	}
	if result.Skipped != 0 {
		t.Errorf("expected Skipped=0, got %d", result.Skipped)
	}
}

func TestBackfill_AlreadyEmbedded(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	ids := seedTopics(t, st, ctx, "topic X", "topic Y")

	client := embed.NewFakeClient("fake-model", 4)
	// First run.
	_, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard)
	if err != nil {
		t.Fatalf("first Backfill: %v", err)
	}

	// Verify embedded.
	missing, _ := st.TopicsNeedingEmbedding(ctx, "fake-model", 0)
	if len(missing) != 0 {
		t.Fatalf("expected 0 missing after backfill, got %d", len(missing))
	}
	_ = ids

	// Second run should embed 0 additional.
	result2, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard)
	if err != nil {
		t.Fatalf("second Backfill: %v", err)
	}
	if result2.Embedded != 0 {
		t.Errorf("second run should embed 0, got %d", result2.Embedded)
	}
}

func TestBackfill_Disabled(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	cfg.Embeddings.Enabled = false
	client := embed.NewFakeClient("fake-model", 4)
	_, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard)
	if err == nil {
		t.Fatal("expected error when embeddings disabled")
	}
}

func TestBackfill_Limit(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	seedTopics(t, st, ctx, "t1", "t2", "t3", "t4", "t5")

	client := embed.NewFakeClient("fake-model", 4)
	result, err := Backfill(ctx, cfg, st, client, BackfillOptions{Limit: 3}, io.Discard)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if result.Embedded != 3 {
		t.Errorf("expected Embedded=3 with limit=3, got %d", result.Embedded)
	}
}

func TestSemanticSearch_Basic(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	seedTopics(t, st, ctx, "deployment", "onboarding", "random banter")

	client := embed.NewFakeClient("fake-model", 8)

	// Backfill first.
	if _, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	hits, err := SemanticSearch(ctx, st, client, SemanticSearchOptions{
		Query: "deploy release",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SemanticSearch: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("expected at least one hit")
	}
	// All scores should be in [-1, 1].
	for _, h := range hits {
		if h.Score < -1.1 || h.Score > 1.1 {
			t.Errorf("unexpected score %f for topic %q", h.Score, h.TopicName)
		}
	}
}

func TestSemanticSearch_NoEmbeddings(t *testing.T) {
	st, _, ctx := setupEmbedTest(t)
	client := embed.NewFakeClient("fake-model", 4)
	_, err := SemanticSearch(ctx, st, client, SemanticSearchOptions{Query: "test"})
	if err == nil {
		t.Fatal("expected error when no embeddings exist")
	}
}
