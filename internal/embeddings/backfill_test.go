package embeddings

import (
	"context"
	"io"
	"path/filepath"
	"strings"
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

// seedTopicsMultiStream seeds topics across two streams (general / dev) and
// returns (generalTopicIDs, devTopicIDs).  Both streams must exist in the db;
// the standard setupEmbedTest already creates stream 10 ("general"), so here we
// also add stream 11 ("dev").
func seedTopicsMultiStream(t *testing.T, st *store.Store, ctx context.Context) ([]int64, []int64) {
	t.Helper()
	if err := st.UpsertStream(ctx, store.Stream{ID: 11, OrgID: 1, Name: "dev"}); err != nil {
		t.Fatalf("UpsertStream dev: %v", err)
	}
	generalNames := []string{"general topic 1", "general topic 2", "general topic 3"}
	devNames := []string{"dev topic alpha", "dev topic beta"}

	msgID := int64(7000)
	seedInStream := func(streamID int64, names []string) []int64 {
		ids := make([]int64, 0, len(names))
		for _, name := range names {
			id, err := st.GetOrCreateTopic(ctx, 1, streamID, name)
			if err != nil {
				t.Fatalf("GetOrCreateTopic %q: %v", name, err)
			}
			if err := st.UpsertMessage(ctx, store.Message{
				ID: msgID, OrgID: 1, StreamID: streamID, TopicID: id,
				SenderID: 20, Content: "content " + name, ContentText: "content " + name,
				Timestamp: "2026-04-29T10:00:00Z",
			}); err != nil {
				t.Fatalf("UpsertMessage for %q: %v", name, err)
			}
			msgID++
			ids = append(ids, id)
		}
		return ids
	}
	return seedInStream(10, generalNames), seedInStream(11, devNames)
}

// TestSemanticSearch_StreamFilter verifies that the stream filter is applied
// before sorting/limiting so that in-stream topics below a global cutoff are
// not silently dropped.
func TestSemanticSearch_StreamFilter(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	_, devIDs := seedTopicsMultiStream(t, st, ctx)

	client := embed.NewFakeClient("fake-model", 8)
	if _, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	// Ask for only 2 results scoped to the "dev" stream; there are exactly 2
	// dev topics so both should be returned regardless of global rank.
	hits, err := SemanticSearch(ctx, st, client, SemanticSearchOptions{
		Query:  "topic content",
		Stream: "dev",
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("SemanticSearch: %v", err)
	}
	if len(hits) != len(devIDs) {
		t.Fatalf("expected %d hits for stream=dev, got %d", len(devIDs), len(hits))
	}
	for _, h := range hits {
		if h.StreamName != "dev" {
			t.Errorf("expected stream=dev, got %q", h.StreamName)
		}
	}
}

// TestSemanticSearch_OnlyUnresolved verifies that resolved topics are excluded
// when OnlyUnresolved is true, mirroring the behaviour of FTS topic search.
func TestSemanticSearch_OnlyUnresolved(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	// Seed three topics; mark the first one resolved.
	ids := seedTopics(t, st, ctx, "resolved topic", "open topic A", "open topic B")
	if err := st.SetTopicResolved(ctx, ids[0], true); err != nil {
		t.Fatalf("mark resolved: %v", err)
	}

	client := embed.NewFakeClient("fake-model", 8)
	if _, err := Backfill(ctx, cfg, st, client, BackfillOptions{}, io.Discard); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	hits, err := SemanticSearch(ctx, st, client, SemanticSearchOptions{
		Query:          "topic",
		Limit:          10,
		OnlyUnresolved: true,
	})
	if err != nil {
		t.Fatalf("SemanticSearch OnlyUnresolved: %v", err)
	}
	for _, h := range hits {
		if h.Resolved {
			t.Errorf("OnlyUnresolved=true but got resolved topic %q", h.TopicName)
		}
	}
	if len(hits) != 2 {
		t.Errorf("expected 2 unresolved hits, got %d", len(hits))
	}
}

// TestBackfill_DimensionMismatchAfterFirstEmbed exercises the P1 fix:
// when stored embeddings exist under a different dimension for the same model
// name, Backfill must abort before writing any new vectors, even though
// client.Dim() returns 0 on construction (i.e. before the first embed call).
func TestBackfill_DimensionMismatchAfterFirstEmbed(t *testing.T) {
	st, cfg, ctx := setupEmbedTest(t)
	// Seed and embed two topics with dim=4.
	seedTopics(t, st, ctx, "existing A", "existing B")
	clientDim4 := embed.NewFakeClient("shared-model", 4)
	cfg.Embeddings.Model = "shared-model"
	if _, err := Backfill(ctx, cfg, st, clientDim4, BackfillOptions{}, io.Discard); err != nil {
		t.Fatalf("initial backfill: %v", err)
	}

	// Now add a new topic that hasn't been embedded yet.
	seedTopics(t, st, ctx, "new topic")

	// Try to backfill with a client that returns dim=8 vectors for the same model.
	clientDim8 := embed.NewFakeClient("shared-model", 8)
	_, err := Backfill(ctx, cfg, st, clientDim8, BackfillOptions{}, io.Discard)
	if err == nil {
		t.Fatal("expected dimension mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "dimension mismatch") {
		t.Errorf("expected 'dimension mismatch' in error, got: %v", err)
	}
}
