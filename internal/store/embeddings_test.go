package store

import (
	"database/sql"
	"testing"
)

func TestUpsertAndGetTopicEmbedding(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "embed test topic")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}

	vec := []float32{0.1, 0.2, 0.3, 0.4}
	if err := st.UpsertTopicEmbedding(ctx, topicID, "test-model", "ollama", vec); err != nil {
		t.Fatalf("UpsertTopicEmbedding: %v", err)
	}

	rec, err := st.GetTopicEmbedding(ctx, topicID, "test-model")
	if err != nil {
		t.Fatalf("GetTopicEmbedding: %v", err)
	}
	if len(rec.Vec) != len(vec) {
		t.Fatalf("vec length: want %d got %d", len(vec), len(rec.Vec))
	}
	for i := range vec {
		if rec.Vec[i] != vec[i] {
			t.Errorf("vec[%d]: want %v got %v", i, vec[i], rec.Vec[i])
		}
	}
}

func TestUpsertTopicEmbeddingIdempotent(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, _ := st.GetOrCreateTopic(ctx, 1, 10, "embed idempotent")

	vec1 := []float32{0.1, 0.9, 0.0, 0.0}
	vec2 := []float32{0.0, 0.0, 0.6, 0.8}

	if err := st.UpsertTopicEmbedding(ctx, topicID, "m1", "ollama", vec1); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := st.UpsertTopicEmbedding(ctx, topicID, "m1", "ollama", vec2); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	rec, _ := st.GetTopicEmbedding(ctx, topicID, "m1")
	if rec.Vec[2] != vec2[2] {
		t.Errorf("expected updated vector, got old value")
	}
}

func TestGetTopicEmbedding_NotFound(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, _ := st.GetOrCreateTopic(ctx, 1, 10, "embed notfound")
	_, err := st.GetTopicEmbedding(ctx, topicID, "nonexistent-model")
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestTopicsNeedingEmbedding(t *testing.T) {
	st, ctx := setupTestStore(t)
	t1, _ := st.GetOrCreateTopic(ctx, 1, 10, "topic 1")
	t2, _ := st.GetOrCreateTopic(ctx, 1, 10, "topic 2")
	t3, _ := st.GetOrCreateTopic(ctx, 1, 10, "topic 3")

	// Embed only t1 and t2.
	_ = st.UpsertTopicEmbedding(ctx, t1, "m1", "ollama", []float32{1, 0})
	_ = st.UpsertTopicEmbedding(ctx, t2, "m1", "ollama", []float32{0, 1})

	// t3 should be missing.
	missing, err := st.TopicsNeedingEmbedding(ctx, "m1", 0)
	if err != nil {
		t.Fatalf("TopicsNeedingEmbedding: %v", err)
	}
	if len(missing) != 1 || missing[0] != t3 {
		t.Errorf("expected [%d], got %v", t3, missing)
	}
}

func TestEmbeddingStats(t *testing.T) {
	st, ctx := setupTestStore(t)
	t1, _ := st.GetOrCreateTopic(ctx, 1, 10, "stats t1")
	t2, _ := st.GetOrCreateTopic(ctx, 1, 10, "stats t2")

	_ = st.UpsertTopicEmbedding(ctx, t1, "nomic-embed-text-v2-moe", "ollama", []float32{1, 0, 0})
	_ = st.UpsertTopicEmbedding(ctx, t2, "nomic-embed-text-v2-moe", "ollama", []float32{0, 1, 0})

	info, err := st.EmbeddingStats(ctx, "nomic-embed-text-v2-moe")
	if err != nil {
		t.Fatalf("EmbeddingStats: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil info")
	}
	if info.Count != 2 {
		t.Errorf("expected count=2, got %d", info.Count)
	}
	if info.Dim != 3 {
		t.Errorf("expected dim=3, got %d", info.Dim)
	}
}

func TestAllEmbeddings(t *testing.T) {
	st, ctx := setupTestStore(t)
	t1, _ := st.GetOrCreateTopic(ctx, 1, 10, "all t1")
	t2, _ := st.GetOrCreateTopic(ctx, 1, 10, "all t2")

	v1 := []float32{1, 0}
	v2 := []float32{0, 1}
	_ = st.UpsertTopicEmbedding(ctx, t1, "m", "ollama", v1)
	_ = st.UpsertTopicEmbedding(ctx, t2, "m", "ollama", v2)

	ids, vecs, err := st.AllEmbeddings(ctx, "m")
	if err != nil {
		t.Fatalf("AllEmbeddings: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 embeddings, got %d", len(ids))
	}
	_ = vecs
}

func TestTopicText(t *testing.T) {
	st, ctx := setupTestStore(t)
	topicID, _ := st.GetOrCreateTopic(ctx, 1, 10, "text topic")

	_ = st.UpsertMessage(ctx, Message{
		ID: 9001, OrgID: 1, StreamID: 10, TopicID: topicID, SenderID: 20,
		Content: "hello world", ContentText: "hello world",
		Timestamp: "2026-04-29T10:00:00Z",
	})

	text, err := st.TopicText(ctx, topicID, 10)
	if err != nil {
		t.Fatalf("TopicText: %v", err)
	}
	if text == "" {
		t.Error("expected non-empty topic text")
	}
}
