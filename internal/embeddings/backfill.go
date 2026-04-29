// Package embeddings provides topic embedding backfill and semantic search.
//
// Embeddings are disabled by default.  They require:
//   - [embeddings] enabled = true in config
//   - An Ollama server running locally
//   - The configured model pulled: ollama pull nomic-embed-text-v2-moe
//
// No paid or cloud APIs are called.  No Python is required.  No sqlite-vec
// or HNSW index is used — brute-force cosine similarity is sufficient for
// the expected corpus size.
package embeddings

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/embed"
	"github.com/FtlC-ian/zulcrawl/internal/store"
)

// BackfillOptions controls how the backfill command runs.
type BackfillOptions struct {
	// BatchSize is the number of topics sent to the embedding client at once.
	// Defaults to cfg.Embeddings.BatchSize.
	BatchSize int
	// Limit caps the total number of topics to embed. 0 = all.
	Limit int
}

// BackfillResult summarises what happened.
type BackfillResult struct {
	Embedded  int
	Skipped   int
	AlreadyOK int
}

// Backfill builds embeddings for all topics that do not yet have one for the
// configured model.  It logs progress to w.
func Backfill(ctx context.Context, cfg *config.Config, st *store.Store, client embed.Client, opts BackfillOptions, w io.Writer) (*BackfillResult, error) {
	if !cfg.Embeddings.Enabled {
		return nil, fmt.Errorf("embeddings: feature is disabled — set [embeddings] enabled = true in config")
	}

	model := client.Model()
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = cfg.Embeddings.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 32
	}

	// Detect model/dimension mismatch with existing stored embeddings.
	if err := checkModelMismatch(ctx, st, model, client.Dim()); err != nil {
		return nil, err
	}

	// Get all topic IDs that still need embedding.
	topicIDs, err := st.TopicsNeedingEmbedding(ctx, model, opts.Limit)
	if err != nil {
		return nil, fmt.Errorf("embeddings: list topics: %w", err)
	}

	result := &BackfillResult{}
	total := len(topicIDs)
	if total == 0 {
		fmt.Fprintln(w, "embeddings: all topics are already embedded.")
		return result, nil
	}
	fmt.Fprintf(w, "embeddings: %d topics to embed with model %q\n", total, model)

	done := 0
	for start := 0; start < len(topicIDs); start += batchSize {
		end := start + batchSize
		if end > len(topicIDs) {
			end = len(topicIDs)
		}
		batch := topicIDs[start:end]

		texts := make([]string, 0, len(batch))
		validIDs := make([]int64, 0, len(batch))
		for _, id := range batch {
			text, err := st.TopicText(ctx, id, cfg.Embeddings.SampleMessages)
			if err != nil {
				fmt.Fprintf(w, "  skip topic %d: %v\n", id, err)
				result.Skipped++
				continue
			}
			texts = append(texts, text)
			validIDs = append(validIDs, id)
		}
		if len(texts) == 0 {
			continue
		}

		vecs, err := client.Embed(ctx, texts)
		if err != nil {
			return result, fmt.Errorf("embeddings: embed batch starting at topic %d: %w", batch[0], err)
		}
		if len(vecs) != len(validIDs) {
			return result, fmt.Errorf("embeddings: expected %d vectors, got %d", len(validIDs), len(vecs))
		}

		for i, id := range validIDs {
			if err := st.UpsertTopicEmbedding(ctx, id, model, cfg.Embeddings.Provider, vecs[i]); err != nil {
				return result, fmt.Errorf("embeddings: store topic %d: %w", id, err)
			}
			result.Embedded++
		}

		done += len(batch)
		pct := 100 * done / total
		fmt.Fprintf(w, "  %d/%d (%d%%) embedded\n", done, total, pct)
	}

	fmt.Fprintf(w, "embeddings: done. embedded=%d skipped=%d\n", result.Embedded, result.Skipped)
	return result, nil
}

// SemanticHit is a single topic result from semantic search.
type SemanticHit struct {
	TopicID       int64
	StreamName    string
	TopicName     string
	Resolved      bool
	MessageCount  int
	LastMessageAt string
	Score         float64
}

// SemanticSearchOptions controls semantic search.
type SemanticSearchOptions struct {
	Query  string
	Stream string
	Limit  int
}

// SemanticSearch performs a brute-force cosine similarity search over stored
// topic embeddings.  It returns an error if no embeddings exist for the model.
func SemanticSearch(ctx context.Context, st *store.Store, client embed.Client, opts SemanticSearchOptions) ([]SemanticHit, error) {
	model := client.Model()
	info, err := st.EmbeddingStats(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("embeddings: check stats: %w", err)
	}
	if info == nil || info.Count == 0 {
		return nil, fmt.Errorf(
			"embeddings: no embeddings found for model %q — run `zulcrawl embeddings backfill` first",
			model,
		)
	}

	// Embed the query.
	qvecs, err := client.Embed(ctx, []string{opts.Query})
	if err != nil {
		return nil, fmt.Errorf("embeddings: embed query: %w", err)
	}
	qvec := qvecs[0]

	// Load all vectors.
	ids, vecs, err := st.AllEmbeddings(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("embeddings: load embeddings: %w", err)
	}

	// Validate dimension consistency.
	if len(vecs) > 0 && len(vecs[0]) != len(qvec) {
		return nil, fmt.Errorf(
			"embeddings: dimension mismatch — stored vectors have dim=%d but query produced dim=%d.\n"+
				"This usually means the model changed. Re-embed with:\n  zulcrawl embeddings backfill --force",
			len(vecs[0]), len(qvec),
		)
	}

	type scored struct {
		id    int64
		score float64
	}
	scores := make([]scored, len(ids))
	for i, v := range vecs {
		scores[i] = scored{id: ids[i], score: embed.CosineSimilarity(qvec, v)}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	if len(scores) > limit {
		scores = scores[:limit]
	}

	// Fetch topic metadata for results.
	out := make([]SemanticHit, 0, len(scores))
	for _, s := range scores {
		meta, err := st.TopicMeta(ctx, s.id)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("embeddings: get topic meta %d: %w", s.id, err)
		}
		if opts.Stream != "" && meta.StreamName != opts.Stream {
			continue
		}
		out = append(out, SemanticHit{
			TopicID:       s.id,
			StreamName:    meta.StreamName,
			TopicName:     meta.TopicName,
			Resolved:      meta.Resolved,
			MessageCount:  meta.MessageCount,
			LastMessageAt: meta.LastMessageAt,
			Score:         s.score,
		})
	}
	return out, nil
}

// checkModelMismatch returns an error if there are already embeddings stored
// under a *different* dimension for the same model (can't happen) or if the
// stored dimension doesn't match what the client will produce (dim=0 means
// unknown, skip check).
func checkModelMismatch(ctx context.Context, st *store.Store, model string, clientDim int) error {
	if clientDim <= 0 {
		return nil // dimension not yet known, skip
	}
	info, err := st.EmbeddingStats(ctx, model)
	if err != nil {
		return fmt.Errorf("embeddings: check stored dimension: %w", err)
	}
	if info == nil {
		return nil // no stored embeddings yet
	}
	if info.Dim != clientDim {
		return fmt.Errorf(
			"embeddings: dimension mismatch for model %q — stored=%d, client=%d.\n"+
				"This usually means you changed the model. Re-embed all topics with:\n"+
				"  zulcrawl embeddings backfill --force",
			model, info.Dim, clientDim,
		)
	}
	return nil
}
