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

		// P1 fix: validate dimension against stored embeddings after the first
		// real embed response.  client.Dim() returns 0 until the first Embed call
		// (OllamaClient lazily sets it), so the pre-flight checkModelMismatch at
		// the top of Backfill is a no-op on a fresh client.  We catch the mismatch
		// here — before any writes — using the actual vector length returned by
		// the model, so mixed-dimension vectors can never be written for the same
		// model name.
		if result.Embedded == 0 && len(vecs) > 0 && len(vecs[0]) > 0 {
			if err := checkModelMismatch(ctx, st, model, len(vecs[0])); err != nil {
				return result, err
			}
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
	Query          string
	Stream         string
	Limit          int
	// OnlyUnresolved excludes resolved topics from semantic search results.
	// Mirrors the --unresolved flag available in FTS topic search.
	OnlyUnresolved bool
}

// SemanticSearch performs a brute-force cosine similarity search over stored
// topic embeddings.  It returns an error if no embeddings exist for the model.
//
// Filtering by Stream and OnlyUnresolved is pushed to the SQL layer via
// FilteredEmbeddings so that the limit is applied to the correctly-scoped set.
// Previously, filtering happened after a global sort+limit, which silently
// discarded in-stream or unresolved topics that fell below the global cutoff.
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

	// Load embeddings already filtered by stream and resolved status at the SQL
	// layer.  This is the fix for the stream-filter and unresolved-filter bugs:
	// sorting and limiting now operate on the correctly-scoped set, not on all
	// topics globally.
	entries, err := st.FilteredEmbeddings(ctx, model, opts.Stream, opts.OnlyUnresolved)
	if err != nil {
		return nil, fmt.Errorf("embeddings: load filtered embeddings: %w", err)
	}

	// Validate dimension consistency.
	if len(entries) > 0 && len(entries[0].Vec) != len(qvec) {
		return nil, fmt.Errorf(
			"embeddings: dimension mismatch — stored vectors have dim=%d but query produced dim=%d.\n"+
				"This usually means the model changed. Re-embed with:\n  zulcrawl embeddings backfill --force",
			len(entries[0].Vec), len(qvec),
		)
	}

	type scored struct {
		entry store.EmbeddingEntry
		score float64
	}
	scores := make([]scored, len(entries))
	for i, e := range entries {
		scores[i] = scored{entry: e, score: embed.CosineSimilarity(qvec, e.Vec)}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	if len(scores) > limit {
		scores = scores[:limit]
	}

	out := make([]SemanticHit, 0, len(scores))
	for _, s := range scores {
		out = append(out, SemanticHit{
			TopicID:       s.entry.TopicID,
			StreamName:    s.entry.StreamName,
			TopicName:     s.entry.TopicName,
			Resolved:      s.entry.Resolved,
			MessageCount:  s.entry.MessageCount,
			LastMessageAt: s.entry.LastMessageAt,
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
