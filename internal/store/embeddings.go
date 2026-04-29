package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
)

// EmbeddingRecord represents one stored embedding row.
type EmbeddingRecord struct {
	TopicID   int64
	Model     string
	Provider  string
	Dimension int
	Vec       []float32
}

// embeddingSchema is appended to InitSchema.
const embeddingSchema = `
-- topic_embeddings stores one normalised float32 vector per (topic, model).
-- Vectors are stored as little-endian IEEE-754 bytes.
-- A unique constraint on (topic_id, model) makes re-embedding idempotent.
CREATE TABLE IF NOT EXISTS topic_embeddings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_id   INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    model      TEXT    NOT NULL,
    provider   TEXT    NOT NULL DEFAULT 'ollama',
    dimension  INTEGER NOT NULL,
    vector     BLOB    NOT NULL,
    indexed_at TEXT    NOT NULL,
    UNIQUE(topic_id, model)
);
CREATE INDEX IF NOT EXISTS idx_topic_embeddings_topic ON topic_embeddings(topic_id);
CREATE INDEX IF NOT EXISTS idx_topic_embeddings_model ON topic_embeddings(model);
`

// InitEmbeddingSchema creates the embeddings table if it does not exist.
// It is called by InitSchema; callers should not need to invoke it directly.
func (s *Store) InitEmbeddingSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, embeddingSchema)
	return err
}

// UpsertTopicEmbedding stores or replaces a topic embedding.
// indexed_at is set to the current UTC timestamp.
func (s *Store) UpsertTopicEmbedding(ctx context.Context, topicID int64, model, provider string, vec []float32) error {
	if len(vec) == 0 {
		return fmt.Errorf("store: cannot upsert empty embedding for topic %d", topicID)
	}
	blob := encodeVec(vec)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO topic_embeddings(topic_id, model, provider, dimension, vector, indexed_at)
VALUES (?, ?, ?, ?, ?, datetime('now'))
ON CONFLICT(topic_id, model) DO UPDATE SET
    provider   = excluded.provider,
    dimension  = excluded.dimension,
    vector     = excluded.vector,
    indexed_at = excluded.indexed_at`,
		topicID, model, provider, len(vec), blob,
	)
	return err
}

// GetTopicEmbedding retrieves the stored embedding for a topic and model.
// Returns sql.ErrNoRows if none exists.
func (s *Store) GetTopicEmbedding(ctx context.Context, topicID int64, model string) (*EmbeddingRecord, error) {
	var rec EmbeddingRecord
	var blob []byte
	err := s.db.QueryRowContext(ctx, `
SELECT topic_id, model, provider, dimension, vector
FROM topic_embeddings
WHERE topic_id = ? AND model = ?`, topicID, model).Scan(
		&rec.TopicID, &rec.Model, &rec.Provider, &rec.Dimension, &blob,
	)
	if err != nil {
		return nil, err
	}
	vec, err := decodeVec(blob)
	if err != nil {
		return nil, fmt.Errorf("store: decode embedding for topic %d: %w", topicID, err)
	}
	rec.Vec = vec
	return &rec, nil
}

// EmbeddingModelInfo describes the model and dimension currently stored.
type EmbeddingModelInfo struct {
	Model    string
	Provider string
	Dim      int
	Count    int64
}

// EmbeddingStats returns stored model info for a given model name, or nil if none.
func (s *Store) EmbeddingStats(ctx context.Context, model string) (*EmbeddingModelInfo, error) {
	var info EmbeddingModelInfo
	err := s.db.QueryRowContext(ctx, `
SELECT model, provider, dimension, COUNT(*) AS cnt
FROM topic_embeddings
WHERE model = ?
GROUP BY model, provider, dimension`, model).Scan(
		&info.Model, &info.Provider, &info.Dim, &info.Count,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// TopicsNeedingEmbedding returns topic IDs that have no embedding for model,
// up to limit rows. Passing limit ≤ 0 returns all.
func (s *Store) TopicsNeedingEmbedding(ctx context.Context, model string, limit int) ([]int64, error) {
	q := `
SELECT t.id
FROM topics t
WHERE NOT EXISTS (
    SELECT 1 FROM topic_embeddings e WHERE e.topic_id = t.id AND e.model = ?
)
ORDER BY t.id ASC`
	var args []any
	args = append(args, model)
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// TopicText returns a concatenated text representation of a topic suitable for
// embedding: topic name followed by a sample of message content.
// sampleMessages limits how many messages are included; 0 means 50.
func (s *Store) TopicText(ctx context.Context, topicID int64, sampleMessages int) (string, error) {
	if sampleMessages <= 0 {
		sampleMessages = 50
	}
	// Get topic name and stream name.
	var topicName, streamName string
	err := s.db.QueryRowContext(ctx, `
SELECT t.name, st.name
FROM topics t
JOIN streams st ON st.id = t.stream_id
WHERE t.id = ?`, topicID).Scan(&topicName, &streamName)
	if err != nil {
		return "", fmt.Errorf("store: topic text topic %d: %w", topicID, err)
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT content_text
FROM messages
WHERE topic_id = ?
ORDER BY id ASC
LIMIT ?`, topicID, sampleMessages)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var sb []string
	sb = append(sb, "#"+streamName+" > "+topicName)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return "", err
		}
		if t != "" {
			sb = append(sb, t)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	result := ""
	for i, s := range sb {
		if i > 0 {
			result += "\n"
		}
		result += s
	}
	return result, nil
}

// AllEmbeddings returns all stored embeddings for the given model, for brute-force
// cosine search. Returns (topicIDs, vectors).
func (s *Store) AllEmbeddings(ctx context.Context, model string) ([]int64, [][]float32, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT topic_id, vector
FROM topic_embeddings
WHERE model = ?
ORDER BY topic_id ASC`, model)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var ids []int64
	var vecs [][]float32
	for rows.Next() {
		var id int64
		var blob []byte
		if err := rows.Scan(&id, &blob); err != nil {
			return nil, nil, err
		}
		v, err := decodeVec(blob)
		if err != nil {
			return nil, nil, fmt.Errorf("store: decode embedding for topic %d: %w", id, err)
		}
		ids = append(ids, id)
		vecs = append(vecs, v)
	}
	return ids, vecs, rows.Err()
}

// TopicMetaRow holds minimal metadata about a topic for display.
type TopicMetaRow struct {
	TopicID       int64
	StreamName    string
	TopicName     string
	Resolved      bool
	MessageCount  int
	LastMessageAt string
}

// DeleteEmbeddingsByModel removes all stored embeddings for the given model.
// Used by 'embeddings backfill --force' to trigger a full re-embed.
func (s *Store) DeleteEmbeddingsByModel(ctx context.Context, model string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM topic_embeddings WHERE model = ?`, model)
	return err
}

// EmbeddingEntry combines a stored vector with its topic metadata.
// Used by FilteredEmbeddings for stream/resolved-scoped semantic search.
type EmbeddingEntry struct {
	TopicID       int64
	StreamName    string
	TopicName     string
	Resolved      bool
	MessageCount  int
	LastMessageAt string
	Vec           []float32
}

// FilteredEmbeddings returns stored embeddings for model, filtered by optional
// stream name and resolved status. Filtering is applied at the SQL layer so
// that callers can limit results to a correctly-scoped set without a post-filter
// that would silently discard relevant in-stream topics below a global cutoff.
func (s *Store) FilteredEmbeddings(ctx context.Context, model, stream string, onlyUnresolved bool) ([]EmbeddingEntry, error) {
	q := `
SELECT te.topic_id, st.name, t.name, t.resolved, t.message_count,
       COALESCE(t.last_message_at, ''), te.vector
FROM topic_embeddings te
JOIN topics t  ON t.id  = te.topic_id
JOIN streams st ON st.id = t.stream_id
WHERE te.model = ?`
	args := []any{model}
	if stream != "" {
		q += " AND st.name = ?"
		args = append(args, stream)
	}
	if onlyUnresolved {
		q += " AND t.resolved = 0"
	}
	q += " ORDER BY te.topic_id ASC"

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmbeddingEntry
	for rows.Next() {
		var e EmbeddingEntry
		var blob []byte
		var resolved int
		if err := rows.Scan(
			&e.TopicID, &e.StreamName, &e.TopicName,
			&resolved, &e.MessageCount, &e.LastMessageAt, &blob,
		); err != nil {
			return nil, err
		}
		e.Resolved = resolved == 1
		v, err := decodeVec(blob)
		if err != nil {
			return nil, fmt.Errorf("store: decode embedding for topic %d: %w", e.TopicID, err)
		}
		e.Vec = v
		out = append(out, e)
	}
	return out, rows.Err()
}

// TopicMeta returns lightweight metadata for a single topic by ID.
func (s *Store) TopicMeta(ctx context.Context, topicID int64) (*TopicMetaRow, error) {
	var r TopicMetaRow
	var resolved int
	err := s.db.QueryRowContext(ctx, `
SELECT t.id, st.name, t.name, t.resolved, t.message_count, COALESCE(t.last_message_at,'')
FROM topics t
JOIN streams st ON st.id = t.stream_id
WHERE t.id = ?`, topicID).Scan(
		&r.TopicID, &r.StreamName, &r.TopicName, &resolved, &r.MessageCount, &r.LastMessageAt,
	)
	if err != nil {
		return nil, err
	}
	r.Resolved = resolved == 1
	return &r, nil
}

func encodeVec(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(x))
	}
	return b
}

// decodeVec deserialises a float32 vector from little-endian bytes.
func decodeVec(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("byte length %d not a multiple of 4", len(b))
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		bits := binary.LittleEndian.Uint32(b[i*4:])
		v[i] = math.Float32frombits(bits)
	}
	return v, nil
}
