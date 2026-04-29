package syncer

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/store"
)

const defaultBackfillBatch = 200

// BackfillOptions controls the backfill-indexes command.
type BackfillOptions struct {
	// BatchSize is the number of messages processed per transaction.
	// Defaults to 200.
	BatchSize int
}

// BackfillIndexer rebuilds message_mentions, message_attachments, and the
// attachment_text column in messages_fts from the stored rendered HTML in
// messages.content. It processes messages in ascending ID order in batches so
// large archives neither appear hung nor hold one giant transaction.
//
// The function is idempotent: it deletes and reinserts derived rows on each
// pass, so running twice leaves the same result.
func BackfillIndexes(ctx context.Context, st *store.Store, opts BackfillOptions, w io.Writer) error {
	if w == nil {
		w = os.Stderr
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBackfillBatch
	}

	total, err := st.CountMessages(ctx)
	if err != nil {
		return fmt.Errorf("backfill: count messages: %w", err)
	}
	if total == 0 {
		_, _ = fmt.Fprintln(w, "backfill-indexes: no messages in archive, nothing to do")
		return nil
	}

	_, _ = fmt.Fprintf(w, "backfill-indexes: %d messages to process (batch=%d)\n", total, batchSize)
	start := time.Now()

	indexer := func(r store.BackfillRow) ([]store.Mention, []store.Attachment) {
		return parseMessageIndexables(r.ID, r.OrgID, r.StreamID, r.TopicID, r.Timestamp, r.Content)
	}

	var processed int64
	var afterID int64
	for {
		batch, err := st.MessageRowsForBackfill(ctx, afterID, batchSize)
		if err != nil {
			return fmt.Errorf("backfill: fetch batch after id=%d: %w", afterID, err)
		}
		if len(batch) == 0 {
			break
		}

		if err := st.UpsertDerivedIndexesWithFTS(ctx, batch, indexer); err != nil {
			return fmt.Errorf("backfill: upsert derived indexes (batch starting id=%d): %w", batch[0].ID, err)
		}

		processed += int64(len(batch))
		afterID = batch[len(batch)-1].ID
		_, _ = fmt.Fprintf(w, "backfill-indexes: %d/%d messages done (last id=%d, elapsed=%s)\n",
			processed, total, afterID, time.Since(start).Round(time.Millisecond))
	}

	_, _ = fmt.Fprintf(w, "backfill-indexes: complete – %d messages processed in %s\n",
		processed, time.Since(start).Round(time.Millisecond))
	return nil
}
