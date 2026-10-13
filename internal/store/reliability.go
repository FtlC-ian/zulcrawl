package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const reliabilitySchema = `
CREATE TABLE IF NOT EXISTS sync_health (
 org_id INTEGER NOT NULL, stream_id INTEGER NOT NULL,
 last_attempt TEXT NOT NULL DEFAULT '', last_success TEXT NOT NULL DEFAULT '',
 last_error TEXT NOT NULL DEFAULT '', repair_anchor TEXT NOT NULL DEFAULT 'oldest',
 repair_completed_at TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(org_id, stream_id)
);
CREATE TABLE IF NOT EXISTS sync_failures (
 id INTEGER PRIMARY KEY AUTOINCREMENT, org_id INTEGER NOT NULL, stream_id INTEGER NOT NULL,
 occurred_at TEXT NOT NULL, error TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_org_stream_id ON messages(org_id, stream_id, id);
INSERT OR IGNORE INTO sync_health(org_id,stream_id,last_success)
 SELECT org_id,stream_id,COALESCE(synced_at,'') FROM sync_state;
`

func (s *Store) initReliabilitySchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, reliabilitySchema); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) BeginSync(ctx context.Context, org, stream int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO sync_health(org_id,stream_id,last_attempt) VALUES(?,?,?)
 ON CONFLICT(org_id,stream_id) DO UPDATE SET last_attempt=excluded.last_attempt`, org, stream, now())
	return err
}

func (s *Store) FinishSync(ctx context.Context, org, stream int64, syncErr error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if syncErr == nil {
		_, err = tx.ExecContext(ctx, `UPDATE sync_health SET last_success=?,last_error='' WHERE org_id=? AND stream_id=?`, now(), org, stream)
	} else {
		// API errors may contain server-supplied text; do not persist credentials or message content.
		diagnostic := "sync failed (API or storage); retry sync with progress output for details"
		switch {
		case errors.Is(syncErr, context.Canceled):
			diagnostic = "sync canceled"
		case errors.Is(syncErr, context.DeadlineExceeded):
			diagnostic = "sync deadline exceeded"
		case strings.Contains(syncErr.Error(), "pagination stalled:"):
			diagnostic = "pagination stalled: empty, repeated or non-advancing message page"
		}
		_, err = tx.ExecContext(ctx, `UPDATE sync_health SET last_error=? WHERE org_id=? AND stream_id=?`, diagnostic, org, stream)
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO sync_failures(org_id,stream_id,occurred_at,error) VALUES(?,?,?,?)`, org, stream, now(), diagnostic)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM sync_failures WHERE id NOT IN (SELECT id FROM sync_failures ORDER BY id DESC LIMIT 200)`)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RepairAnchor(ctx context.Context, org, stream int64) (string, error) {
	var anchor, completed string
	err := s.db.QueryRowContext(ctx, `SELECT repair_anchor,repair_completed_at FROM sync_health WHERE org_id=? AND stream_id=?`, org, stream).Scan(&anchor, &completed)
	if err != nil {
		return "", err
	}
	if completed != "" {
		var missing bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_state ss WHERE ss.org_id=? AND ss.stream_id=? AND ss.last_message_id>0 AND NOT EXISTS(SELECT 1 FROM messages m WHERE m.id=ss.last_message_id AND m.org_id=ss.org_id AND m.stream_id=ss.stream_id))`, org, stream).Scan(&missing); err != nil {
			return "", err
		}
		if missing {
			return "oldest", nil
		}
		t, err := time.Parse(time.RFC3339, completed)
		if err != nil {
			return "", fmt.Errorf("invalid repair timestamp: %w", err)
		}
		if time.Since(t) < 24*time.Hour {
			return "", nil
		}
		return "oldest", nil
	}
	return anchor, nil
}

func (s *Store) SaveRepair(ctx context.Context, org, stream int64, anchor string, complete bool) error {
	completed := ""
	if complete {
		completed = now()
		anchor = "oldest"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE sync_health SET repair_anchor=?,repair_completed_at=? WHERE org_id=? AND stream_id=?`, anchor, completed, org, stream)
	return err
}

type StreamHealth struct {
	StreamID          int64  `json:"stream_id"`
	Name              string `json:"name"`
	MessageCount      int64  `json:"message_count"`
	FirstMessageID    int64  `json:"first_message_id"`
	LastMessageID     int64  `json:"last_message_id"`
	CheckpointID      int64  `json:"checkpoint_id"`
	NewestMessageAt   string `json:"newest_message_at"`
	LastAttempt       string `json:"last_attempt"`
	LastSuccess       string `json:"last_success"`
	LastError         string `json:"last_error"`
	RepairAnchor      string `json:"repair_anchor"`
	RepairCompletedAt string `json:"repair_completed_at"`
	Stale             bool   `json:"stale"`
	Quiet             bool   `json:"quiet"`
	CheckpointMissing bool   `json:"checkpoint_missing"`
	Excluded          bool   `json:"excluded"`
}

type SyncFailure struct {
	StreamID   int64  `json:"stream_id"`
	OccurredAt string `json:"occurred_at"`
	Error      string `json:"error"`
}

type HealthReport struct {
	SchemaVersion int            `json:"schema_version"`
	Streams       []StreamHealth `json:"streams"`
	Failures      []SyncFailure  `json:"failures"`
}

func (s *Store) Health(ctx context.Context) (HealthReport, error) {
	report := HealthReport{SchemaVersion: 1, Streams: []StreamHealth{}, Failures: []SyncFailure{}}
	var migrated bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_health')`).Scan(&migrated); err != nil {
		return report, err
	}
	healthTable := "sync_health"
	if !migrated {
		healthTable = "(SELECT org_id,stream_id,'' AS last_attempt,COALESCE(synced_at,'') AS last_success,'' AS last_error,'oldest' AS repair_anchor,'' AS repair_completed_at FROM sync_state)"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT st.id,st.name,COUNT(m.id),COALESCE(MIN(m.id),0),COALESCE(MAX(m.id),0),
 COALESCE(ss.last_message_id,0),COALESCE(MAX(m.timestamp),''),COALESCE(h.last_attempt,''),
 COALESCE(h.last_success,''),COALESCE(h.last_error,''),COALESCE(h.repair_anchor,'oldest'),COALESCE(h.repair_completed_at,''),
 CASE WHEN COALESCE(ss.last_message_id,0)>0 AND NOT EXISTS(SELECT 1 FROM messages cp WHERE cp.id=ss.last_message_id AND cp.stream_id=st.id AND cp.org_id=st.org_id) THEN 1 ELSE 0 END
 FROM streams st LEFT JOIN messages m ON m.stream_id=st.id AND m.org_id=st.org_id
 LEFT JOIN sync_state ss ON ss.stream_id=st.id AND ss.org_id=st.org_id
 LEFT JOIN `+healthTable+` h ON h.stream_id=st.id AND h.org_id=st.org_id GROUP BY st.id ORDER BY st.id`)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var h StreamHealth
		if err = rows.Scan(&h.StreamID, &h.Name, &h.MessageCount, &h.FirstMessageID, &h.LastMessageID, &h.CheckpointID, &h.NewestMessageAt, &h.LastAttempt, &h.LastSuccess, &h.LastError, &h.RepairAnchor, &h.RepairCompletedAt, &h.CheckpointMissing); err != nil {
			rows.Close()
			return report, err
		}
		success, e := time.Parse(time.RFC3339, h.LastSuccess)
		h.Stale = e != nil || time.Since(success) > 2*time.Hour || h.LastError != "" || h.CheckpointMissing
		newest, e := time.Parse(time.RFC3339, h.NewestMessageAt)
		h.Quiet = e != nil || success.Sub(newest) > 24*time.Hour
		h.Stale = h.Stale || h.Quiet
		report.Streams = append(report.Streams, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	if !migrated {
		return report, nil
	}
	rows, err = s.db.QueryContext(ctx, `SELECT stream_id,occurred_at,error FROM sync_failures ORDER BY id DESC LIMIT 200`)
	if err != nil {
		return report, err
	}
	defer rows.Close()
	for rows.Next() {
		var f SyncFailure
		if err = rows.Scan(&f.StreamID, &f.OccurredAt, &f.Error); err != nil {
			return report, err
		}
		report.Failures = append(report.Failures, f)
	}
	return report, rows.Err()
}
