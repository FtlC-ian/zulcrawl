package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestReliabilityMigrationPreservesExistingSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// The legacy checkpoint shape is deliberately independent of InitSchema.
	_, err = st.db.Exec(`CREATE TABLE sync_state(org_id INTEGER NOT NULL,stream_id INTEGER NOT NULL,last_message_id INTEGER,last_event_id INTEGER,synced_at TEXT,PRIMARY KEY(org_id,stream_id));
 INSERT INTO sync_state VALUES(1,7,99,123,'2026-05-18T00:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := st.InitSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var id, event int64
	var success string
	if err := st.db.QueryRow(`SELECT last_message_id,last_event_id FROM sync_state WHERE stream_id=7`).Scan(&id, &event); err != nil {
		t.Fatal(err)
	}
	if id != 99 || event != 123 {
		t.Fatalf("checkpoint changed: %d %d", id, event)
	}
	if err := st.db.QueryRow(`SELECT last_success FROM sync_health WHERE stream_id=7`).Scan(&success); err != nil {
		t.Fatal(err)
	}
	if success != "2026-05-18T00:00:00Z" {
		t.Fatalf("legacy freshness not migrated: %q", success)
	}
	if err := st.UpdateSyncState(ctx, 1, 7, 10); err != nil {
		t.Fatal(err)
	}
	id, err = st.LastMessageID(ctx, 1, 7)
	if err != nil || id != 99 {
		t.Fatalf("checkpoint regressed: %d %v", id, err)
	}
	if err := st.BeginSync(ctx, 1, 7); err != nil {
		t.Fatal(err)
	}
	for range 205 {
		if err := st.FinishSync(ctx, 1, 7, fmt.Errorf("server returned private content secret-key")); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	var diagnostic string
	if err := st.db.QueryRow(`SELECT COUNT(*),MAX(error) FROM sync_failures`).Scan(&count, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if count != 200 || diagnostic != "sync failed (API or storage); retry sync with progress output for details" {
		t.Fatalf("ledger retention/privacy: %d %q", count, diagnostic)
	}
	if err := st.FinishSync(ctx, 1, 7, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT last_success,last_error FROM sync_health WHERE stream_id=7`).Scan(&success, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if success == "" || diagnostic != "" {
		t.Fatalf("success freshness: %q %q", success, diagnostic)
	}
}
