package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestHealthSyncFreshness(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		age                           time.Duration
		failed, empty, missing, never bool
		stale                         bool
	}{
		{name: "quiet healthy", age: time.Minute},
		{name: "stale by age", age: 3 * time.Hour, stale: true},
		{name: "failed attempt", age: time.Minute, failed: true, stale: true},
		{name: "empty healthy", age: time.Minute, empty: true},
		{name: "missing checkpoint", age: time.Minute, missing: true, stale: true},
		{name: "never synced", never: true, empty: true, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(filepath.Join(t.TempDir(), "health.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if err := st.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertOrganization(ctx, 1, "https://example.com", "test"); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertUser(ctx, User{ID: 1, OrgID: 1}); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertStream(ctx, Stream{ID: 7, OrgID: 1, Name: tc.name}); err != nil {
				t.Fatal(err)
			}
			checkpoint := int64(0)
			if !tc.empty {
				topic, err := st.GetOrCreateTopic(ctx, 1, 7, "topic")
				if err != nil {
					t.Fatal(err)
				}
				if err := st.UpsertMessage(ctx, Message{ID: 10, OrgID: 1, StreamID: 7, TopicID: topic, SenderID: 1, Timestamp: time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
					t.Fatal(err)
				}
				checkpoint = 10
			}
			if tc.missing {
				checkpoint = 99
			}
			if !tc.never {
				if err := st.UpdateSyncState(ctx, 1, 7, checkpoint); err != nil {
					t.Fatal(err)
				}
				lastError := ""
				if tc.failed {
					lastError = "sync failed"
				}
				if _, err := st.db.Exec("INSERT INTO sync_health(org_id,stream_id,last_attempt,last_success,last_error) VALUES(1,7,?,?,?)", time.Now().UTC().Format(time.RFC3339), time.Now().Add(-tc.age).UTC().Format(time.RFC3339), lastError); err != nil {
					t.Fatal(err)
				}
			}
			report, err := st.Health(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Streams) != 1 {
				t.Fatalf("health: %+v", report)
			}
			h := report.Streams[0]
			if h.Stale != tc.stale || !h.Quiet || h.CheckpointMissing != tc.missing {
				t.Fatalf("health: %+v", h)
			}
			if tc.empty && h.MessageCount != 0 {
				t.Fatalf("empty stream: %+v", h)
			}
			if tc.name == "stale by age" {
				report, err = st.HealthWithMaxAge(ctx, 4*time.Hour)
				if err != nil || report.Streams[0].Stale {
					t.Fatalf("custom threshold: %+v %v", report, err)
				}
			}
		})
	}
}
