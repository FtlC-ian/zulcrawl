package syncer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/syncer"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

// ---------------------------------------------------------------------------
// Minimal fake Zulip server
// ---------------------------------------------------------------------------

type fakeZulip struct {
	streams  []map[string]any
	users    []map[string]any
	messages map[string][]map[string]any // narrow-key → messages
}

func (fz *fakeZulip) handler(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/server_settings"):
		writeJSON(w, map[string]any{"result": "success", "realm_name": "test", "realm_uri": "https://example.com"})

	case strings.HasSuffix(r.URL.Path, "/streams"):
		writeJSON(w, map[string]any{"result": "success", "streams": fz.streams})

	case strings.HasSuffix(r.URL.Path, "/users"):
		writeJSON(w, map[string]any{"result": "success", "members": fz.users})

	case strings.HasSuffix(r.URL.Path, "/messages"):
		narrowRaw := r.URL.Query().Get("narrow")
		key := "all"
		if narrowRaw != "" {
			key = narrowRaw
		}
		msgs := fz.messages[key]
		if msgs == nil {
			msgs = []map[string]any{}
		}
		writeJSON(w, map[string]any{
			"result":       "success",
			"messages":     msgs,
			"found_newest": true,
			"found_oldest": true,
			"anchor":       0,
		})

	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeMsg(id int64, senderID int64, content, typ string) map[string]any {
	return map[string]any{
		"id":               id,
		"sender_id":        senderID,
		"sender_full_name": fmt.Sprintf("User %d", senderID),
		"content":          content,
		"timestamp":        int64(1700000000 + id),
		"edit_timestamp":   int64(0),
		"type":             typ,
		"subject":          "test-topic",
		"topic":            "test-topic",
		"is_me_message":    false,
		"reactions":        json.RawMessage("[]"),
	}
}

func narrowKey(op, operand string) string {
	narrow := []map[string]any{{"operator": op, "operand": operand}}
	b, _ := json.Marshal(narrow)
	return string(b)
}

func setupDB(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	ctx := context.Background()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if err := st.UpsertOrganization(ctx, 1, "https://example.com", "test"); err != nil {
		t.Fatalf("UpsertOrganization: %v", err)
	}
	return st, dbPath
}

func buildConfig(serverURL string, streams []string) *config.Config {
	cfg := &config.Config{}
	cfg.Zulip.URL = serverURL
	cfg.Zulip.Email = "test@example.com"
	cfg.Zulip.APIKey = "testkey"
	cfg.Sync.Streams = streams
	cfg.Normalize()
	return cfg
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestSyncDMOnlyConfiguration verifies that when all streams are excluded
// (empty stream list + exclude-all config) the syncer still successfully
// reaches and stores private/direct messages.
func TestSyncDMOnlyConfiguration(t *testing.T) {
	dmMsg := makeMsg(1001, 42, "hello dm world", "private")

	fz := &fakeZulip{
		streams: []map[string]any{
			{"stream_id": 1, "name": "general", "description": "", "invite_only": false, "is_web_public": false},
		},
		users: []map[string]any{
			{"user_id": 42, "email": "alice@example.com", "full_name": "Alice", "is_bot": false, "avatar_url": ""},
		},
		messages: map[string][]map[string]any{},
	}
	// DM narrow key
	fz.messages[narrowKey("is", "private")] = []map[string]any{dmMsg}

	srv := httptest.NewServer(http.HandlerFunc(fz.handler))
	defer srv.Close()

	// Exclude all streams so only DMs are synced.
	cfg := buildConfig(srv.URL, []string{})
	cfg.Sync.ExcludeStreams = []string{"general"}

	st, _ := setupDB(t)
	defer st.Close()

	var logBuf bytes.Buffer
	api := zulip.NewClient(srv.URL, "test@example.com", "testkey")
	sy := syncer.NewWithLogger(cfg, api, st, &logBuf)

	ctx := context.Background()
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatalf("Sync (DM-only config) failed: %v", err)
	}

	// Verify the DM message was stored.
	hits, err := st.Search(ctx, "hello", "", false, 10)
	if err != nil {
		t.Fatalf("Search after DM sync: %v", err)
	}
	if len(hits) == 0 {
		t.Error("expected DM message to be findable in FTS after DM-only sync, got 0 hits")
	}

	// Verify progress was logged.
	logOut := logBuf.String()
	if !strings.Contains(logOut, "private") {
		t.Errorf("expected progress log to mention private messages, got: %q", logOut)
	}
}

// TestSyncFullyFilteredStreams verifies that when the configured stream list
// contains only non-existent streams (so zero streams match), private messages
// are still synced without error.
func TestSyncFullyFilteredStreams(t *testing.T) {
	dmMsg := makeMsg(2001, 99, "DM in filtered org", "private")

	fz := &fakeZulip{
		streams: []map[string]any{
			{"stream_id": 10, "name": "announcements", "description": "", "invite_only": false, "is_web_public": false},
		},
		users: []map[string]any{
			{"user_id": 99, "email": "bob@example.com", "full_name": "Bob", "is_bot": false, "avatar_url": ""},
		},
		messages: map[string][]map[string]any{},
	}
	fz.messages[narrowKey("is", "private")] = []map[string]any{dmMsg}

	srv := httptest.NewServer(http.HandlerFunc(fz.handler))
	defer srv.Close()

	// Only include a stream that does not exist → zero streams selected.
	cfg := buildConfig(srv.URL, []string{"does-not-exist"})

	st, _ := setupDB(t)
	defer st.Close()

	var logBuf bytes.Buffer
	api := zulip.NewClient(srv.URL, "test@example.com", "testkey")
	sy := syncer.NewWithLogger(cfg, api, st, &logBuf)

	ctx := context.Background()
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatalf("Sync (fully-filtered streams) failed: %v", err)
	}

	hits, err := st.Search(ctx, "filtered", "", false, 10)
	if err != nil {
		t.Fatalf("Search after filtered sync: %v", err)
	}
	if len(hits) == 0 {
		t.Error("expected DM message to be found after fully-filtered stream sync, got 0 hits")
	}
}

// TestSyncProgressLogging verifies that multi-page syncs emit per-page log
// lines so a long sync does not look hung.
func TestSyncProgressLogging(t *testing.T) {
	// Build 3 pages worth of messages (the fake server returns them all in
	// one shot, but the syncer's page count should be logged).
	msgs := make([]map[string]any, 3)
	for i := range msgs {
		msgs[i] = makeMsg(int64(3000+i), 11, fmt.Sprintf("stream msg %d", i), "stream")
	}

	fz := &fakeZulip{
		streams: []map[string]any{
			{"stream_id": 5, "name": "team", "description": "", "invite_only": false, "is_web_public": false},
		},
		users: []map[string]any{
			{"user_id": 11, "email": "carol@example.com", "full_name": "Carol", "is_bot": false, "avatar_url": ""},
		},
		messages: map[string][]map[string]any{},
	}
	fz.messages[narrowKey("stream", "team")] = msgs
	fz.messages[narrowKey("is", "private")] = []map[string]any{}

	srv := httptest.NewServer(http.HandlerFunc(fz.handler))
	defer srv.Close()

	cfg := buildConfig(srv.URL, []string{"team"})

	st, _ := setupDB(t)
	defer st.Close()

	var logBuf bytes.Buffer
	api := zulip.NewClient(srv.URL, "test@example.com", "testkey")
	sy := syncer.NewWithLogger(cfg, api, st, &logBuf)

	ctx := context.Background()
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatalf("Sync (progress logging) failed: %v", err)
	}

	log := logBuf.String()
	if !strings.Contains(log, "team") {
		t.Errorf("expected stream name in progress log, got: %q", log)
	}
	if !strings.Contains(log, "page") {
		t.Errorf("expected page progress in log, got: %q", log)
	}
	if !strings.Contains(log, "done") {
		t.Errorf("expected completion log, got: %q", log)
	}
}

// TestSyncIncrementalAnchorAdvances verifies that a second sync run starts
// from the last seen message ID, not from "oldest".
func TestSyncIncrementalAnchorAdvances(t *testing.T) {
	var requestedAnchors []string

	firstMsg := makeMsg(5000, 20, "first batch", "stream")
	secondMsg := makeMsg(5001, 20, "second batch", "stream")

	callCount := 0
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/server_settings"):
			writeJSON(w, map[string]any{"result": "success", "realm_name": "t", "realm_uri": "https://x.com"})
		case strings.HasSuffix(r.URL.Path, "/streams"):
			writeJSON(w, map[string]any{"result": "success", "streams": []map[string]any{
				{"stream_id": 7, "name": "alpha", "description": "", "invite_only": false, "is_web_public": false},
			}})
		case strings.HasSuffix(r.URL.Path, "/users"):
			writeJSON(w, map[string]any{"result": "success", "members": []map[string]any{
				{"user_id": 20, "email": "dave@x.com", "full_name": "Dave", "is_bot": false, "avatar_url": ""},
			}})
		case strings.HasSuffix(r.URL.Path, "/messages"):
			anchor := r.URL.Query().Get("anchor")
			narrowRaw := r.URL.Query().Get("narrow")
			if strings.Contains(narrowRaw, "stream") {
				requestedAnchors = append(requestedAnchors, anchor)
				callCount++
				msgs := []map[string]any{}
				if callCount == 1 {
					msgs = []map[string]any{firstMsg}
				} else {
					msgs = []map[string]any{secondMsg}
				}
				writeJSON(w, map[string]any{"result": "success", "messages": msgs, "found_newest": true, "found_oldest": true})
			} else {
				writeJSON(w, map[string]any{"result": "success", "messages": []map[string]any{}, "found_newest": true, "found_oldest": true})
			}
		default:
			http.NotFound(w, r)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	cfg := buildConfig(srv.URL, []string{"alpha"})
	st, _ := setupDB(t)
	defer st.Close()

	api := zulip.NewClient(srv.URL, "test@example.com", "testkey")
	sy := syncer.NewWithLogger(cfg, api, st, os.Stderr)
	ctx := context.Background()

	// First sync: should start from "oldest"
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Second sync: should start from last_message_id, not "oldest"
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	if len(requestedAnchors) < 2 {
		t.Fatalf("expected at least 2 message requests, got %d", len(requestedAnchors))
	}
	if requestedAnchors[0] != "oldest" {
		t.Errorf("first sync anchor: want 'oldest', got %q", requestedAnchors[0])
	}
	if requestedAnchors[1] == "oldest" {
		t.Error("second sync anchor should not be 'oldest' (incremental should advance past first batch)")
	}
}
