package syncer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/syncer"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

func TestPaginationStallRecordsFailure(t *testing.T) {
	for _, scope := range []string{"stream", "private"} {
		for _, kind := range []string{"repeat", "empty", "unordered"} {
			t.Run(scope+"/"+kind, func(t *testing.T) {
				calls := 0
				fz := &fakeZulip{streams: []map[string]any{{"stream_id": 1, "name": "general"}}, users: []map[string]any{}}
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/messages") {
						isStream := strings.Contains(r.URL.Query().Get("narrow"), "stream")
						if isStream != (scope == "stream") {
							writeJSON(w, map[string]any{"result": "success", "messages": []any{}, "found_newest": true})
							return
						}
						calls++
						msgs := []map[string]any{makeMsg(10, 42, "stall", "stream")}
						if kind == "empty" {
							msgs = nil
						}
						if kind == "unordered" {
							msgs = append(msgs, makeMsg(9, 42, "stall", "stream"))
						}
						writeJSON(w, map[string]any{"result": "success", "messages": msgs, "found_newest": false})
						return
					}
					fz.handler(w, r)
				}))
				defer srv.Close()
				st, _ := setupDB(t)
				defer st.Close()
				cfg := buildConfig(srv.URL, nil)
				if scope == "private" {
					cfg.Sync.ExcludeStreams = []string{"general"}
				}
				sy := syncer.NewWithLogger(cfg, zulip.NewClient(srv.URL, "test", "key"), st, nil)
				ctx := context.Background()
				err := sy.Sync(ctx, syncer.Options{})
				if err == nil || !strings.Contains(err.Error(), "pagination stalled") {
					t.Fatalf("expected pagination error, got %v", err)
				}
				if calls > 2 {
					t.Fatalf("did not stop promptly: %d calls", calls)
				}
				health, err := st.Health(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(health.Failures) != 1 || !strings.Contains(health.Failures[0].Error, "pagination stalled") {
					t.Fatalf("ledger: %+v", health.Failures)
				}
				id := int64(1)
				if scope == "private" {
					id = 0
				}
				last, err := st.LastMessageID(ctx, 1, id)
				if err != nil || last != 0 {
					t.Fatalf("failure advanced checkpoint: %d %v", last, err)
				}
				for _, h := range health.Streams {
					if h.StreamID == id && (h.LastAttempt == "" || h.LastSuccess != "" || h.LastError == "") {
						t.Fatalf("health: %+v", h)
					}
				}
			})
		}
	}
}

func TestRepairEmptyCompleteStream(t *testing.T) {
	ctx := context.Background()
	st, _ := setupDB(t)
	defer st.Close()
	if err := st.UpsertStream(ctx, store.Stream{ID: 1, OrgID: 1, Name: "general"}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateSyncState(ctx, 1, 1, 30); err != nil {
		t.Fatal(err)
	}
	if err := st.BeginSync(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveRepair(ctx, 1, 1, "oldest", true); err != nil {
		t.Fatal(err)
	}
	fz := &fakeZulip{streams: []map[string]any{{"stream_id": 1, "name": "general"}}, users: []map[string]any{}, messages: map[string][]map[string]any{narrowKey("stream", "general"): {makeMsg(10, 42, "recoveredold", "stream"), makeMsg(30, 42, "recoverednew", "stream")}}}
	srv := httptest.NewServer(http.HandlerFunc(fz.handler))
	defer srv.Close()
	sy := syncer.NewWithLogger(buildConfig(srv.URL, nil), zulip.NewClient(srv.URL, "test", "key"), st, nil)
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatal(err)
	}
	hits, err := st.Search(ctx, "recoveredold", "general", false, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("empty completed stream was not repaired: %v %v", hits, err)
	}
	report, err := st.Health(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range report.Streams {
		if h.StreamID == 1 && (h.MessageCount != 2 || h.CheckpointMissing) {
			t.Fatalf("coverage: %+v", h)
		}
	}
}

func TestRepairIsBoundedResumableAndFillsInternalHole(t *testing.T) {
	ctx := context.Background()
	fz := &fakeZulip{streams: []map[string]any{{"stream_id": 1, "name": "general"}}, users: []map[string]any{}}
	var anchors []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/messages") {
			fz.handler(w, r)
			return
		}
		if !strings.Contains(r.URL.Query().Get("narrow"), "stream") {
			writeJSON(w, map[string]any{"result": "success", "messages": []any{}, "found_newest": true})
			return
		}
		anchor := r.URL.Query().Get("anchor")
		anchors = append(anchors, anchor)
		a, _ := strconv.Atoi(anchor)
		id := 10
		for id < a && id < 70 {
			id += 10
		}
		writeJSON(w, map[string]any{"result": "success", "messages": []any{makeMsg(int64(id), 42, fmt.Sprintf("repairmessage%d", id), "stream")}, "found_newest": id == 70})
	}))
	defer srv.Close()
	st, _ := setupDB(t)
	defer st.Close()
	if err := st.UpsertStream(ctx, store.Stream{ID: 1, OrgID: 1, Name: "general"}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureMessageSender(ctx, store.User{ID: 42, OrgID: 1, FullName: "User 42"}); err != nil {
		t.Fatal(err)
	}
	tid, err := st.GetOrCreateTopic(ctx, 1, 1, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{10, 70} {
		if err := st.UpsertMessage(ctx, store.Message{ID: id, OrgID: 1, StreamID: 1, TopicID: tid, SenderID: 42, Content: "preserved", ContentText: "preserved", Timestamp: "2023-11-14T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpdateSyncState(ctx, 1, 1, 70); err != nil {
		t.Fatal(err)
	}
	sy := syncer.NewWithLogger(buildConfig(srv.URL, nil), zulip.NewClient(srv.URL, "test", "key"), st, nil)
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatal(err)
	}
	if len(anchors) != 6 || anchors[0] != "oldest" || anchors[5] != "70" {
		t.Fatalf("budget/incremental anchors: %v", anchors)
	}
	health, err := st.Health(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range health.Streams {
		if h.StreamID == 1 && (h.MessageCount != 6 || h.RepairAnchor != "51" || h.RepairCompletedAt != "") {
			t.Fatalf("partial repair: %+v", h)
		}
	}
	if err := sy.Sync(ctx, syncer.Options{}); err != nil {
		t.Fatal(err)
	}
	if anchors[6] != "51" {
		t.Fatalf("did not resume: %v", anchors)
	}
	health, err = st.Health(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range health.Streams {
		if h.StreamID == 1 && (h.MessageCount != 7 || h.RepairCompletedAt == "" || h.CheckpointID != 70 || h.LastSuccess == "") {
			t.Fatalf("completed repair: %+v", h)
		}
	}
	hits, err := st.Search(ctx, "repairmessage60", "general", false, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("repaired message not searchable: %v %v", hits, err)
	}
	hits, err = st.Search(ctx, "preserved", "general", false, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("repair replaced old snapshot: %v %v", hits, err)
	}
}
