package media

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

func TestFetchPendingDefaultsToAllPendingAttachments(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if err := st.UpsertOrganization(ctx, 1, "https://example.com", "Example"); err != nil {
		t.Fatalf("UpsertOrganization: %v", err)
	}
	if err := st.UpsertStream(ctx, store.Stream{ID: 10, OrgID: 1, Name: "general"}); err != nil {
		t.Fatalf("UpsertStream: %v", err)
	}
	if err := st.UpsertUser(ctx, store.User{ID: 20, OrgID: 1, FullName: "Sender"}); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	topicID, err := st.GetOrCreateTopic(ctx, 1, 10, "triage")
	if err != nil {
		t.Fatalf("GetOrCreateTopic: %v", err)
	}
	for i := int64(1); i <= 105; i++ {
		path := fmt.Sprintf("/user_uploads/a/file%03d.txt", i)
		msg := store.Message{
			ID:            1000 + i,
			OrgID:         1,
			StreamID:      10,
			TopicID:       topicID,
			SenderID:      20,
			Content:       "file",
			ContentText:   "file",
			Timestamp:     "2026-01-02T03:04:05Z",
			HasAttachment: true,
			Attachments: []store.Attachment{{
				URL:      path,
				FileName: filepath.Base(path),
			}},
		}
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage %d: %v", i, err)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Path) < len("/user_uploads/") || r.URL.Path[:len("/user_uploads/")] != "/user_uploads/" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "body for %s", r.URL.Path)
	}))
	defer srv.Close()

	api := zulip.NewClient(srv.URL, "test@example.com", "testkey")
	res, err := FetchPending(ctx, st, api, FetchOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if res.Fetched != 105 || res.Skipped != 0 || res.Failed != 0 {
		t.Fatalf("FetchPending result = %+v, want 105 fetched", res)
	}
	rows, err := st.ListAttachments(ctx, store.AttachmentFilter{Status: StatusFetched})
	if err != nil {
		t.Fatalf("ListAttachments fetched: %v", err)
	}
	if len(rows) != 105 {
		t.Fatalf("fetched rows = %d, want 105", len(rows))
	}
}
