package zulip

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDownloadStreamsBody(t *testing.T) {
	continueBody := make(chan struct{})
	handlerStarted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user_uploads/1/large.bin" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if user, pass, ok := r.BasicAuth(); !ok || user != "test@example.com" || pass != "testkey" {
			t.Fatalf("missing basic auth: %q %q %v", user, pass, ok)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("first"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		close(handlerStarted)
		<-continueBody
		_, _ = w.Write([]byte("second"))
	}))
	defer srv.Close()

	api := NewClient(srv.URL, "test@example.com", "testkey")
	type result struct {
		body        io.ReadCloser
		contentType string
		err         error
	}
	done := make(chan result, 1)
	go func() {
		body, contentType, err := api.Download(context.Background(), "/user_uploads/1/large.bin")
		done <- result{body: body, contentType: contentType, err: err}
	}()

	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start streaming response")
	}

	var res result
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Download waited for the full response body instead of returning a stream")
	}
	if res.err != nil {
		t.Fatalf("Download: %v", res.err)
	}
	defer res.body.Close()
	if res.contentType != "application/octet-stream" {
		t.Fatalf("contentType = %q", res.contentType)
	}

	close(continueBody)
	got, err := io.ReadAll(res.body)
	if err != nil {
		t.Fatalf("ReadAll streamed body: %v", err)
	}
	if string(got) != "firstsecond" {
		t.Fatalf("body = %q", got)
	}
}

func TestDownloadRejectsNonUploadPath(t *testing.T) {
	api := NewClient("https://zulip.example.com", "test@example.com", "testkey")
	body, _, err := api.Download(context.Background(), "/api/v1/messages")
	if err == nil {
		if body != nil {
			_ = body.Close()
		}
		t.Fatal("Download accepted non-upload path")
	}
}
