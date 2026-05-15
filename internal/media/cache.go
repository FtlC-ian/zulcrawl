package media

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/store"
	"github.com/FtlC-ian/zulcrawl/internal/zulip"
)

const StatusFetched = "fetched"
const StatusError = "error"

// FetchOptions controls attachment media downloads.
type FetchOptions struct {
	CacheDir string
	Limit    int
	Force    bool
}

type FetchResult struct {
	Fetched int
	Skipped int
	Failed  int
}

// FetchPending downloads locally indexed Zulip /user_uploads/ attachments into
// the configured cache directory. It only reads private media into local files;
// nothing is published or uploaded elsewhere.
func FetchPending(ctx context.Context, st *store.Store, api *zulip.Client, opts FetchOptions) (FetchResult, error) {
	var res FetchResult
	if strings.TrimSpace(opts.CacheDir) == "" {
		return res, fmt.Errorf("cache dir is required")
	}
	if err := os.MkdirAll(opts.CacheDir, 0o700); err != nil {
		return res, err
	}

	rows, err := st.ListAttachments(ctx, store.AttachmentFilter{Status: fetchStatus(opts.Force), Limit: opts.Limit})
	if err != nil {
		return res, err
	}
	for _, a := range rows {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		if !strings.HasPrefix(a.URL, "/user_uploads/") {
			res.Skipped++
			continue
		}
		rel, err := safeRelativePath(a.URL, a.FileName)
		if err != nil {
			res.Failed++
			_ = st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{Status: StatusError, Error: err.Error(), FetchedAt: time.Now().UTC().Format(time.RFC3339)})
			continue
		}
		dest := filepath.Join(opts.CacheDir, rel)
		if !strings.HasPrefix(dest, filepath.Clean(opts.CacheDir)+string(os.PathSeparator)) && filepath.Clean(dest) != filepath.Clean(opts.CacheDir) {
			res.Failed++
			_ = st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{Status: StatusError, Error: "unsafe cache path", FetchedAt: time.Now().UTC().Format(time.RFC3339)})
			continue
		}
		if !opts.Force {
			if _, err := os.Stat(dest); err == nil && a.MediaStatus == StatusFetched {
				res.Skipped++
				continue
			}
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return res, err
		}
		tmp := dest + ".tmp"
		body, contentType, err := api.Download(ctx, a.URL)
		if err != nil {
			res.Failed++
			_ = st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{Status: StatusError, Path: dest, Error: err.Error(), FetchedAt: time.Now().UTC().Format(time.RFC3339)})
			continue
		}
		bytes, copyErr := writeBody(tmp, body)
		closeErr := body.Close()
		if copyErr != nil {
			_ = os.Remove(tmp)
			res.Failed++
			_ = st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{Status: StatusError, Path: dest, Error: copyErr.Error(), FetchedAt: time.Now().UTC().Format(time.RFC3339)})
			continue
		}
		if closeErr != nil {
			_ = os.Remove(tmp)
			res.Failed++
			_ = st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{Status: StatusError, Path: dest, Error: closeErr.Error(), FetchedAt: time.Now().UTC().Format(time.RFC3339)})
			continue
		}
		if err := os.Rename(tmp, dest); err != nil {
			_ = os.Remove(tmp)
			return res, err
		}
		if contentType == "" {
			contentType = a.ContentType
		}
		if err := st.UpdateAttachmentMedia(ctx, a.ID, store.AttachmentMediaUpdate{
			Path:        dest,
			Status:      StatusFetched,
			FetchedAt:   time.Now().UTC().Format(time.RFC3339),
			ContentType: contentType,
			Bytes:       bytes,
			Error:       "",
		}); err != nil {
			return res, err
		}
		res.Fetched++
	}
	return res, nil
}

func fetchStatus(force bool) string {
	if force {
		return ""
	}
	return "pending"
}

func writeBody(path string, body io.Reader) (int64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	n, copyErr := io.Copy(f, body)
	closeErr := f.Close()
	if copyErr != nil {
		return n, copyErr
	}
	return n, closeErr
}

func safeRelativePath(rawURL, fallback string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	p := strings.TrimPrefix(u.EscapedPath(), "/user_uploads/")
	if p == "" || strings.Contains(p, "..") {
		p = fallback
	}
	p, err = url.PathUnescape(p)
	if err != nil {
		return "", err
	}
	p = filepath.Clean(filepath.FromSlash(p))
	if p == "." || p == string(os.PathSeparator) || strings.HasPrefix(p, "..") || filepath.IsAbs(p) {
		return "", fmt.Errorf("unsafe attachment path %q", rawURL)
	}
	return p, nil
}
