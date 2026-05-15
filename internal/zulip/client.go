package zulip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	email   string
	apiKey  string
	hc      *http.Client
}

func NewClient(baseURL, email, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		email:   email,
		apiKey:  apiKey,
		hc:      &http.Client{Timeout: 60 * time.Second},
	}
}

type apiResult struct {
	Result string `json:"result"`
	Msg    string `json:"msg"`
}

type ServerSettings struct {
	RealmName string `json:"realm_name"`
	RealmURI  string `json:"realm_uri"`
}

type Stream struct {
	StreamID    int64  `json:"stream_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	InviteOnly  bool   `json:"invite_only"`
	IsWebPublic bool   `json:"is_web_public"`
}

type User struct {
	UserID    int64  `json:"user_id"`
	Email     string `json:"email"`
	FullName  string `json:"full_name"`
	IsBot     bool   `json:"is_bot"`
	AvatarURL string `json:"avatar_url"`
}

type Message struct {
	ID             int64  `json:"id"`
	SenderID       int64  `json:"sender_id"`
	SenderFullName string `json:"sender_full_name"`
	StreamID       int64  `json:"stream_id"`
	Subject        string `json:"subject"`
	Topic          string `json:"topic"`
	Content        string `json:"content"`
	Timestamp      int64  `json:"timestamp"`
	EditTimestamp  int64  `json:"edit_timestamp"`
	Type           string `json:"type"`
	// IsMe is true for /me action messages (Zulip field: is_me_message).
	// This is distinct from Type=="private" which indicates a direct message.
	IsMe      bool            `json:"is_me_message"`
	Reactions json.RawMessage `json:"reactions"`
}

type messagesResp struct {
	apiResult
	Messages    []Message `json:"messages"`
	FoundOldest bool      `json:"found_oldest"`
	FoundNewest bool      `json:"found_newest"`
	Anchor      int64     `json:"anchor"`
}

type getStreamsResp struct {
	apiResult
	Streams []Stream `json:"streams"`
}

type getUsersResp struct {
	apiResult
	Members []User `json:"members"`
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, out any) error {
	body, _, err := c.doRaw(ctx, method, path, q)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func (c *Client) doRaw(ctx context.Context, method, path string, q url.Values) ([]byte, http.Header, error) {
	u := c.baseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u, nil)
		if err != nil {
			return nil, nil, err
		}
		req.SetBasicAuth(c.email, c.apiKey)

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := 2 * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if n, err := strconv.Atoi(ra); err == nil {
					wait = time.Duration(n) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, nil, fmt.Errorf("zulip api %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		if rem := resp.Header.Get("X-RateLimit-Remaining"); rem != "" {
			if n, err := strconv.Atoi(rem); err == nil && n < 5 {
				time.Sleep(1500 * time.Millisecond)
			}
		}
		return body, resp.Header.Clone(), nil
	}
	if lastErr != nil {
		return nil, nil, lastErr
	}
	return nil, nil, fmt.Errorf("zulip api %s failed after retries", path)
}

// Download fetches a Zulip-hosted upload using the same basic auth credentials
// as API calls. Only local /user_uploads/ paths are accepted. The response body
// is streamed to the caller; callers must close it.
func (c *Client) Download(ctx context.Context, path string) (io.ReadCloser, string, error) {
	resp, err := c.downloadResponse(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return resp.Body, resp.Header.Get("Content-Type"), nil
}

func (c *Client) downloadResponse(ctx context.Context, path string) (*http.Response, error) {
	if !strings.HasPrefix(path, "/user_uploads/") {
		return nil, fmt.Errorf("not a Zulip upload path: %s", path)
	}
	u := c.baseURL + path

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(c.email, c.apiKey)

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			wait := 2 * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if n, err := strconv.Atoi(ra); err == nil {
					wait = time.Duration(n) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			return nil, fmt.Errorf("zulip download %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		if rem := resp.Header.Get("X-RateLimit-Remaining"); rem != "" {
			if n, err := strconv.Atoi(rem); err == nil && n < 5 {
				time.Sleep(1500 * time.Millisecond)
			}
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("zulip download %s failed after retries", path)
}

func (c *Client) ServerSettings(ctx context.Context) (*ServerSettings, error) {
	var out ServerSettings
	if err := c.do(ctx, http.MethodGet, "/api/v1/server_settings", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Streams(ctx context.Context) ([]Stream, error) {
	q := url.Values{}
	q.Set("include_subscribed", "true")
	q.Set("include_all_active", "true")
	q.Set("include_public", "true")
	q.Set("include_web_public", "true")
	var out getStreamsResp
	if err := c.do(ctx, http.MethodGet, "/api/v1/streams", q, &out); err != nil {
		return nil, err
	}
	if out.Result != "success" {
		return nil, fmt.Errorf("streams error: %s", out.Msg)
	}
	return out.Streams, nil
}

func (c *Client) Users(ctx context.Context) ([]User, error) {
	q := url.Values{}
	q.Set("client_gravatar", "false")
	q.Set("include_custom_profile_fields", "false")
	var out getUsersResp
	if err := c.do(ctx, http.MethodGet, "/api/v1/users", q, &out); err != nil {
		return nil, err
	}
	if out.Result != "success" {
		return nil, fmt.Errorf("users error: %s", out.Msg)
	}
	return out.Members, nil
}

type MessagesRequest struct {
	Anchor        string
	NumBefore     int
	NumAfter      int
	Narrow        []map[string]any
	ApplyMarkdown bool
}

func (c *Client) Messages(ctx context.Context, req MessagesRequest) (*messagesResp, error) {
	q := url.Values{}
	q.Set("anchor", req.Anchor)
	q.Set("num_before", strconv.Itoa(req.NumBefore))
	q.Set("num_after", strconv.Itoa(req.NumAfter))
	if req.ApplyMarkdown {
		q.Set("apply_markdown", "true")
	} else {
		q.Set("apply_markdown", "false")
	}
	if len(req.Narrow) > 0 {
		b, _ := json.Marshal(req.Narrow)
		q.Set("narrow", string(b))
	}
	var out messagesResp
	if err := c.do(ctx, http.MethodGet, "/api/v1/messages", q, &out); err != nil {
		return nil, err
	}
	if out.Result != "success" {
		return nil, fmt.Errorf("messages error: %s", out.Msg)
	}
	return &out, nil
}
