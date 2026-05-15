package cli_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/cli"
	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/store"
)

// setupCLITest creates a config + seeded DB, returns a ready config path.
func setupCLITest(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	dbPath := filepath.Join(dir, "test.db")
	cfgPath := filepath.Join(dir, "config.toml")

	// Write a minimal config.
	cfgContent := "[zulip]\nurl = \"https://z.example.com\"\nemail = \"bot@example.com\"\napi_key = \"fakekey\"\n\n[database]\npath = \"" + dbPath + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o600); err != nil {
		t.Fatal(err)
	}

	// Seed the DB.
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctx := context.Background()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertOrganization(ctx, 1, "https://z.example.com", "TestOrg"); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertStream(ctx, store.Stream{ID: 1, OrgID: 1, Name: "general"}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertUser(ctx, store.User{ID: 10, OrgID: 1, FullName: "Alice"}); err != nil {
		t.Fatal(err)
	}
	topicID, err := st.GetOrCreateTopic(ctx, 1, 1, "deploys")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i, content := range []string{"deployed v1", "deployed v2", "deployed v3"} {
		msg := store.Message{
			ID:          int64(100 + i),
			OrgID:       1,
			StreamID:    1,
			TopicID:     topicID,
			SenderID:    10,
			Content:     content,
			ContentText: content,
			Timestamp:   now.Add(time.Duration(-3+i) * 24 * time.Hour).Format(time.RFC3339),
		}
		if i < 2 {
			msg.Mentions = []store.Mention{{UserID: 10, Name: "Alice", Kind: "user"}}
		}
		if i == 2 {
			msg.HasAttachment = true
			msg.Attachments = []store.Attachment{
				{URL: "/user_uploads/1/deploy-log.txt", FileName: "deploy-log.txt", Text: "deploy log", Indexed: true},
				{URL: "/user_uploads/1/screenshot.png", FileName: "screenshot.png"},
			}
		}
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage: %v", err)
		}
	}
	return cfgPath
}

func runMessages(t *testing.T, cfgPath string, args ...string) (string, error) {
	t.Helper()
	root := cli.NewRootCmd()

	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)

	// Prepend --config and the sub-command.
	fullArgs := append([]string{"--config", cfgPath, "messages"}, args...)
	root.SetArgs(fullArgs)

	err := root.ExecuteContext(context.Background())
	return buf.String(), err
}

// ---------------------------------------------------------------------------

func TestMessagesCmd_RequiresFilter(t *testing.T) {
	cfgPath := setupCLITest(t)
	_, err := runMessages(t, cfgPath) // no filters
	if err == nil {
		t.Fatal("expected error without filters, got nil")
	}
	if !strings.Contains(err.Error(), "narrowing filter") {
		t.Errorf("error should mention narrowing filter, got: %v", err)
	}
}

func TestMessagesCmd_ByStream(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--stream", "general")
	if err != nil {
		t.Fatalf("unexpected error: %v\nOutput: %s", err, out)
	}
	if !strings.Contains(out, "general") {
		t.Errorf("expected 'general' in output, got: %s", out)
	}
	if !strings.Contains(out, "deployed") {
		t.Errorf("expected 'deployed' in output, got: %s", out)
	}
	if !strings.Contains(out, "3 messages") {
		t.Errorf("expected '3 messages' footer, got: %s", out)
	}
}

func TestMessagesCmd_ByStreamAndTopic(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--stream", "general", "--topic", "deploys", "--days", "7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "deploys") {
		t.Errorf("expected topic 'deploys' in output, got: %s", out)
	}
}

func TestMessagesCmd_ByDays(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--days", "7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "deployed") {
		t.Errorf("expected messages in output, got: %s", out)
	}
}

func TestMessagesCmd_BySender(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--sender", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected sender Alice in output, got: %s", out)
	}
}

func TestMessagesCmd_InvalidSince(t *testing.T) {
	cfgPath := setupCLITest(t)
	_, err := runMessages(t, cfgPath, "--since", "not-a-date")
	if err == nil {
		t.Fatal("expected error for invalid --since")
	}
}

func TestMessagesCmd_ValidSinceRFC3339(t *testing.T) {
	cfgPath := setupCLITest(t)
	since := time.Now().UTC().Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	out, err := runMessages(t, cfgPath, "--since", since)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "deployed") {
		t.Errorf("expected messages in output, got: %s", out)
	}
}

func TestMessagesCmd_ValidSinceDate(t *testing.T) {
	cfgPath := setupCLITest(t)
	since := time.Now().UTC().Add(-8 * 24 * time.Hour).Format("2006-01-02")
	out, err := runMessages(t, cfgPath, "--since", since)
	if err != nil {
		t.Fatalf("unexpected error for YYYY-MM-DD --since: %v", err)
	}
	_ = out
}

func TestMessagesCmd_NoResults(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--stream", "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "No messages found") {
		t.Errorf("expected 'No messages found', got: %s", out)
	}
}

func TestMessagesCmd_Last(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--sender", "Alice", "--last", "2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "2 messages") {
		t.Errorf("expected '2 messages', got: %s", out)
	}
}

func TestMessagesCmd_All(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runMessages(t, cfgPath, "--stream", "general", "--all")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "3 messages") {
		t.Errorf("expected '3 messages', got: %s", out)
	}
}

// Verify config package is referenced (avoids unused import warning).
var _ = config.DefaultPath

func TestMessagesCommandNormalizesRFC3339Offsets(t *testing.T) {
	cfgPath := setupCLITest(t)

	out, err := runMessages(t, cfgPath, "--since", "2026-01-01T05:30:00-06:00", "--until", "2026-01-01T05:30:00-06:00")
	if err != nil {
		t.Fatalf("unexpected error for RFC3339 offset: %v", err)
	}
	if !strings.Contains(out, "No messages found") {
		t.Errorf("expected normalized offset query to run and find no messages, got: %s", out)
	}
}

func TestMessagesCommandRejectsNegativeCounts(t *testing.T) {
	cfgPath := setupCLITest(t)

	cases := [][]string{
		{"--days", "-1"},
		{"--hours", "-1"},
		{"--last", "-1"},
		{"--stream", "general", "--limit", "0"},
	}
	for _, tc := range cases {
		_, err := runMessages(t, cfgPath, tc...)
		if err == nil {
			t.Fatalf("expected error for args %v", tc)
		}
	}
}

func runDigest(t *testing.T, cfgPath string, args ...string) (string, error) {
	t.Helper()
	root := cli.NewRootCmd()

	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)

	fullArgs := append([]string{"--config", cfgPath, "digest"}, args...)
	root.SetArgs(fullArgs)

	err := root.ExecuteContext(context.Background())
	return buf.String(), err
}

func TestDigestCmd_TextOutput(t *testing.T) {
	cfgPath := setupCLITest(t)
	since := time.Now().UTC().Add(-8 * 24 * time.Hour).Format("2006-01-02")
	out, err := runDigest(t, cfgPath, "--stream", "general", "--since", since)
	if err != nil {
		t.Fatalf("unexpected error: %v\nOutput: %s", err, out)
	}
	for _, want := range []string{"#general > deploys", "3 messages", "participants: Alice", "latest: deployed v3", "Mention-heavy topics:", "2 mentions", "Attachment-heavy topics:", "2 attachments", "(1 topics)"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got: %s", want, out)
		}
	}
}

func TestDigestCmd_JSONOutput(t *testing.T) {
	cfgPath := setupCLITest(t)
	since := time.Now().UTC().Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	out, err := runDigest(t, cfgPath, "--stream", "general", "--since", since, "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{`"stream": "general"`, `"topics": [`, `"topic": "deploys"`, `"messages": 3`, `"mention_heavy_topics": [`, `"count": 2`, `"attachment_heavy_topics": [`} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %s in JSON digest, got: %s", want, out)
		}
	}
}

func TestDigestCmd_RequiresStreamAndSince(t *testing.T) {
	cfgPath := setupCLITest(t)
	if _, err := runDigest(t, cfgPath, "--since", "2026-01-01"); err == nil {
		t.Fatal("expected missing --stream error")
	}
	if _, err := runDigest(t, cfgPath, "--stream", "general"); err == nil {
		t.Fatal("expected missing --since error")
	}
}

func TestDigestCmd_UntilAndLimit(t *testing.T) {
	cfgPath := setupCLITest(t)
	out, err := runDigest(t, cfgPath,
		"--stream", "general",
		"--since", time.Now().UTC().Add(-8*24*time.Hour).Format("2006-01-02"),
		"--until", time.Now().UTC().Add(-2*24*time.Hour).Format(time.RFC3339),
		"--limit", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "2 messages") {
		t.Errorf("expected until filter to leave 2 messages, got: %s", out)
	}
}
