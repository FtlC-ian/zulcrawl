package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/FtlC-ian/zulcrawl/internal/cli"
	"github.com/FtlC-ian/zulcrawl/internal/config"
	"github.com/FtlC-ian/zulcrawl/internal/store"
)

func TestHealthyQuietStreamDiagnostics(t *testing.T) {
	path := setupCLITest(t)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	topic, err := st.GetOrCreateTopic(ctx, 1, 1, "deploys")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMessage(ctx, store.Message{ID: 102, OrgID: 1, StreamID: 1, TopicID: topic, SenderID: 10, ContentText: "deployed v3", Timestamp: time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertStream(ctx, store.Stream{ID: 2, OrgID: 1, Name: "empty"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2} {
		if err := st.BeginSync(ctx, 1, id); err != nil {
			t.Fatal(err)
		}
		if err := st.FinishSync(ctx, 1, id, nil); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	for _, args := range [][]string{{"status"}, {"search", "unmatchablezzzz", "--stream", "general"}, {"search", "unmatchablezzzz", "--stream", "empty"}, {"search", "unmatchablezzzz"}} {
		cmd := cli.NewRootCmd()
		var out, stderr bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&stderr)
		cmd.SetArgs(append([]string{"--config", path}, args...))
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if args[0] == "status" {
			for _, name := range []string{"general", "empty"} {
				if !strings.Contains(out.String(), name+": fresh (quiet)") {
					t.Fatalf("status: %s", out.String())
				}
			}
		} else if strings.Contains(stderr.String(), "archive is stale") || !strings.Contains(stderr.String(), "No results:") {
			t.Fatalf("healthy search diagnostic: %s", stderr.String())
		}
	}
	cfg.Sync.StaleAfter = "1ns"
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	cmd := cli.NewRootCmd()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--config", path, "search", "unmatchablezzzz", "--stream", "general"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "archive is stale") {
		t.Fatalf("configured age diagnostic: %s", stderr.String())
	}
}
