package cli_test

import (
	"bytes"
	"encoding/json"
	"github.com/FtlC-ian/zulcrawl/internal/cli"
	"github.com/FtlC-ian/zulcrawl/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatusJSONShape(t *testing.T) {
	cfg := setupCLITest(t)
	cmd := cli.NewRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--config", cfg, "status", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var report struct {
		SchemaVersion int              `json:"schema_version"`
		Streams       []map[string]any `json:"streams"`
		Failures      []any            `json:"failures"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON: %s: %v", out.String(), err)
	}
	if report.SchemaVersion != 1 || len(report.Streams) != 1 || report.Failures == nil {
		t.Fatalf("shape: %+v", report)
	}
	for _, key := range []string{"stream_id", "name", "message_count", "last_attempt", "last_success", "last_error", "newest_message_at", "stale", "quiet", "checkpoint_missing", "repair_anchor", "repair_completed_at", "excluded"} {
		if _, ok := report.Streams[0][key]; !ok {
			t.Errorf("missing key %s", key)
		}
	}
}

func TestDoctorJSONAndEmptySearchDiagnostics(t *testing.T) {
	path := setupCLITest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/streams" {
			t.Errorf("unexpected API request %s", r.URL.Path)
		}
		fmtJSON := `{"result":"success","streams":[]}`
		_, _ = w.Write([]byte(fmtJSON))
	}))
	defer server.Close()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Zulip.URL = server.URL
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	cmd := cli.NewRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--config", path, "doctor", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["ok"] != true || result["schema_version"] != float64(1) || result["streams"] == nil || result["failures"] == nil {
		t.Fatalf("doctor JSON: %s", out.String())
	}
	for _, tc := range []struct{ stream, reason string }{{"general", "archive is stale"}, {"random", "excluded by sync config"}, {"absent", "no local messages"}} {
		t.Run(tc.stream, func(t *testing.T) {
			cmd := cli.NewRootCmd()
			var stderr bytes.Buffer
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{"--config", path, "search", "unmatchablezzzz", "--stream", tc.stream})
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(stderr.String(), tc.reason) {
				t.Fatalf("diagnostic: %s", stderr.String())
			}
		})
	}
}
