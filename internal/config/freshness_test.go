package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSyncStaleAfterConfig(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{value: "", want: 2 * time.Hour},
		{value: "4h", want: 4 * time.Hour},
		{value: "0s", invalid: true},
		{value: "-1h", invalid: true},
		{value: "tomorrow", invalid: true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			text := "[sync]"
			if tc.value != "" {
				text += "\nstale_after = \"" + tc.value + "\""
			}
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			d, err := cfg.SyncStaleAfter()
			if (err != nil) != tc.invalid || (!tc.invalid && d != tc.want) {
				t.Fatalf("duration: %s %v", d, err)
			}
		})
	}
}
