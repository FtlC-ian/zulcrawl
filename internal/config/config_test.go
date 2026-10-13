package config

import (
	"path/filepath"
	"testing"
)

func TestExplicitMissingConfigDoesNotFallBackToLiveDatabase(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.toml"))
	if err == nil || cfg != nil {
		t.Fatalf("missing explicit config must fail closed, got %+v %v", cfg, err)
	}
}
