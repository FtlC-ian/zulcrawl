package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	toml "github.com/pelletier/go-toml/v2"
)

type Config struct {
	Zulip struct {
		URL    string `toml:"url"`
		Email  string `toml:"email"`
		APIKey string `toml:"api_key"`
	} `toml:"zulip"`
	Database struct {
		Path string `toml:"path"`
	} `toml:"database"`
	Sync struct {
		Concurrency    int      `toml:"concurrency"`
		Streams        []string `toml:"streams"`
		ExcludeStreams []string `toml:"exclude_streams"`
	} `toml:"sync"`
	Tail struct {
		RepairInterval string `toml:"repair_interval"`
	} `toml:"tail"`
	Search struct {
		SemanticEnabled bool   `toml:"semantic_enabled"`
		EmbeddingModel  string `toml:"embedding_model"`
	} `toml:"search"`
	MCP struct {
		Enabled bool `toml:"enabled"`
		Port    int  `toml:"port"`
	} `toml:"mcp"`
}

func Default() *Config {
	c := &Config{}
	c.Database.Path = "~/.zulcrawl/zulcrawl.db"
	c.Sync.Concurrency = 4
	c.Sync.Streams = []string{}
	c.Sync.ExcludeStreams = []string{"social", "random"}
	c.Tail.RepairInterval = "30m"
	c.MCP.Port = 8849
	return c
}

func DefaultPath() string {
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".zulcrawl", "config.toml")
}

func ExpandPath(path string) string {
	if path == "" {
		return path
	}
	if strings.HasPrefix(path, "~/") {
		h, _ := os.UserHomeDir()
		return filepath.Join(h, path[2:])
	}
	return path
}

func Load(path string) (*Config, error) {
	if path == "" {
		path = DefaultPath()
	}
	path = ExpandPath(path)

	cfg := Default()
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			cfg.ApplyEnv()
			return cfg, nil
		}
		return nil, err
	}
	if err := toml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.ApplyEnv()
	cfg.Normalize()
	return cfg, nil
}

func (c *Config) Normalize() {
	if c.Sync.Concurrency <= 0 {
		c.Sync.Concurrency = 4
	}
	if c.Database.Path == "" {
		c.Database.Path = "~/.zulcrawl/zulcrawl.db"
	}
}

func (c *Config) ApplyEnv() {
	if v := os.Getenv("ZULIP_EMAIL"); v != "" {
		c.Zulip.Email = v
	}
	if v := os.Getenv("ZULIP_API_KEY"); v != "" {
		c.Zulip.APIKey = v
	}
	if v := os.Getenv("ZULIP_URL"); v != "" {
		c.Zulip.URL = v
	}
}

func (c *Config) Save(path string) error {
	if path == "" {
		path = DefaultPath()
	}
	path = ExpandPath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := toml.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (c *Config) DBPath() string {
	return ExpandPath(c.Database.Path)
}

func (c *Config) ValidateAuth() error {
	if c.Zulip.URL == "" {
		return errors.New("zulip.url is missing (config or ZULIP_URL)")
	}
	if c.Zulip.Email == "" {
		return errors.New("zulip.email is missing (config or ZULIP_EMAIL)")
	}
	if c.Zulip.APIKey == "" {
		return errors.New("zulip.api_key is missing (config or ZULIP_API_KEY)")
	}
	return nil
}
