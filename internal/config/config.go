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
	Media struct {
		// CacheDir is a local-only cache for Zulip /user_uploads/ attachments.
		// Cached private media is never published by zulcrawl.
		CacheDir string `toml:"cache_dir"`
	} `toml:"media"`
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
	Embeddings struct {
		// Enabled controls whether embedding features are compiled-in and available.
		// Default: false. No embedding calls are made unless this is true.
		Enabled bool `toml:"enabled"`
		// Provider selects the embedding backend. Only "ollama" is supported.
		Provider string `toml:"provider"`
		// OllamaBase is the Ollama server base URL. Defaults to http://localhost:11434.
		OllamaBase string `toml:"ollama_base"`
		// Model is the embedding model name, e.g. "nomic-embed-text-v2-moe".
		// Pull it first: ollama pull nomic-embed-text-v2-moe
		Model string `toml:"model"`
		// BatchSize is the number of texts per Ollama /api/embed request.
		// Defaults to 32.
		BatchSize int `toml:"batch_size"`
		// SampleMessages is the number of messages sampled per topic when
		// building its embedding text. Defaults to 50.
		SampleMessages int `toml:"sample_messages"`
	} `toml:"embeddings"`
	MCP struct {
		Enabled bool `toml:"enabled"`
		Port    int  `toml:"port"`
	} `toml:"mcp"`
}

func Default() *Config {
	c := &Config{}
	c.Database.Path = "~/.zulcrawl/zulcrawl.db"
	c.Media.CacheDir = "~/.zulcrawl/media"
	c.Sync.Concurrency = 4
	c.Sync.Streams = []string{}
	c.Sync.ExcludeStreams = []string{"social", "random"}
	c.Tail.RepairInterval = "30m"
	c.MCP.Port = 8849
	c.Embeddings.Enabled = false
	c.Embeddings.Provider = "ollama"
	c.Embeddings.OllamaBase = "http://localhost:11434"
	c.Embeddings.Model = "nomic-embed-text-v2-moe"
	c.Embeddings.BatchSize = 32
	c.Embeddings.SampleMessages = 50
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
	if c.Media.CacheDir == "" {
		c.Media.CacheDir = "~/.zulcrawl/media"
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

func (c *Config) MediaCacheDir() string {
	return ExpandPath(c.Media.CacheDir)
}

func (c *Config) ValidateEmbeddings() error {
	if !c.Embeddings.Enabled {
		return nil
	}
	provider := strings.ToLower(strings.TrimSpace(c.Embeddings.Provider))
	if provider == "" {
		provider = "ollama"
		c.Embeddings.Provider = provider
	}
	if provider != "ollama" {
		return fmt.Errorf("embeddings.provider %q is not supported (only \"ollama\" is supported)", c.Embeddings.Provider)
	}
	return nil
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
