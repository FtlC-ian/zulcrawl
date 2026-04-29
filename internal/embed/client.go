// Package embed provides a thin client for generating text embeddings via
// a local Ollama server (http://localhost:11434 by default).
//
// Only brute-force cosine similarity is used; no vector extension is required.
// The client normalises every returned vector to unit length so that cosine
// similarity reduces to a dot product at query time.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// DefaultOllamaBase is the default Ollama server URL.
const DefaultOllamaBase = "http://localhost:11434"

// DefaultBatchSize is the number of texts sent in a single /api/embed request.
const DefaultBatchSize = 32

// Client generates embeddings from a text input.
type Client interface {
	// Embed returns a normalised float32 vector for each input text.
	// The returned slice is parallel to texts; len(result) == len(texts).
	Embed(ctx context.Context, texts []string) ([][]float32, error)

	// Model returns the model identifier used by this client.
	Model() string

	// Dim returns the expected embedding dimension, or 0 if unknown.
	Dim() int
}

// OllamaClient calls the Ollama /api/embed endpoint.
// It implements Client.
type OllamaClient struct {
	base      string
	model     string
	batchSize int
	dim       int // 0 = not yet observed
	http      *http.Client
}

// NewOllamaClient constructs an OllamaClient.
// base defaults to DefaultOllamaBase; batchSize defaults to DefaultBatchSize.
func NewOllamaClient(base, model string, batchSize int) *OllamaClient {
	if base == "" {
		base = DefaultOllamaBase
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &OllamaClient{
		base:      strings.TrimRight(base, "/"),
		model:     model,
		batchSize: batchSize,
		http: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

func (c *OllamaClient) Model() string { return c.model }
func (c *OllamaClient) Dim() int      { return c.dim }

// ollamaEmbedRequest mirrors the /api/embed request body.
type ollamaEmbedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

// ollamaEmbedResponse mirrors the /api/embed response body.
type ollamaEmbedResponse struct {
	Model      string      `json:"model"`
	Embeddings [][]float64 `json:"embeddings"`
}

// tag is a single model entry from /api/tags.
type tag struct {
	Name string `json:"name"`
}

// Embed sends texts in batches and returns normalised float32 vectors.
func (c *OllamaClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += c.batchSize {
		end := start + c.batchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[start:end]
		vecs, err := c.embedBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (c *OllamaClient) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(ollamaEmbedRequest{
		Model: c.model,
		Input: texts,
	})
	if err != nil {
		return nil, fmt.Errorf("embed: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embed: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: ollama request failed — is Ollama running at %s? (%w)", c.base, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var body strings.Builder
		_, _ = io.Copy(&body, resp.Body)
		return nil, fmt.Errorf("embed: ollama returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(body.String()))
	}

	var result ollamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("embed: decode response: %w", err)
	}
	if len(result.Embeddings) != len(texts) {
		return nil, fmt.Errorf("embed: expected %d vectors, got %d", len(texts), len(result.Embeddings))
	}

	vecs := make([][]float32, len(result.Embeddings))
	for i, raw := range result.Embeddings {
		vecs[i] = normalizeF32(raw)
		if c.dim == 0 && len(vecs[i]) > 0 {
			c.dim = len(vecs[i])
		}
	}
	return vecs, nil
}

// CheckAvailable pings Ollama and verifies the requested model is present.
// Returns a helpful error message if not.
func (c *OllamaClient) CheckAvailable(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("embed: cannot build request to %s: %w", c.base, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("embed: Ollama not reachable at %s — run `ollama serve` first: %w", c.base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("embed: Ollama /api/tags returned HTTP %d", resp.StatusCode)
	}
	type listResp struct {
		Models []tag `json:"models"`
	}
	var lr listResp
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return fmt.Errorf("embed: decode /api/tags: %w", err)
	}
	for _, m := range lr.Models {
		// Ollama model names may include a ":latest" suffix.
		if strings.EqualFold(m.Name, c.model) || strings.EqualFold(strings.SplitN(m.Name, ":", 2)[0], strings.SplitN(c.model, ":", 2)[0]) {
			return nil
		}
	}
	return fmt.Errorf(
		"embed: model %q not found in Ollama — pull it first:\n  ollama pull %s\nAvailable models: %s",
		c.model, c.model, formatModelList(lr.Models),
	)
}

func formatModelList(tags []tag) string {
	names := make([]string, len(tags))
	for i, t := range tags {
		names[i] = t.Name
	}
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}

// normalizeF32 converts a float64 slice to float32 and normalises to unit length.
// If the vector is all-zero it is returned as-is.
func normalizeF32(v []float64) []float32 {
	out := make([]float32, len(v))
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	norm := math.Sqrt(sum)
	if norm < 1e-10 {
		for i, x := range v {
			out[i] = float32(x)
		}
		return out
	}
	for i, x := range v {
		out[i] = float32(x / norm)
	}
	return out
}

// CosineSimilarity computes the dot product of two unit-length float32 vectors.
// Both vectors must have the same length; behaviour is undefined otherwise.
func CosineSimilarity(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot
}

// EncodeVec serialises a float32 vector to little-endian bytes for SQLite BLOB storage.
func EncodeVec(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, x := range v {
		bits := math.Float32bits(x)
		b[i*4] = byte(bits)
		b[i*4+1] = byte(bits >> 8)
		b[i*4+2] = byte(bits >> 16)
		b[i*4+3] = byte(bits >> 24)
	}
	return b
}

// DecodeVec deserialises a float32 vector from little-endian bytes.
func DecodeVec(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("embed: vector byte length %d is not a multiple of 4", len(b))
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		bits := uint32(b[i*4]) |
			uint32(b[i*4+1])<<8 |
			uint32(b[i*4+2])<<16 |
			uint32(b[i*4+3])<<24
		v[i] = math.Float32frombits(bits)
	}
	return v, nil
}
