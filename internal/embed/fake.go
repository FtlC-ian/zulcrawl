package embed

import (
	"context"
	"math"
)

// FakeClient is a Client implementation for use in tests.
// It returns deterministic unit-length vectors without any network call.
type FakeClient struct {
	model string
	dim   int
	// EmbedFunc can be set to override the default deterministic behaviour.
	EmbedFunc func(ctx context.Context, texts []string) ([][]float32, error)
}

// NewFakeClient creates a FakeClient that returns dim-dimensional unit vectors.
// dim defaults to 4 when ≤ 0.
func NewFakeClient(model string, dim int) *FakeClient {
	if dim <= 0 {
		dim = 4
	}
	return &FakeClient{model: model, dim: dim}
}

func (f *FakeClient) Model() string { return f.model }
func (f *FakeClient) Dim() int      { return f.dim }

// Embed returns a synthetic unit-length vector for each input text.
// The first component equals cos(hash(text)) and the rest are spread evenly on
// the unit hypersphere; uniqueness is good enough for test assertions.
func (f *FakeClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if f.EmbedFunc != nil {
		return f.EmbedFunc(ctx, texts)
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = fakeVec(t, f.dim)
	}
	return out, nil
}

// fakeVec returns a stable unit-length float32 vector derived from text.
func fakeVec(text string, dim int) []float32 {
	// Use a simple hash seed derived from the text so identical texts produce
	// identical vectors.
	seed := uint32(2166136261)
	for _, b := range []byte(text) {
		seed ^= uint32(b)
		seed *= 16777619
	}

	v := make([]float64, dim)
	for i := range v {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		v[i] = float64(int32(seed)) / float64(1<<31)
	}
	return normalizeF32(v)
}

// CosineSimilarityTest is a convenience function for test assertions.
func CosineSimilarityTest(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot
}

// EncodeDecodeRoundtrip checks that encode→decode is lossless within float32
// precision. Exported for use in tests.
func EncodeDecodeRoundtrip(v []float32) ([]float32, error) {
	return DecodeVec(EncodeVec(v))
}

// IsUnitLength reports whether v is within tolerance of unit length.
func IsUnitLength(v []float32) bool {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return math.Abs(sum-1.0) < 1e-5
}
