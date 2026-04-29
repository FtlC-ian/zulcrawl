package embed

import (
	"context"
	"testing"
)

func TestFakeClientReturnsUnitVectors(t *testing.T) {
	c := NewFakeClient("test-model", 8)
	vecs, err := c.Embed(context.Background(), []string{"hello", "world", "foo"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 vectors, got %d", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 8 {
			t.Errorf("vec[%d]: expected dim=8, got %d", i, len(v))
		}
		if !IsUnitLength(v) {
			t.Errorf("vec[%d]: not unit length", i)
		}
	}
}

func TestFakeClientDeterministic(t *testing.T) {
	c := NewFakeClient("test-model", 4)
	v1, _ := c.Embed(context.Background(), []string{"hello"})
	v2, _ := c.Embed(context.Background(), []string{"hello"})
	for i := range v1[0] {
		if v1[0][i] != v2[0][i] {
			t.Errorf("non-deterministic at index %d: %v vs %v", i, v1[0][i], v2[0][i])
		}
	}
}

func TestFakeClientDifferentTexts(t *testing.T) {
	c := NewFakeClient("test-model", 4)
	vecs, _ := c.Embed(context.Background(), []string{"hello", "world"})
	identical := true
	for i := range vecs[0] {
		if vecs[0][i] != vecs[1][i] {
			identical = false
			break
		}
	}
	if identical {
		t.Error("expected different vectors for different texts")
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	c := NewFakeClient("test-model", 16)
	vecs, _ := c.Embed(context.Background(), []string{"round trip test"})
	v := vecs[0]
	got, err := EncodeDecodeRoundtrip(v)
	if err != nil {
		t.Fatalf("EncodeDecodeRoundtrip: %v", err)
	}
	if len(got) != len(v) {
		t.Fatalf("length mismatch: want %d got %d", len(v), len(got))
	}
	for i := range v {
		if v[i] != got[i] {
			t.Errorf("index %d: want %v got %v", i, v[i], got[i])
		}
	}
}

func TestEncodeDecodeInvalidLength(t *testing.T) {
	_, err := DecodeVec([]byte{1, 2, 3}) // not divisible by 4
	if err == nil {
		t.Fatal("expected error for invalid byte length")
	}
}

func TestCosineSimilarity(t *testing.T) {
	// Two identical unit vectors → similarity ≈ 1.0
	c := NewFakeClient("test-model", 4)
	vecs, _ := c.Embed(context.Background(), []string{"same"})
	sim := CosineSimilarity(vecs[0], vecs[0])
	if sim < 0.9999 {
		t.Errorf("expected similarity ~1.0 for identical vectors, got %f", sim)
	}
}

func TestEmptyEmbed(t *testing.T) {
	c := NewFakeClient("test-model", 4)
	vecs, err := c.Embed(context.Background(), nil)
	if err != nil {
		t.Fatalf("Embed nil: %v", err)
	}
	if len(vecs) != 0 {
		t.Errorf("expected empty result, got %d vectors", len(vecs))
	}
}
