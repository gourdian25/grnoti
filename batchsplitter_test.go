// File: batchsplitter_test.go

package grnoti

import "testing"

func TestBatchSplitter_Deduplicate(t *testing.T) {
	s := NewBatchSplitter()
	in := []DeviceToken{{Token: "a"}, {Token: "b"}, {Token: "a"}, {Token: "c"}, {Token: "b"}}
	out := s.Deduplicate(in)
	if len(out) != 3 {
		t.Fatalf("Deduplicate() returned %d tokens, want 3", len(out))
	}
	if out[0].Token != "a" || out[1].Token != "b" || out[2].Token != "c" {
		t.Fatalf("Deduplicate() = %v, want order-preserving [a b c]", out)
	}
}

func TestBatchSplitter_Split(t *testing.T) {
	s := NewBatchSplitter()
	tokens := make([]DeviceToken, 10)
	for i := range tokens {
		tokens[i] = DeviceToken{Token: string(rune('a' + i))}
	}

	batches := s.Split(tokens, 3)
	if len(batches) != 4 {
		t.Fatalf("Split(10 tokens, batch=3) returned %d batches, want 4", len(batches))
	}
	if len(batches[0]) != 3 || len(batches[3]) != 1 {
		t.Fatalf("Split() batch sizes = %v, want [3 3 3 1]", batchSizes(batches))
	}
}

func TestBatchSplitter_Split_NoLimit(t *testing.T) {
	s := NewBatchSplitter()
	tokens := []DeviceToken{{Token: "a"}, {Token: "b"}}
	batches := s.Split(tokens, 0)
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("Split(maxBatchSize=0) = %v, want a single batch with all tokens", batches)
	}
}

func batchSizes(batches [][]DeviceToken) []int {
	sizes := make([]int, len(batches))
	for i, b := range batches {
		sizes[i] = len(b)
	}
	return sizes
}
