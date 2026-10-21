// File: batchsplitter_test.go

package grnoti

import "testing"

func TestBatchSplitter_Deduplicate(t *testing.T) {
	s := NewBatchSplitter()
	in := []Device{{FID: "a"}, {FID: "b"}, {FID: "a"}, {FID: "c"}, {FID: "b"}}
	out := s.Deduplicate(in)
	if len(out) != 3 {
		t.Fatalf("Deduplicate() returned %d devices, want 3", len(out))
	}
	if out[0].FID != "a" || out[1].FID != "b" || out[2].FID != "c" {
		t.Fatalf("Deduplicate() = %v, want order-preserving [a b c]", out)
	}
}

func TestBatchSplitter_Split(t *testing.T) {
	s := NewBatchSplitter()
	devices := make([]Device, 10)
	for i := range devices {
		devices[i] = Device{FID: string(rune('a' + i))}
	}

	batches := s.Split(devices, 3)
	if len(batches) != 4 {
		t.Fatalf("Split(10 devices, batch=3) returned %d batches, want 4", len(batches))
	}
	if len(batches[0]) != 3 || len(batches[3]) != 1 {
		t.Fatalf("Split() batch sizes = %v, want [3 3 3 1]", batchSizes(batches))
	}
}

func TestBatchSplitter_Split_NoLimit(t *testing.T) {
	s := NewBatchSplitter()
	devices := []Device{{FID: "a"}, {FID: "b"}}
	batches := s.Split(devices, 0)
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("Split(maxBatchSize=0) = %v, want a single batch with all devices", batches)
	}
}

func batchSizes(batches [][]Device) []int {
	sizes := make([]int, len(batches))
	for i, b := range batches {
		sizes[i] = len(b)
	}
	return sizes
}
