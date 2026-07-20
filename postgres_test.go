// File: postgres_test.go

package grnoti

import (
	"math"
	"testing"
)

func TestPgInt32(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int32
	}{
		{"zero", 0, 0},
		{"typical maxRetries", 3, 3},
		{"typical claim limit", 100, 100},
		{"exactly MaxInt32", math.MaxInt32, math.MaxInt32},
		{"overflow clamps to MaxInt32", math.MaxInt32 + 1000, math.MaxInt32},
		{"negative passes through", -1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pgInt32(tc.in); got != tc.want {
				t.Fatalf("pgInt32(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
