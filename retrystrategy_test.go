// File: retrystrategy_test.go

package grnoti

import (
	"errors"
	"testing"
	"time"
)

func TestFullJitterBackoff_Bounds(t *testing.T) {
	base := 100 * time.Millisecond
	max := time.Second
	for attempt := 0; attempt < 10; attempt++ {
		for i := 0; i < 20; i++ {
			d := FullJitterBackoff(base, max, attempt)
			if d < 0 || d > max {
				t.Fatalf("FullJitterBackoff(attempt=%d) = %v, want in [0, %v]", attempt, d, max)
			}
		}
	}
}

func TestFullJitterBackoff_ZeroBase(t *testing.T) {
	if d := FullJitterBackoff(0, time.Second, 0); d != 0 {
		t.Fatalf("FullJitterBackoff(base=0) = %v, want 0", d)
	}
}

func TestFullJitterBackoff_DefaultCeiling(t *testing.T) {
	d := FullJitterBackoff(time.Hour, 0, 5) // max<=0 should fall back to defaultMaxBackoff
	if d > defaultMaxBackoff {
		t.Fatalf("FullJitterBackoff(max<=0) = %v, want <= defaultMaxBackoff (%v)", d, defaultMaxBackoff)
	}
}

func TestFullJitterRetry_ShouldRetry(t *testing.T) {
	rs := NewFullJitterRetry(3, 10*time.Millisecond, time.Second)

	if rs.ShouldRetry(0, nil) {
		t.Error("ShouldRetry(attempt=0, err=nil) = true, want false")
	}
	if rs.ShouldRetry(3, errors.New("boom")) {
		t.Error("ShouldRetry(attempt=maxAttempts) = true, want false")
	}

	retryable := NewFCMError(FCMErrorCodeUnavailable, "tok", "unavailable", nil)
	if !rs.ShouldRetry(0, retryable) {
		t.Error("ShouldRetry with a retryable FCMError = false, want true")
	}

	permanent := NewFCMError(FCMErrorCodeUnregistered, "tok", "gone", nil)
	if rs.ShouldRetry(0, permanent) {
		t.Error("ShouldRetry with a permanent FCMError = true, want false")
	}

	if !rs.ShouldRetry(0, errors.New("plain transient error")) {
		t.Error("ShouldRetry with an unclassified error = false, want true (default to retryable)")
	}
}

func TestFullJitterRetry_GetDelay(t *testing.T) {
	rs := NewFullJitterRetry(5, 50*time.Millisecond, time.Second)
	if d := rs.GetDelay(-1); d < 0 {
		t.Fatalf("GetDelay(-1) = %v, want >= 0", d)
	}
}

func TestNoopRetryStrategy(t *testing.T) {
	rs := NewNoopRetryStrategy()
	if rs.ShouldRetry(0, errors.New("boom")) {
		t.Error("noopRetryStrategy.ShouldRetry() = true, want false")
	}
	if rs.GetDelay(0) != 0 {
		t.Error("noopRetryStrategy.GetDelay() != 0")
	}
}
