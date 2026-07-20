// File: ratelimiter_test.go

package grnoti

import (
	"context"
	"testing"
	"time"
)

func TestLocalRateLimiter_AllowWithinBurst(t *testing.T) {
	rl, err := NewLocalRateLimiter(5, 10)
	if err != nil {
		t.Fatalf("NewLocalRateLimiter: %v", err)
	}
	allowedOnce := false
	for i := 0; i < 5; i++ {
		ok, err := rl.Allow(context.Background())
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if ok {
			allowedOnce = true
		}
	}
	if !allowedOnce {
		t.Fatal("Allow() never returned true within burst capacity")
	}
}

func TestLocalRateLimiter_InvalidConfig(t *testing.T) {
	if _, err := NewLocalRateLimiter(0, 10); err == nil {
		t.Fatal("NewLocalRateLimiter(rps=0) = nil error, want non-nil")
	}
	if _, err := NewLocalRateLimiter(10, 5); err == nil {
		t.Fatal("NewLocalRateLimiter(burst<rps) = nil error, want non-nil")
	}
}

func TestLocalRateLimiter_Wait(t *testing.T) {
	rl, _ := NewLocalRateLimiter(1000, 1000)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestLocalRateLimiter_GetStats(t *testing.T) {
	rl, _ := NewLocalRateLimiter(10, 10)
	_, _ = rl.Allow(context.Background())
	stats, err := rl.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.RequestsPerSecond != 10 || stats.BurstSize != 10 {
		t.Fatalf("GetStats() = %+v, want RequestsPerSecond=10 BurstSize=10", stats)
	}
	if stats.AllowedCount == 0 {
		t.Fatal("GetStats().AllowedCount = 0, want > 0 after a successful Allow")
	}
}

func TestLocalRateLimiter_UpdateLimit(t *testing.T) {
	limiter, _ := NewLocalRateLimiter(5, 5)
	rl := limiter.(*localRateLimiter)
	if err := rl.UpdateLimit(20, 20); err != nil {
		t.Fatalf("UpdateLimit: %v", err)
	}
	stats, _ := rl.GetStats(context.Background())
	if stats.RequestsPerSecond != 20 {
		t.Fatalf("GetStats() after UpdateLimit = %+v, want RequestsPerSecond=20", stats)
	}
}
