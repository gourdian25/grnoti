// File: ratelimiter.redis_test.go

package grnoti

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testRedisAddr = "localhost:6379"

// newTestRedisRateLimiter gives each test its own bucket key — same
// per-test-isolation reasoning as the Mongo backend's per-test collection
// name (contract_helpers_test.go), just without needing an explicit drop
// since redisRateLimiterKeyTTL reclaims idle keys on its own.
func newTestRedisRateLimiter(t *testing.T, rps, burst int) *redisRateLimiter {
	t.Helper()
	rl, err := NewRedisRateLimiter(RedisRateLimiterConfig{
		Addr: testRedisAddr, RequestsPerSecond: rps, BurstSize: burst,
		Key: fmt.Sprintf("test:ratelimit:%s:%d", t.Name(), time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("Redis not available at %s, skipping: %v", testRedisAddr, err)
	}
	t.Cleanup(func() { _ = rl.(*redisRateLimiter).Close() })
	return rl.(*redisRateLimiter)
}

func TestRedisRateLimiter_AllowWithinBurst(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 10)
	ctx := context.Background()

	allowedOnce := false
	for i := 0; i < 5; i++ {
		ok, err := rl.Allow(ctx)
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

func TestRedisRateLimiter_ExhaustsBucketThenRefuses(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 2, 2)
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 2; i++ {
		ok, err := rl.Allow(ctx)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if ok {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed = %d within burst=2, want 2", allowed)
	}

	ok, err := rl.Allow(ctx)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatal("Allow() = true immediately after exhausting burst, want false")
	}
}

func TestRedisRateLimiter_Allow_CanceledContext(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rl.Allow(ctx); err == nil {
		t.Fatal("Allow(canceled ctx) = nil error, want non-nil")
	}
}

func TestRedisRateLimiter_UpdateLimit_InvalidConfig(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 5)
	if err := rl.UpdateLimit(0, 5); err == nil {
		t.Fatal("UpdateLimit(rps=0) = nil error, want non-nil")
	}
	if err := rl.UpdateLimit(10, 5); err == nil {
		t.Fatal("UpdateLimit(burst<rps) = nil error, want non-nil")
	}
}

func TestRedisRateLimiter_InvalidConfig(t *testing.T) {
	if _, err := NewRedisRateLimiter(RedisRateLimiterConfig{Addr: testRedisAddr, RequestsPerSecond: 0, BurstSize: 10}); err == nil {
		t.Fatal("NewRedisRateLimiter(rps=0) = nil error, want non-nil")
	}
	if _, err := NewRedisRateLimiter(RedisRateLimiterConfig{Addr: testRedisAddr, RequestsPerSecond: 10, BurstSize: 5}); err == nil {
		t.Fatal("NewRedisRateLimiter(burst<rps) = nil error, want non-nil")
	}
	if _, err := NewRedisRateLimiter(RedisRateLimiterConfig{RequestsPerSecond: 10, BurstSize: 10}); err == nil {
		t.Fatal("NewRedisRateLimiter(no Addr) = nil error, want non-nil")
	}
}

func TestRedisRateLimiter_Unreachable(t *testing.T) {
	if _, err := NewRedisRateLimiter(RedisRateLimiterConfig{Addr: "localhost:1", RequestsPerSecond: 10, BurstSize: 10}); err == nil {
		t.Fatal("NewRedisRateLimiter(unreachable) = nil error, want non-nil")
	}
}

func TestRedisRateLimiter_Wait(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 1000, 1000)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestRedisRateLimiter_WaitRespectsContextCancellation(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 1, 1)
	ctx := context.Background()
	_, _ = rl.Allow(ctx) // exhaust the single token

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := rl.Wait(waitCtx); err == nil {
		t.Fatal("Wait() on an exhausted bucket with a short-lived context = nil error, want context deadline error")
	}
}

func TestRedisRateLimiter_GetStats(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 10, 10)
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

func TestRedisRateLimiter_UpdateLimit(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 5)
	if err := rl.UpdateLimit(20, 20); err != nil {
		t.Fatalf("UpdateLimit: %v", err)
	}
	stats, _ := rl.GetStats(context.Background())
	if stats.RequestsPerSecond != 20 {
		t.Fatalf("GetStats() after UpdateLimit = %+v, want RequestsPerSecond=20", stats)
	}
}

func TestRedisRateLimiter_Close_Idempotent(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 5)
	if err := rl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rl.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if _, err := rl.Allow(context.Background()); err != ErrClosed {
		t.Fatalf("Allow after Close error = %v, want ErrClosed", err)
	}
}

func TestRedisRateLimiter_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	rl := newTestRedisRateLimiter(t, 5, 5)
	if err := rl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if err := rl.Wait(ctx); err != ErrClosed {
		t.Errorf("Wait after Close = %v, want ErrClosed", err)
	}
	if _, err := rl.GetStats(ctx); err != ErrClosed {
		t.Errorf("GetStats after Close = %v, want ErrClosed", err)
	}
	// Regression test for the real bug found in this hardening pass:
	// UpdateLimit didn't check r.closed at all, unlike every sibling
	// method, so it silently mutated in-memory rate/burst fields after
	// Close instead of failing.
	if err := rl.UpdateLimit(10, 10); err != ErrClosed {
		t.Errorf("UpdateLimit after Close = %v, want ErrClosed", err)
	}
}

// TestRedisRateLimiter_SharedBucketIsGloballyConsistent proves the core
// distributed claim: two independent redisRateLimiter instances (standing
// in for two service replicas) pointed at the same Key share one quota
// rather than each independently enforcing the full rate — the entire
// reason this backend exists over localRateLimiter (docs/plan/
// grnoti-plan.md §1.1, §2 item 7).
func TestRedisRateLimiter_SharedBucketIsGloballyConsistent(t *testing.T) {
	key := fmt.Sprintf("test:ratelimit:shared:%d", time.Now().UnixNano())
	newReplica := func() *redisRateLimiter {
		rl, err := NewRedisRateLimiter(RedisRateLimiterConfig{
			Addr: testRedisAddr, RequestsPerSecond: 10, BurstSize: 10, Key: key,
		})
		if err != nil {
			t.Skipf("Redis not available at %s, skipping: %v", testRedisAddr, err)
		}
		t.Cleanup(func() { _ = rl.(*redisRateLimiter).Close() })
		return rl.(*redisRateLimiter)
	}

	replicaA := newReplica()
	replicaB := newReplica()
	ctx := context.Background()

	var totalAllowed atomic.Int64
	var wg sync.WaitGroup
	for _, replica := range []*redisRateLimiter{replicaA, replicaB} {
		wg.Add(1)
		go func(rl *redisRateLimiter) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				ok, err := rl.Allow(ctx)
				if err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
				if ok {
					totalAllowed.Add(1)
				}
			}
		}(replica)
	}
	wg.Wait()

	// Burst=10 shared across both replicas' 20 combined requests: the
	// bucket refills at 10/s, negligible over this sub-second loop, so
	// allow only a little headroom above 10 — it must stay far below 20,
	// the count two *independent* per-process buckets (the bug this
	// backend fixes) would produce.
	if got := totalAllowed.Load(); got < 10 || got > 13 {
		t.Fatalf("totalAllowed = %d across two replicas sharing burst=10, want in [10,13] (proves shared quota, not two independent 10s)", got)
	}
}
