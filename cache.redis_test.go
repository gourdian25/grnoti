// File: cache.redis_test.go

package grnoti

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gourdian25/grcache"
	grcacheredis "github.com/gourdian25/grcache/redis"
)

// newTestRedisCache is cache_test.go's newTestCache, but against real local
// Redis instead of grcache/memory — closing out Stage 4's deferred "extend
// grcache/redis integration coverage once Stage 8's Redis setup exists"
// item (docs/plan/grnoti-plan.md §8 Stage 4/Stage 8). The cache-backed
// adapters (cache.idempotency.go, cache.preferences.go, cache.experiment.go)
// are otherwise only ever exercised against grcache/memory in this repo's
// test suite, which proves the adapter logic but not that it actually works
// against the backend it's meant to run against in production.
func newTestRedisCache(t *testing.T) grcache.Cache {
	t.Helper()
	cache, err := grcacheredis.NewRedisCache(grcacheredis.RedisConfig{Addr: testRedisAddr})
	if err != nil {
		t.Skipf("Redis not available at %s, skipping: %v", testRedisAddr, err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func TestCacheIdempotencyStore_Redis_MarkAndCheck(t *testing.T) {
	store := NewCacheIdempotencyStore(newTestRedisCache(t))
	ctx := context.Background()
	// A nonce beyond t.Name() is required: this hits real Redis, which
	// persists across separate `go test` invocations (unlike the ephemeral
	// in-memory backend used elsewhere), so a name-only key collides with
	// leftover state from a prior run within the same TTL window — the
	// same class of bug documented in docs/plan/grnoti-plan.md §11 for the
	// Stage-7 Mongo contract tests.
	eventID := fmt.Sprintf("redis-evt-%s-%d", t.Name(), time.Now().UnixNano())

	if processed, err := store.IsProcessed(ctx, eventID); err != nil || processed {
		t.Fatalf("IsProcessed(unmarked) = (%v, %v), want (false, nil)", processed, err)
	}
	if err := store.MarkProcessed(ctx, eventID, time.Minute); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if processed, err := store.IsProcessed(ctx, eventID); err != nil || !processed {
		t.Fatalf("IsProcessed(marked) = (%v, %v), want (true, nil)", processed, err)
	}
}

func TestCachedPreferencesStore_Redis_ReadThroughAndInvalidate(t *testing.T) {
	durable := newStubDurablePreferencesStore()
	userID := fmt.Sprintf("redis-u-%s-%d", t.Name(), time.Now().UnixNano())
	_ = durable.SavePreferences(context.Background(), &NotificationPreferences{UserID: userID, GlobalEnabled: true})

	store := NewCachedPreferencesStore(durable, newTestRedisCache(t), time.Minute, nil)
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, userID); err != nil {
		t.Fatalf("GetPreferences (first, cache miss): %v", err)
	}
	if _, err := store.GetPreferences(ctx, userID); err != nil {
		t.Fatalf("GetPreferences (second, cache hit): %v", err)
	}
	if durable.gets != 1 {
		t.Fatalf("durable store GetPreferences called %d times, want 1 (second call should have hit Redis)", durable.gets)
	}

	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: userID, GlobalEnabled: false}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}
	got, err := store.GetPreferences(ctx, userID)
	if err != nil {
		t.Fatalf("GetPreferences (after invalidation): %v", err)
	}
	if got.GlobalEnabled {
		t.Fatal("GetPreferences after SavePreferences returned stale cached value")
	}
	if durable.gets != 2 {
		t.Fatalf("durable gets after invalidation = %d, want 2 (Redis tag invalidation should have evicted the entry)", durable.gets)
	}
}

func TestCacheBackedExperimentEngine_Redis_AssignAndGet(t *testing.T) {
	engine := NewCacheBackedExperimentEngine(newTestRedisCache(t), nil, nil, nil)
	ctx := context.Background()
	nonce := time.Now().UnixNano()
	experiment := &Experiment{ID: fmt.Sprintf("redis-exp-%s-%d", t.Name(), nonce), Variants: []ExperimentVariant{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}}
	userID := fmt.Sprintf("redis-user-%s-%d", t.Name(), nonce)

	assigned, err := engine.AssignVariant(ctx, userID, experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}
	got, err := engine.GetVariant(ctx, userID, experiment.ID)
	if err != nil {
		t.Fatalf("GetVariant: %v", err)
	}
	if got == nil || got.ID != assigned.ID {
		t.Fatalf("GetVariant() = %+v, want %+v", got, assigned)
	}
}
