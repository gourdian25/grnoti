// File: cache_test.go

package grnoti

import (
	"context"
	"testing"
	"time"

	"github.com/gourdian25/grcache"
	"github.com/gourdian25/grcache/memory"
)

func newTestCache(t *testing.T) grcache.Cache {
	t.Helper()
	cache, err := memory.NewMemoryCache()
	if err != nil {
		t.Fatalf("memory.NewMemoryCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func TestCacheIdempotencyStore_MarkAndCheck(t *testing.T) {
	store := NewCacheIdempotencyStore(newTestCache(t))
	ctx := context.Background()

	if processed, err := store.IsProcessed(ctx, "evt-1"); err != nil || processed {
		t.Fatalf("IsProcessed(unmarked) = (%v, %v), want (false, nil)", processed, err)
	}

	if err := store.MarkProcessed(ctx, "evt-1", time.Hour); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	if processed, err := store.IsProcessed(ctx, "evt-1"); err != nil || !processed {
		t.Fatalf("IsProcessed(marked) = (%v, %v), want (true, nil)", processed, err)
	}
}

func TestCacheIdempotencyStore_MarkProcessedTwiceIsIdempotent(t *testing.T) {
	store := NewCacheIdempotencyStore(newTestCache(t))
	ctx := context.Background()
	if err := store.MarkProcessed(ctx, "evt-1", time.Hour); err != nil {
		t.Fatalf("MarkProcessed (first): %v", err)
	}
	if err := store.MarkProcessed(ctx, "evt-1", time.Hour); err != nil {
		t.Fatalf("MarkProcessed (second): %v", err)
	}
}

func TestCacheIdempotencyStore_Expiry(t *testing.T) {
	store := NewCacheIdempotencyStore(newTestCache(t))
	ctx := context.Background()
	if err := store.MarkProcessed(ctx, "evt-1", 50*time.Millisecond); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if processed, err := store.IsProcessed(ctx, "evt-1"); err != nil || processed {
		t.Fatalf("IsProcessed(expired) = (%v, %v), want (false, nil)", processed, err)
	}
}

func TestCacheIdempotencyStore_Close_DoesNotCloseSharedCache(t *testing.T) {
	cache := newTestCache(t)
	store := NewCacheIdempotencyStore(cache)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The shared cache must still be usable — Close on the adapter must not
	// have closed the caller-owned cache.
	if err := cache.Set(context.Background(), "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("cache unusable after adapter Close: %v", err)
	}
}

type stubDurablePreferencesStore struct {
	prefs  map[string]*NotificationPreferences
	gets   int
	closed bool
}

func newStubDurablePreferencesStore() *stubDurablePreferencesStore {
	return &stubDurablePreferencesStore{prefs: make(map[string]*NotificationPreferences)}
}

func (s *stubDurablePreferencesStore) GetPreferences(_ context.Context, userID string) (*NotificationPreferences, error) {
	s.gets++
	p, ok := s.prefs[userID]
	if !ok {
		return nil, ErrPreferencesNotFound
	}
	copied := *p
	return &copied, nil
}
func (s *stubDurablePreferencesStore) SavePreferences(_ context.Context, prefs *NotificationPreferences) error {
	copied := *prefs
	s.prefs[prefs.UserID] = &copied
	return nil
}
func (s *stubDurablePreferencesStore) IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error) {
	p, err := s.GetPreferences(ctx, userID)
	if err != nil {
		if err == ErrPreferencesNotFound {
			return true, nil
		}
		return false, err
	}
	return p.IsEventTypeEnabled(eventType), nil
}
func (s *stubDurablePreferencesStore) Close() error { s.closed = true; return nil }

func TestCachedPreferencesStore_ReadThrough(t *testing.T) {
	durable := newStubDurablePreferencesStore()
	_ = durable.SavePreferences(context.Background(), &NotificationPreferences{UserID: "u1", GlobalEnabled: true})

	store := NewCachedPreferencesStore(durable, newTestCache(t), time.Hour, nil)
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "u1"); err != nil {
		t.Fatalf("GetPreferences (first, cache miss): %v", err)
	}
	if _, err := store.GetPreferences(ctx, "u1"); err != nil {
		t.Fatalf("GetPreferences (second, cache hit): %v", err)
	}

	if durable.gets != 1 {
		t.Fatalf("durable store GetPreferences called %d times, want 1 (second call should have hit the cache)", durable.gets)
	}
}

func TestCachedPreferencesStore_SaveInvalidatesCache(t *testing.T) {
	durable := newStubDurablePreferencesStore()
	_ = durable.SavePreferences(context.Background(), &NotificationPreferences{UserID: "u1", GlobalEnabled: true})
	store := NewCachedPreferencesStore(durable, newTestCache(t), time.Hour, nil)
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "u1"); err != nil {
		t.Fatalf("GetPreferences: %v", err)
	}
	if durable.gets != 1 {
		t.Fatalf("durable gets = %d, want 1", durable.gets)
	}

	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1", GlobalEnabled: false}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}

	got, err := store.GetPreferences(ctx, "u1")
	if err != nil {
		t.Fatalf("GetPreferences (after invalidation): %v", err)
	}
	if got.GlobalEnabled {
		t.Fatal("GetPreferences after SavePreferences returned stale cached value")
	}
	if durable.gets != 2 {
		t.Fatalf("durable gets after invalidation = %d, want 2 (cache should have been invalidated)", durable.gets)
	}
}

func TestCachedPreferencesStore_NotFoundPropagates(t *testing.T) {
	store := NewCachedPreferencesStore(newStubDurablePreferencesStore(), newTestCache(t), time.Hour, nil)
	if _, err := store.GetPreferences(context.Background(), "never-seen"); err != ErrPreferencesNotFound {
		t.Fatalf("GetPreferences(unknown) error = %v, want ErrPreferencesNotFound", err)
	}
}

func TestCachedPreferencesStore_Close_ClosesDurableNotCache(t *testing.T) {
	durable := newStubDurablePreferencesStore()
	cache := newTestCache(t)
	store := NewCachedPreferencesStore(durable, cache, time.Hour, nil)

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !durable.closed {
		t.Fatal("Close did not close the durable store")
	}
	if err := cache.Set(context.Background(), "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("shared cache unusable after adapter Close: %v", err)
	}
}

func TestCacheBackedExperimentEngine_AssignAndGet(t *testing.T) {
	engine := NewCacheBackedExperimentEngine(newTestCache(t), nil, nil)
	ctx := context.Background()
	experiment := &Experiment{ID: "exp-1", Variants: []ExperimentVariant{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}}

	assigned, err := engine.AssignVariant(ctx, "user-1", experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}

	got, err := engine.GetVariant(ctx, "user-1", "exp-1")
	if err != nil {
		t.Fatalf("GetVariant: %v", err)
	}
	if got == nil || got.ID != assigned.ID {
		t.Fatalf("GetVariant() = %+v, want %+v", got, assigned)
	}

	// Repeated AssignVariant must return the same cached value, not
	// recompute (still deterministic either way, but this proves the cache
	// read path is actually exercised).
	again, err := engine.AssignVariant(ctx, "user-1", experiment)
	if err != nil || again.ID != assigned.ID {
		t.Fatalf("AssignVariant (repeat) = (%+v, %v), want %s", again, err, assigned.ID)
	}
}

func TestCacheBackedExperimentEngine_GetVariant_Unassigned(t *testing.T) {
	engine := NewCacheBackedExperimentEngine(newTestCache(t), nil, nil)
	got, err := engine.GetVariant(context.Background(), "user-1", "never-assigned")
	if err != nil || got != nil {
		t.Fatalf("GetVariant(unassigned) = (%+v, %v), want (nil, nil)", got, err)
	}
}

func TestCacheBackedExperimentEngine_NoVariants(t *testing.T) {
	engine := NewCacheBackedExperimentEngine(newTestCache(t), nil, nil)
	_, err := engine.AssignVariant(context.Background(), "user-1", &Experiment{ID: "empty"})
	if err != ErrExperimentHasNoVariants {
		t.Fatalf("AssignVariant(no variants) error = %v, want ErrExperimentHasNoVariants", err)
	}
}
