// File: cache.preferences.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gourdian25/grcache"
)

// cachedPreferencesStore decorates a durable PreferencesStore with
// read-through caching against any grcache.Cache, tagging every cached
// entry with the owning user so a write invalidates exactly that user's
// cache entry via grcache's InvalidateTag — a natural fit the reference
// implementation's bespoke Redis/Mongo clients had no equivalent for (see
// docs/plan/grnoti-plan.md §1.1). The durable store is always the source of
// truth: a cache read/write failure degrades to hitting the durable store
// directly, never to a hard failure.
type cachedPreferencesStore struct {
	store  PreferencesStore
	cache  grcache.Cache
	ttl    time.Duration
	logger Logger
}

var _ PreferencesStore = (*cachedPreferencesStore)(nil)

// NewCachedPreferencesStore wraps store with a grcache.Cache-backed
// read-through cache.
//
// Parameters:
//   - store: PreferencesStore — the durable source of truth
//   - cache: grcache.Cache — caller-owned; not closed by this store's
//     Close
//   - ttl: time.Duration — cache entry TTL; 0 means no expiry (rely
//     entirely on tag invalidation to keep entries fresh)
//   - logger: Logger — may be nil
func NewCachedPreferencesStore(store PreferencesStore, cache grcache.Cache, ttl time.Duration, logger Logger) PreferencesStore {
	return &cachedPreferencesStore{store: store, cache: cache, ttl: ttl, logger: OrNop(logger)}
}

func preferencesCacheKey(userID string) string { return "grnoti:preferences:" + userID }
func preferencesCacheTag(userID string) string { return "grnoti:preferences:user:" + userID }

func (s *cachedPreferencesStore) GetPreferences(ctx context.Context, userID string) (*NotificationPreferences, error) {
	key := preferencesCacheKey(userID)

	if raw, err := s.cache.Get(ctx, key); err == nil {
		var prefs NotificationPreferences
		if jsonErr := json.Unmarshal(raw, &prefs); jsonErr == nil {
			return &prefs, nil
		}
		s.logger.Warn("grnoti: preferences cache entry corrupt, falling back to durable store", "user_id", userID)
	} else if !errors.Is(err, grcache.ErrKeyNotFound) {
		s.logger.Warn("grnoti: preferences cache read failed, falling back to durable store", "user_id", userID, "error", err)
	}

	prefs, err := s.store.GetPreferences(ctx, userID)
	if err != nil {
		return nil, err
	}

	if raw, jsonErr := json.Marshal(prefs); jsonErr == nil {
		if setErr := s.cache.Set(ctx, key, raw, s.ttl, preferencesCacheTag(userID)); setErr != nil {
			s.logger.Warn("grnoti: preferences cache write failed", "user_id", userID, "error", setErr)
		}
	}
	return prefs, nil
}

func (s *cachedPreferencesStore) SavePreferences(ctx context.Context, prefs *NotificationPreferences) error {
	if err := s.store.SavePreferences(ctx, prefs); err != nil {
		return err
	}
	// Invalidate rather than update-in-place: the durable write already
	// succeeded (the guarantee), so a failed invalidation here just means a
	// stale cache entry — surfaced as a warning, not an error returned to
	// the caller, whose SavePreferences call already durably succeeded.
	if _, err := s.cache.InvalidateTag(ctx, preferencesCacheTag(prefs.UserID)); err != nil {
		s.logger.Warn("grnoti: preferences cache invalidation failed", "user_id", prefs.UserID, "error", err)
	}
	return nil
}

func (s *cachedPreferencesStore) IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error) {
	prefs, err := s.GetPreferences(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrPreferencesNotFound) {
			return true, nil
		}
		return false, fmt.Errorf("grnoti: is event type enabled for %s: %w", userID, err)
	}
	return prefs.IsEventTypeEnabled(eventType), nil
}

// Close closes the durable store. The shared grcache.Cache is caller-owned
// and not closed here — see cacheIdempotencyStore.Close's doc comment for
// why.
func (s *cachedPreferencesStore) Close() error { return s.store.Close() }
