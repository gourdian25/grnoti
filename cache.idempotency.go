// File: cache.idempotency.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gourdian25/grcache"
)

// cacheIdempotencyStore implements IdempotencyStore as a thin adapter over
// any grcache.Cache — Get/Exists/Set with a TTL is all IsProcessed/
// MarkProcessed actually need, so one adapter here replaces what the
// reference implementation wrote as two separate ~150-400 line hand-rolled
// Redis and Mongo clients (see docs/plan/grnoti-plan.md §1.1). Backend
// choice becomes "which grcache.Cache the caller constructs and passes
// in" (grcache/redis, grcache/mongostore, grcache/memory, ...), not "which
// grnoti store type to construct."
type cacheIdempotencyStore struct {
	cache     grcache.Cache
	keyPrefix string
}

var _ IdempotencyStore = (*cacheIdempotencyStore)(nil)

// NewCacheIdempotencyStore constructs an IdempotencyStore backed by cache.
//
// Parameters:
//   - cache: grcache.Cache — caller-owned; not closed by this store's
//     Close (see Close's doc comment)
func NewCacheIdempotencyStore(cache grcache.Cache) IdempotencyStore {
	return &cacheIdempotencyStore{cache: cache, keyPrefix: "grnoti:idempotency:"}
}

func (s *cacheIdempotencyStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	ok, err := s.cache.Exists(ctx, s.keyPrefix+eventID)
	if err != nil {
		return false, fmt.Errorf("grnoti: idempotency check for %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return ok, nil
}

func (s *cacheIdempotencyStore) MarkProcessed(ctx context.Context, eventID string, ttl time.Duration) error {
	if ttl < 0 {
		ttl = 0
	}
	marker := []byte(time.Now().UTC().Format(time.RFC3339))
	if err := s.cache.Set(ctx, s.keyPrefix+eventID, marker, ttl); err != nil {
		return fmt.Errorf("grnoti: mark processed for %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

// Close is a no-op: the underlying grcache.Cache is caller-owned (likely
// shared with other grnoti components, e.g. the preferences read-cache and
// experiment-assignment cache — see cache.preferences.go,
// cache.experiment.go), so closing it here would break every other holder
// of the same *grcache.Cache handle. The caller is responsible for closing
// the cache it constructed.
func (s *cacheIdempotencyStore) Close() error { return nil }
