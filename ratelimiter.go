// File: ratelimiter.go

package grnoti

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// localRateLimiter is a per-process token bucket over golang.org/x/time/rate
// — the default/dev RateLimiter. It limits calls made through this one
// instance in this one process only: running N replicas of a service using
// it gives N times the configured rate, not a shared global rate. See
// ratelimiter.redis.go for the distributed variant, and
// docs/plan/grnoti-plan.md §1.1 for why the two need different backends
// rather than both building on grcache.Cache.
//
// Unlike the reference implementation, this type's RateLimiter interface
// does not expose Reserve() *rate.Reservation — that leaked a
// golang.org/x/time/rate-specific type through the interface, which the
// Redis-backed variant has no equivalent for (see
// docs/plan/grnoti-plan.md §3.4).
type localRateLimiter struct {
	limiter *rate.Limiter

	mu             sync.RWMutex
	requestsPerSec int
	burstSize      int
	allowedCount   int64
	blockedCount   int64
	waitCount      int64
	lastAllowedAt  time.Time
}

var _ RateLimiter = (*localRateLimiter)(nil)

// NewLocalRateLimiter constructs a per-process RateLimiter.
//
// Parameters:
//   - requestsPerSecond: int — must be > 0
//   - burstSize: int — must be >= requestsPerSecond
//
// Returns:
//   - RateLimiter
//   - error: non-nil if either constraint is violated
func NewLocalRateLimiter(requestsPerSecond, burstSize int) (RateLimiter, error) {
	if requestsPerSecond <= 0 {
		return nil, fmt.Errorf("grnoti: requestsPerSecond must be > 0")
	}
	if burstSize < requestsPerSecond {
		return nil, fmt.Errorf("grnoti: burstSize must be >= requestsPerSecond")
	}
	return &localRateLimiter{
		limiter:        rate.NewLimiter(rate.Limit(requestsPerSecond), burstSize),
		requestsPerSec: requestsPerSecond,
		burstSize:      burstSize,
	}, nil
}

func (r *localRateLimiter) Allow(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	allowed := r.limiter.Allow()
	r.mu.Lock()
	if allowed {
		r.allowedCount++
		r.lastAllowedAt = time.Now()
	} else {
		r.blockedCount++
	}
	r.mu.Unlock()
	return allowed, nil
}

func (r *localRateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	r.waitCount++
	r.mu.Unlock()

	if err := r.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("grnoti: rate limiter wait failed: %w", err)
	}

	r.mu.Lock()
	r.allowedCount++
	r.lastAllowedAt = time.Now()
	r.mu.Unlock()
	return nil
}

func (r *localRateLimiter) GetStats(context.Context) (RateLimiterStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return RateLimiterStats{
		RequestsPerSecond: r.requestsPerSec,
		BurstSize:         r.burstSize,
		AllowedCount:      r.allowedCount,
		BlockedCount:      r.blockedCount,
		WaitCount:         r.waitCount,
		LastAllowedAt:     r.lastAllowedAt,
	}, nil
}

// UpdateLimit adjusts the limiter's rate/burst at runtime.
func (r *localRateLimiter) UpdateLimit(requestsPerSecond, burstSize int) error {
	if requestsPerSecond <= 0 {
		return fmt.Errorf("grnoti: requestsPerSecond must be > 0")
	}
	if burstSize < requestsPerSecond {
		return fmt.Errorf("grnoti: burstSize must be >= requestsPerSecond")
	}
	r.limiter.SetLimit(rate.Limit(requestsPerSecond))
	r.limiter.SetBurst(burstSize)
	r.mu.Lock()
	r.requestsPerSec = requestsPerSecond
	r.burstSize = burstSize
	r.mu.Unlock()
	return nil
}
