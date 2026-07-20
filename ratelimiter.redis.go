// File: ratelimiter.redis.go

package grnoti

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	redisRateLimiterDialTimeout  = 5 * time.Second
	redisRateLimiterReadTimeout  = 3 * time.Second
	redisRateLimiterWriteTimeout = 3 * time.Second
	redisRateLimiterPingTimeout  = 5 * time.Second
	redisRateLimiterPoolSize     = 100 // matches grcache/redis's own gourdiantoken-derived default

	// redisRateLimiterKeyTTL bounds how long an idle bucket's Redis hash
	// survives. Set well above any plausible refill window so a bucket
	// under active use never expires mid-burst; a bucket that goes idle
	// for this long simply starts full again next time, which is correct
	// token-bucket behavior anyway.
	redisRateLimiterKeyTTL = 10 * time.Minute

	// redisRateLimiterWaitPollInterval is how often Wait retries Allow
	// while blocked. go-redis has no server-side blocking primitive for a
	// Lua script the way a plain BLPOP would give one, so Wait polls.
	redisRateLimiterWaitPollInterval = 20 * time.Millisecond
)

// tokenBucketScript atomically evaluates and updates a token bucket stored
// as a Redis hash {tokens, updated_at}. Running the refill-then-consume
// logic as one Lua script (rather than separate GET/refill-math/SET calls
// from the Go client) is what makes concurrent callers across N replicas
// see a single consistent bucket instead of a classic read-modify-write
// race — the entire reason this file exists instead of just reusing
// localRateLimiter's golang.org/x/time/rate against a shared key.
//
// KEYS[1] = bucket key
// ARGV[1] = capacity (burst size)
// ARGV[2] = refill rate, tokens/second
// ARGV[3] = now, unix seconds as a float
// ARGV[4] = key TTL, seconds
//
// Returns 1 if a token was consumed (allowed), 0 otherwise.
var tokenBucketScript = goredis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket = redis.call("HMGET", key, "tokens", "updated_at")
local tokens = tonumber(bucket[1])
local updatedAt = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    updatedAt = now
end

local elapsed = now - updatedAt
if elapsed < 0 then
    elapsed = 0
end
tokens = math.min(capacity, tokens + elapsed * refillRate)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call("HMSET", key, "tokens", tostring(tokens), "updated_at", tostring(now))
redis.call("EXPIRE", key, ttl)

return allowed
`)

// RedisRateLimiterConfig configures a redisRateLimiter constructed by
// NewRedisRateLimiter. Zero-valued connection fields fall back to the same
// defaults as grcache/redis's RedisConfig; RequestsPerSecond and BurstSize
// have no sensible zero value and must be set explicitly.
type RedisRateLimiterConfig struct {
	// Addr is the Redis server address, e.g. "localhost:6379". Required.
	Addr string
	// Password authenticates with the server. Empty means no auth.
	Password string
	// DB selects the Redis logical database.
	DB int
	// PoolSize is the maximum number of connections in the pool. Defaults to 100.
	PoolSize int
	// DialTimeout bounds how long connecting to Redis may take. Defaults to 5s.
	DialTimeout time.Duration
	// ReadTimeout bounds how long a read may take. Defaults to 3s.
	ReadTimeout time.Duration
	// WriteTimeout bounds how long a write may take. Defaults to 3s.
	WriteTimeout time.Duration

	// RequestsPerSecond is the bucket's steady-state refill rate, shared
	// across every process using the same Key. Required, must be > 0.
	RequestsPerSecond int
	// BurstSize is the bucket's capacity. Required, must be >= RequestsPerSecond.
	BurstSize int
	// Key identifies the shared bucket. All processes that should share
	// one distributed quota must use the same Key. Defaults to
	// "grnoti:ratelimit:default".
	Key string

	// Logger receives optional diagnostic messages. A nil Logger disables logging.
	Logger Logger
}

func (cfg RedisRateLimiterConfig) withDefaults() RedisRateLimiterConfig {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = redisRateLimiterPoolSize
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = redisRateLimiterDialTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = redisRateLimiterReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = redisRateLimiterWriteTimeout
	}
	if cfg.Key == "" {
		cfg.Key = "grnoti:ratelimit:default"
	}
	return cfg
}

// redisRateLimiter is a distributed token bucket backed by a raw
// *redis.Client — the RateLimiter this service should actually run with
// under more than one replica, since localRateLimiter enforces its quota
// per-process only (N replicas each independently enforcing the full rate).
// See docs/plan/grnoti-plan.md §1.1.
//
// GetStats reports counters observed by this instance only: Allow/Wait
// calls made through other processes sharing the same Key are invisible to
// it, since the authoritative token count lives in Redis, not locally.
type redisRateLimiter struct {
	client *goredis.Client
	logger Logger
	key    string

	mu             sync.RWMutex
	requestsPerSec int
	burstSize      int

	closed    atomic.Bool
	closeOnce sync.Once

	allowedCount  atomic.Int64
	blockedCount  atomic.Int64
	waitCount     atomic.Int64
	lastAllowedAt atomic.Int64 // unix nanoseconds; 0 means unset
}

var _ RateLimiter = (*redisRateLimiter)(nil)

// NewRedisRateLimiter builds its own *redis.Client from cfg and validates
// connectivity with a Ping before returning, mirroring grcache/redis's
// constructor-time validation.
//
// Parameters:
//   - cfg: RedisRateLimiterConfig — Addr, RequestsPerSecond, and BurstSize
//     are required; other fields default (see field docs)
//
// Returns:
//   - RateLimiter: ready to use, shared across every process using the
//     same cfg.Addr/cfg.Key pair
//   - error: non-nil if a required field is invalid or the connection fails
func NewRedisRateLimiter(cfg RedisRateLimiterConfig) (RateLimiter, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("grnoti: RedisRateLimiterConfig.Addr is required")
	}
	if cfg.RequestsPerSecond <= 0 {
		return nil, fmt.Errorf("grnoti: RequestsPerSecond must be > 0")
	}
	if cfg.BurstSize < cfg.RequestsPerSecond {
		return nil, fmt.Errorf("grnoti: BurstSize must be >= RequestsPerSecond")
	}
	cfg = cfg.withDefaults()
	logger := OrNop(cfg.Logger)

	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), redisRateLimiterPingTimeout)
	defer cancel()
	if _, err := client.Ping(ctx).Result(); err != nil {
		_ = client.Close()
		logger.Errorf("grnoti: redis rate limiter connect %s failed: %v", cfg.Addr, err)
		return nil, fmt.Errorf("grnoti: connect %s: %w", cfg.Addr, ErrBackendUnavailable)
	}

	logger.Infof("grnoti: redis rate limiter connected to %s (key %q)", cfg.Addr, cfg.Key)
	r := &redisRateLimiter{
		client:         client,
		logger:         logger,
		key:            cfg.Key,
		requestsPerSec: cfg.RequestsPerSecond,
		burstSize:      cfg.BurstSize,
	}
	return r, nil
}

func (r *redisRateLimiter) Allow(ctx context.Context) (bool, error) {
	if r.closed.Load() {
		return false, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	r.mu.RLock()
	rate, burst := r.requestsPerSec, r.burstSize
	r.mu.RUnlock()

	now := float64(time.Now().UnixNano()) / 1e9
	res, err := tokenBucketScript.Run(ctx, r.client, []string{r.key}, burst, rate, now, int(redisRateLimiterKeyTTL.Seconds())).Result()
	if err != nil {
		return false, fmt.Errorf("grnoti: redis rate limiter eval: %w", ErrBackendUnavailable)
	}

	allowed := res.(int64) == 1
	if allowed {
		r.allowedCount.Add(1)
		r.lastAllowedAt.Store(time.Now().UnixNano())
	} else {
		r.blockedCount.Add(1)
	}
	return allowed, nil
}

// Wait polls Allow at redisRateLimiterWaitPollInterval until a token is
// available or ctx is done. Redis has no server-side blocking primitive
// for a Lua-scripted token bucket the way BLPOP gives a plain list, so
// unlike localRateLimiter's Wait (which defers to golang.org/x/time/rate's
// own timer-based Wait), this one polls.
func (r *redisRateLimiter) Wait(ctx context.Context) error {
	if r.closed.Load() {
		return ErrClosed
	}
	r.waitCount.Add(1)

	ticker := time.NewTicker(redisRateLimiterWaitPollInterval)
	defer ticker.Stop()
	for {
		allowed, err := r.Allow(ctx)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *redisRateLimiter) GetStats(ctx context.Context) (RateLimiterStats, error) {
	if r.closed.Load() {
		return RateLimiterStats{}, ErrClosed
	}
	r.mu.RLock()
	rate, burst := r.requestsPerSec, r.burstSize
	r.mu.RUnlock()

	stats := RateLimiterStats{
		RequestsPerSecond: rate,
		BurstSize:         burst,
		AllowedCount:      r.allowedCount.Load(),
		BlockedCount:      r.blockedCount.Load(),
		WaitCount:         r.waitCount.Load(),
	}
	if ns := r.lastAllowedAt.Load(); ns != 0 {
		stats.LastAllowedAt = time.Unix(0, ns)
	}
	return stats, nil
}

// UpdateLimit adjusts the shared bucket's rate/capacity at runtime. Since
// capacity/rate are passed as script arguments on every call rather than
// stored server-side, this takes effect for this process's subsequent
// calls immediately; other processes sharing the same Key keep using
// whatever they were last configured with until they call UpdateLimit too
// — there is no cross-process config propagation.
func (r *redisRateLimiter) UpdateLimit(requestsPerSecond, burstSize int) error {
	if requestsPerSecond <= 0 {
		return fmt.Errorf("grnoti: requestsPerSecond must be > 0")
	}
	if burstSize < requestsPerSecond {
		return fmt.Errorf("grnoti: burstSize must be >= requestsPerSecond")
	}
	r.mu.Lock()
	r.requestsPerSec = requestsPerSecond
	r.burstSize = burstSize
	r.mu.Unlock()
	return nil
}

// Close closes the underlying *redis.Client, guarded by sync.Once since
// go-redis errors on a double Close.
func (r *redisRateLimiter) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		err = r.client.Close()
		r.logger.Infof("grnoti: redis rate limiter closed")
	})
	return err
}
