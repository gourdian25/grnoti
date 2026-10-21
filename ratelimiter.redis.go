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
	redisRateLimiterPoolSize     = 100 // matches grcache's Redis backend default (itself gourdiantoken-derived)

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
// "now" is read once server-side via redis.call("TIME"), NOT passed in as
// a client-supplied argument — deliberately: this script is evaluated by
// every replica of a multi-process deployment (the entire point of this
// backend over localRateLimiter), and those replicas' wall clocks are not
// guaranteed to agree. A replica with a fast clock computing its own
// elapsed-time-since-last-refill would grant itself a larger refill than a
// replica with an accurate clock, silently corrupting the shared bucket —
// exactly the correctness property a distributed rate limiter exists to
// provide. Reading Redis's own clock makes every caller agree on "now" by
// construction, at the cost of one extra redis.call inside the script
// (still one network round trip total — TIME runs server-side). Mirrors
// grpop's twoTierTokenBucketScript, which establishes this same pattern.
//
// KEYS[1] = bucket key
// ARGV[1] = capacity (burst size)
// ARGV[2] = refill rate, tokens/second
// ARGV[3] = key TTL, seconds
//
// Returns 1 if a token was consumed (allowed), 0 otherwise.
var tokenBucketScript = goredis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local t = redis.call("TIME")
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

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
// NewRedisRateLimiter. Exactly one of Addr or Client must be set: with
// Addr the limiter dials (and owns) its own client, and zero-valued
// connection fields fall back to the same defaults as grcache.RedisConfig;
// with Client it uses the caller's client as given and every connection
// field is ignored. RequestsPerSecond and BurstSize have no sensible zero
// value and must be set explicitly either way.
type RedisRateLimiterConfig struct {
	// Addr is the Redis server address, e.g. "localhost:6379". Exactly one
	// of Addr or Client must be set.
	Addr string
	// Client, if set, is used as given instead of dialing a new client
	// from Addr, so the limiter can share one connection pool with the
	// rest of your application. NewRedisRateLimiter only Pings it. grnoti
	// never closes a Client it did not create: Close leaves it open, and
	// so does a failed Ping. Any redis.UniversalClient works, Cluster and
	// Ring included, since tokenBucketScript touches a single key (unlike
	// grcache's Redis backend, which needs one MULTI/EXEC across keys).
	// Exactly one of Addr or Client must be set.
	Client goredis.UniversalClient
	// Password authenticates with the server. Empty means no auth. Ignored
	// when Client is set.
	Password string
	// DB selects the Redis logical database. Ignored when Client is set.
	DB int
	// PoolSize is the maximum number of connections in the pool. Defaults
	// to 100. Ignored when Client is set — size the client's pool yourself
	// before passing it in.
	PoolSize int
	// DialTimeout bounds how long connecting to Redis may take. Defaults to
	// 5s. Ignored when Client is set.
	DialTimeout time.Duration
	// ReadTimeout bounds how long a read may take. Defaults to 3s. Ignored
	// when Client is set.
	ReadTimeout time.Duration
	// WriteTimeout bounds how long a write may take. Defaults to 3s.
	// Ignored when Client is set.
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

// redisRateLimiter is a distributed token bucket backed by a raw go-redis
// client, its own or an injected one — the RateLimiter this service should
// actually run with
// under more than one replica, since localRateLimiter enforces its quota
// per-process only (N replicas each independently enforcing the full rate).
// See docs/plan/grnoti-plan.md §1.1.
//
// GetStats reports counters observed by this instance only: Allow/Wait
// calls made through other processes sharing the same Key are invisible to
// it, since the authoritative token count lives in Redis, not locally.
type redisRateLimiter struct {
	client goredis.UniversalClient
	// ownsClient is true only when NewRedisRateLimiter dialed client
	// itself from Addr; Close closes the client only then. A client
	// injected through RedisRateLimiterConfig.Client belongs to the caller.
	ownsClient bool
	logger     Logger
	key        string

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

// NewRedisRateLimiter validates connectivity with a Ping before returning,
// mirroring grcache.NewRedisCache's constructor-time validation. With
// cfg.Addr it builds its own *redis.Client, which Close later closes; with
// cfg.Client it uses that client as given and never closes it — the same
// ownership rule as PostgresConfig.Pool.
//
// Parameters:
//   - cfg: RedisRateLimiterConfig — exactly one of Addr or Client, plus
//     RequestsPerSecond and BurstSize, are required; other fields default
//     (see field docs)
//
// Returns:
//   - RateLimiter: ready to use, shared across every process using the
//     same Redis server and cfg.Key
//   - error: non-nil if Addr and Client are both set or both empty, a
//     required field is invalid, or the connection/Ping fails (wrapping
//     ErrBackendUnavailable)
func NewRedisRateLimiter(cfg RedisRateLimiterConfig) (RateLimiter, error) {
	if (cfg.Addr == "") == (cfg.Client == nil) {
		return nil, fmt.Errorf("grnoti: exactly one of RedisRateLimiterConfig.Addr or RedisRateLimiterConfig.Client is required")
	}
	if cfg.RequestsPerSecond <= 0 {
		return nil, fmt.Errorf("grnoti: RequestsPerSecond must be > 0")
	}
	if cfg.BurstSize < cfg.RequestsPerSecond {
		return nil, fmt.Errorf("grnoti: BurstSize must be >= RequestsPerSecond")
	}
	cfg = cfg.withDefaults()
	logger := OrNop(cfg.Logger)

	client, ownsClient := cfg.Client, false
	if client == nil {
		client = goredis.NewClient(&goredis.Options{
			Addr:         cfg.Addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
		ownsClient = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisRateLimiterPingTimeout)
	defer cancel()
	if _, err := client.Ping(ctx).Result(); err != nil {
		if !ownsClient {
			// The injected client is the caller's: report the failure,
			// never close it.
			logger.Error("grnoti: redis rate limiter injected client ping failed", "error", err)
			return nil, fmt.Errorf("grnoti: ping injected redis client: %w", ErrBackendUnavailable)
		}
		_ = client.Close()
		logger.Error("grnoti: redis rate limiter connect failed", "addr", cfg.Addr, "error", err)
		return nil, fmt.Errorf("grnoti: connect %s: %w", cfg.Addr, ErrBackendUnavailable)
	}

	if ownsClient {
		logger.Info("grnoti: redis rate limiter connected", "addr", cfg.Addr, "key", cfg.Key)
	} else {
		logger.Info("grnoti: redis rate limiter using injected client", "key", cfg.Key)
	}
	r := &redisRateLimiter{
		client:         client,
		ownsClient:     ownsClient,
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

	res, err := tokenBucketScript.Run(ctx, r.client, []string{r.key}, burst, rate, int(redisRateLimiterKeyTTL.Seconds())).Result()
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
	if r.closed.Load() {
		return ErrClosed
	}
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

// Close closes the underlying client only if NewRedisRateLimiter created
// it (from RedisRateLimiterConfig.Addr); a client injected through
// RedisRateLimiterConfig.Client is left open for its owner to close.
// Guarded by sync.Once since go-redis errors on a double Close.
func (r *redisRateLimiter) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		if r.ownsClient {
			err = r.client.Close()
		}
		r.logger.Info("grnoti: redis rate limiter closed")
	})
	return err
}
