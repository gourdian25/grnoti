// File: retrystrategy.go

package grnoti

import (
	"errors"
	"math/rand"
	"time"
)

// defaultMaxBackoff is the safety ceiling applied by FullJitterBackoff
// whenever a caller passes max<=0.
const defaultMaxBackoff = 30 * time.Second

// FullJitterBackoff returns a randomized backoff duration for the given
// 0-indexed attempt that just failed: sleep = random(0, min(cap,
// base*2^attempt)) — the AWS "Full Jitter" formula. This mirrors
// grevents' retry.go computeBackoff exactly (see
// docs/plan/grnoti-plan.md §1.2): grevents was the first backoff-with-jitter
// implementation in the gourdian ecosystem, and this is the second,
// deliberately kept identical rather than inventing a second formula. It is
// exported (unlike grevents' own unexported computeBackoff) so both
// retrystrategy.go's FCM-dispatch retry and the Postgres/Mongo DLQ
// backends' own retry-delay computation share one implementation instead
// of two independently-written copies of the same formula.
//
// Parameters:
//   - base: time.Duration — the starting point; base<=0 returns 0
//     (no backoff)
//   - max: time.Duration — the ceiling; max<=0 defaults to
//     defaultMaxBackoff
//   - attempt: int — 0-indexed attempt number
//
// Returns:
//   - time.Duration: a value in [0, min(max, base*2^attempt)]
func FullJitterBackoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	ceiling := max
	if ceiling <= 0 {
		ceiling = defaultMaxBackoff
	}

	exp := ceiling
	if attempt >= 0 && attempt < 62 { // 1<<62 already exceeds any sane base*factor; avoid signed-shift overflow beyond this
		if scaled := base * (1 << uint(attempt)); scaled > 0 && scaled < ceiling { //nolint:gosec // attempt is bounded to [0,62) on this branch
			exp = scaled
		}
	}

	return time.Duration(rand.Int63n(int64(exp) + 1)) //nolint:gosec // backoff jitter has no cryptographic requirement
}

type fullJitterRetry struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

var _ RetryStrategy = (*fullJitterRetry)(nil)

// NewFullJitterRetry constructs a RetryStrategy for FCM dispatch retries,
// backed by FullJitterBackoff. Replaces the reference implementation's
// un-jittered base*2^attempt strategy (see docs/plan/grnoti-plan.md §1.2)
// — a synchronized retry storm across replicas after an FCM outage is
// exactly what jitter exists to avoid.
//
// Parameters:
//   - maxAttempts: int — total attempts, including the first; ShouldRetry
//     returns false once this many attempts have been made
//   - baseDelay, maxDelay: time.Duration — passed through to
//     FullJitterBackoff
func NewFullJitterRetry(maxAttempts int, baseDelay, maxDelay time.Duration) RetryStrategy {
	return &fullJitterRetry{maxAttempts: maxAttempts, baseDelay: baseDelay, maxDelay: maxDelay}
}

func (r *fullJitterRetry) ShouldRetry(attempt int, err error) bool {
	if err == nil || attempt >= r.maxAttempts {
		return false
	}
	var fcmErr *FCMError
	if errors.As(err, &fcmErr) {
		return fcmErr.IsRetryable()
	}
	// An error that isn't a classified FCMError (e.g. a network timeout
	// surfaced directly from the FCM client) is treated as retryable by
	// default — the reference implementation treated any non-FCMError as
	// non-retryable, which silently gave up on plain transient errors
	// (see docs/plan/grnoti-plan.md's research notes on retry.strategy.go).
	return true
}

func (r *fullJitterRetry) GetDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return FullJitterBackoff(r.baseDelay, r.maxDelay, attempt)
}

type noopRetryStrategy struct{}

var _ RetryStrategy = noopRetryStrategy{}

// NewNoopRetryStrategy returns a RetryStrategy that never retries, for
// tests and for dispatchers explicitly configured without retry.
func NewNoopRetryStrategy() RetryStrategy { return noopRetryStrategy{} }

func (noopRetryStrategy) ShouldRetry(int, error) bool { return false }
func (noopRetryStrategy) GetDelay(int) time.Duration  { return 0 }
