// File: circuitbreaker.go

package grnoti

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by CircuitBreaker.Execute when the breaker is
// open and its Timeout has not yet elapsed.
var ErrCircuitOpen = errors.New("grnoti: circuit breaker is open")

// ErrTooManyRequests is returned by CircuitBreaker.Execute when the breaker
// is half-open and MaxHalfOpenRequests trial requests are already in
// flight.
var ErrTooManyRequests = errors.New("grnoti: too many requests while circuit breaker is half-open")

type standardCircuitBreaker struct {
	config CircuitBreakerConfig
	logger Logger

	mu                  sync.RWMutex
	state               CircuitState
	consecutiveFailures int
	halfOpenRequests    int
	lastFailureTime     time.Time
	lastStateChange     time.Time
	openedAt            time.Time
	totalSuccesses      int64
	totalFailures       int64
	totalRejections     int64
}

var _ CircuitBreaker = (*standardCircuitBreaker)(nil)

// NewCircuitBreaker constructs a CircuitBreaker with MaxHalfOpenRequests
// fixed at 1.
//
// Parameters:
//   - maxFailures: int — consecutive failures before opening; must be > 0
//   - timeout: time.Duration — how long to stay open before allowing a
//     trial request; must be > 0
//   - resetTimeout: time.Duration — how long a closed breaker must go
//     without a failure before its consecutive-failure counter resets;
//     must be > 0
//
// Returns:
//   - CircuitBreaker
//   - error: non-nil if any parameter is not positive
func NewCircuitBreaker(maxFailures int, timeout, resetTimeout time.Duration) (CircuitBreaker, error) {
	return NewCircuitBreakerWithConfig(CircuitBreakerConfig{
		MaxFailures:         maxFailures,
		Timeout:             timeout,
		ResetTimeout:        resetTimeout,
		MaxHalfOpenRequests: 1,
	})
}

// NewCircuitBreakerWithConfig constructs a CircuitBreaker from a full
// CircuitBreakerConfig.
//
// Parameters:
//   - config: CircuitBreakerConfig — MaxFailures/Timeout/ResetTimeout must
//     each be > 0; MaxHalfOpenRequests defaults to 1 if <= 0
//
// Returns:
//   - CircuitBreaker
//   - error: non-nil if MaxFailures/Timeout/ResetTimeout is not positive
func NewCircuitBreakerWithConfig(config CircuitBreakerConfig) (CircuitBreaker, error) {
	if config.MaxFailures <= 0 {
		return nil, errors.New("grnoti: CircuitBreakerConfig.MaxFailures must be > 0")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("grnoti: CircuitBreakerConfig.Timeout must be > 0")
	}
	if config.ResetTimeout <= 0 {
		return nil, errors.New("grnoti: CircuitBreakerConfig.ResetTimeout must be > 0")
	}
	if config.MaxHalfOpenRequests <= 0 {
		config.MaxHalfOpenRequests = 1
	}
	return &standardCircuitBreaker{
		config:          config,
		logger:          OrNop(config.Logger),
		state:           CircuitStateClosed,
		lastStateChange: time.Now(),
	}, nil
}

func (cb *standardCircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := cb.beforeRequest(); err != nil {
		return err
	}
	err := fn()
	cb.afterRequest(err)
	return err
}

// beforeRequest decides whether a request may proceed, and performs any
// state transition that decision implies. The Open-to-HalfOpen transition
// and the immediately-following HalfOpen admission check are one
// fallthrough case rather than two separate branches so that the request
// which discovers Timeout has elapsed is itself immediately treated as the
// (first) half-open trial request, instead of being rejected once more and
// forcing a second caller to arrive before any trial happens at all.
func (cb *standardCircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	switch cb.state {
	case CircuitStateClosed:
		if now.Sub(cb.lastFailureTime) > cb.config.ResetTimeout {
			cb.consecutiveFailures = 0
		}
		return nil
	case CircuitStateOpen:
		if now.Sub(cb.openedAt) >= cb.config.Timeout {
			cb.state = CircuitStateHalfOpen
			cb.halfOpenRequests = 0
			cb.lastStateChange = now
			cb.logger.Info("grnoti: circuit breaker half-open, allowing trial request")
		} else {
			cb.totalRejections++
			return ErrCircuitOpen
		}
		fallthrough
	case CircuitStateHalfOpen:
		if cb.halfOpenRequests >= cb.config.MaxHalfOpenRequests {
			cb.totalRejections++
			return ErrTooManyRequests
		}
		cb.halfOpenRequests++
		return nil
	default:
		return nil
	}
}

// afterRequest records a completed request's outcome and applies the one
// state transition it can trigger: a half-open failure reopens the circuit
// immediately (a single trial failure is enough — see beforeRequest's
// MaxHalfOpenRequests admission check for why more than one trial can be
// in flight at once), a half-open success closes it, and a closed-state
// success resets the consecutive-failure counter so isolated failures
// don't accumulate toward MaxFailures across unrelated incidents.
func (cb *standardCircuitBreaker) afterRequest(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if err != nil {
		cb.totalFailures++
		cb.consecutiveFailures++
		cb.lastFailureTime = now
		switch cb.state {
		case CircuitStateHalfOpen:
			cb.openCircuit(now)
		case CircuitStateClosed:
			if cb.consecutiveFailures >= cb.config.MaxFailures {
				cb.openCircuit(now)
			}
		}
		return
	}

	cb.totalSuccesses++
	switch cb.state {
	case CircuitStateHalfOpen:
		cb.closeCircuit(now)
	case CircuitStateClosed:
		cb.consecutiveFailures = 0
	}
}

func (cb *standardCircuitBreaker) openCircuit(now time.Time) {
	cb.state = CircuitStateOpen
	cb.openedAt = now
	cb.lastStateChange = now
	cb.halfOpenRequests = 0
	cb.logger.Warn("grnoti: circuit breaker opened", "consecutive_failures", cb.consecutiveFailures)
}

func (cb *standardCircuitBreaker) closeCircuit(now time.Time) {
	cb.state = CircuitStateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenRequests = 0
	cb.lastStateChange = now
	cb.logger.Info("grnoti: circuit breaker closed")
}

func (cb *standardCircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *standardCircuitBreaker) GetStats() CircuitBreakerStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	stats := CircuitBreakerStats{
		State:               cb.state,
		ConsecutiveFailures: cb.consecutiveFailures,
		TotalSuccesses:      cb.totalSuccesses,
		TotalFailures:       cb.totalFailures,
		TotalRejections:     cb.totalRejections,
		LastFailureTime:     cb.lastFailureTime,
		LastStateChange:     cb.lastStateChange,
		OpenedAt:            cb.openedAt,
	}
	if cb.state == CircuitStateOpen {
		elapsed := time.Since(cb.openedAt)
		if elapsed < cb.config.Timeout {
			stats.TimeUntilNextAttempt = cb.config.Timeout - elapsed
		}
	}
	return stats
}

func (cb *standardCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitStateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenRequests = 0
	cb.lastStateChange = time.Now()
}
