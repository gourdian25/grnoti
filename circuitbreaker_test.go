// File: circuitbreaker_test.go

package grnoti

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cb, err := NewCircuitBreaker(3, 50*time.Millisecond, time.Hour)
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}

	failing := errors.New("boom")
	for i := 0; i < 3; i++ {
		_ = cb.Execute(context.Background(), func() error { return failing })
	}

	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("State() = %s, want %s", got, CircuitStateOpen)
	}

	err = cb.Execute(context.Background(), func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Execute while open error = %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreaker_HalfOpenThenCloses(t *testing.T) {
	cb, err := NewCircuitBreaker(1, 20*time.Millisecond, time.Hour)
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}

	_ = cb.Execute(context.Background(), func() error { return errors.New("boom") })
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("State() = %s, want %s", got, CircuitStateOpen)
	}

	time.Sleep(30 * time.Millisecond)

	if err := cb.Execute(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("Execute (trial request) = %v, want nil", err)
	}
	if got := cb.State(); got != CircuitStateClosed {
		t.Fatalf("State() after successful trial = %s, want %s", got, CircuitStateClosed)
	}
}

func TestCircuitBreaker_LogsStateTransitions(t *testing.T) {
	logger := &recordingLogger{}
	cb, err := NewCircuitBreakerWithConfig(CircuitBreakerConfig{
		MaxFailures:  1,
		Timeout:      20 * time.Millisecond,
		ResetTimeout: time.Hour,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("NewCircuitBreakerWithConfig: %v", err)
	}

	_ = cb.Execute(context.Background(), func() error { return errors.New("boom") })
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("State() = %s, want %s", got, CircuitStateOpen)
	}
	if got := logger.warnCount(); got == 0 {
		t.Fatal("expected at least one Warn-level log when the breaker opened, got none")
	}

	time.Sleep(30 * time.Millisecond)

	if err := cb.Execute(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("Execute (trial request) = %v, want nil", err)
	}
	if got := cb.State(); got != CircuitStateClosed {
		t.Fatalf("State() after successful trial = %s, want %s", got, CircuitStateClosed)
	}
	logger.mu.Lock()
	infoCount := len(logger.infos)
	logger.mu.Unlock()
	if infoCount == 0 {
		t.Fatal("expected at least one Info-level log across half-open/close transitions, got none")
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb, err := NewCircuitBreaker(1, 20*time.Millisecond, time.Hour)
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}
	_ = cb.Execute(context.Background(), func() error { return errors.New("boom") })
	time.Sleep(30 * time.Millisecond)

	_ = cb.Execute(context.Background(), func() error { return errors.New("boom again") })
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("State() after half-open failure = %s, want %s", got, CircuitStateOpen)
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb, _ := NewCircuitBreaker(1, time.Hour, time.Hour)
	_ = cb.Execute(context.Background(), func() error { return errors.New("boom") })
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("State() = %s, want %s", got, CircuitStateOpen)
	}
	cb.Reset()
	if got := cb.State(); got != CircuitStateClosed {
		t.Fatalf("State() after Reset = %s, want %s", got, CircuitStateClosed)
	}
}

func TestCircuitBreaker_InvalidConfig(t *testing.T) {
	if _, err := NewCircuitBreaker(0, time.Second, time.Second); err == nil {
		t.Fatal("NewCircuitBreaker(maxFailures=0) = nil error, want non-nil")
	}
	if _, err := NewCircuitBreaker(1, 0, time.Second); err == nil {
		t.Fatal("NewCircuitBreaker(timeout=0) = nil error, want non-nil")
	}
	if _, err := NewCircuitBreaker(1, time.Second, 0); err == nil {
		t.Fatal("NewCircuitBreaker(resetTimeout=0) = nil error, want non-nil")
	}
}

func TestCircuitBreaker_Execute_CanceledContext(t *testing.T) {
	cb, _ := NewCircuitBreaker(3, time.Hour, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cb.Execute(ctx, func() error { return nil }); err == nil {
		t.Fatal("Execute(canceled ctx) = nil error, want non-nil")
	}
}

func TestNewCircuitBreakerWithConfig_DefaultsMaxHalfOpenRequests(t *testing.T) {
	cb, err := NewCircuitBreakerWithConfig(CircuitBreakerConfig{
		MaxFailures: 1, Timeout: time.Hour, ResetTimeout: time.Hour, MaxHalfOpenRequests: 0,
	})
	if err != nil {
		t.Fatalf("NewCircuitBreakerWithConfig: %v", err)
	}
	scb := cb.(*standardCircuitBreaker)
	if scb.config.MaxHalfOpenRequests != 1 {
		t.Fatalf("MaxHalfOpenRequests = %d, want 1 (the default)", scb.config.MaxHalfOpenRequests)
	}
}

func TestCircuitBreaker_GetStats_OpenState(t *testing.T) {
	cb, _ := NewCircuitBreaker(1, 100*time.Millisecond, time.Hour)
	_ = cb.Execute(context.Background(), func() error { return errors.New("boom") })

	stats := cb.GetStats()
	if stats.State != CircuitStateOpen {
		t.Fatalf("GetStats().State = %s, want %s", stats.State, CircuitStateOpen)
	}
	if stats.TimeUntilNextAttempt <= 0 {
		t.Fatalf("GetStats().TimeUntilNextAttempt = %v, want > 0 while still within Timeout", stats.TimeUntilNextAttempt)
	}
	if stats.OpenedAt.IsZero() {
		t.Fatal("GetStats().OpenedAt is zero, want set once opened")
	}

	time.Sleep(150 * time.Millisecond)
	stats = cb.GetStats()
	if stats.TimeUntilNextAttempt != 0 {
		t.Fatalf("GetStats().TimeUntilNextAttempt = %v after Timeout elapsed, want 0", stats.TimeUntilNextAttempt)
	}
}

// TestCircuitBreaker_ConcurrentExecute proves the state machine's mutex
// protection under real concurrent load, not just single-threaded review.
func TestCircuitBreaker_ConcurrentExecute(t *testing.T) {
	cb, _ := NewCircuitBreakerWithConfig(CircuitBreakerConfig{
		MaxFailures: 5, Timeout: 10 * time.Millisecond, ResetTimeout: time.Hour, MaxHalfOpenRequests: 2,
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = cb.Execute(context.Background(), func() error {
				if i%2 == 0 {
					return errors.New("boom")
				}
				return nil
			})
			_ = cb.GetStats()
			_ = cb.State()
		}(i)
	}
	wg.Wait()
}
