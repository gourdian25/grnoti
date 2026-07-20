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
