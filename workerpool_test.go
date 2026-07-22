// File: workerpool_test.go

package grnoti

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_ProcessesSubmittedEvents(t *testing.T) {
	var processed atomic.Int64
	pool, err := NewWorkerPool(WorkerPoolDeps{
		Config: WorkerPoolConfig{Workers: 2, QueueSize: 10},
		Handler: func(context.Context, Event) error {
			processed.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start()
	defer pool.Stop()

	for i := 0; i < 5; i++ {
		if err := pool.Submit(Event{EventID: "e"}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	deadline := time.Now().Add(time.Second)
	for processed.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := processed.Load(); got != 5 {
		t.Fatalf("processed = %d, want 5", got)
	}
}

func TestWorkerPool_RejectsWhenFull(t *testing.T) {
	block := make(chan struct{})
	pool, err := NewWorkerPool(WorkerPoolDeps{
		Config: WorkerPoolConfig{Workers: 1, QueueSize: 1},
		Handler: func(context.Context, Event) error {
			<-block
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start()
	defer func() {
		close(block)
		pool.Stop()
	}()

	// First Submit is picked up by the single worker (which then blocks on
	// <-block), second fills the size-1 queue, third must be rejected.
	if err := pool.Submit(Event{EventID: "1"}); err != nil {
		t.Fatalf("Submit(1): %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the worker pick up event 1
	if err := pool.Submit(Event{EventID: "2"}); err != nil {
		t.Fatalf("Submit(2): %v", err)
	}
	if err := pool.Submit(Event{EventID: "3"}); err == nil {
		t.Fatal("Submit(3) on full queue = nil error, want ErrWorkerPoolFull")
	}
	if ok := pool.SubmitAsync(Event{EventID: "4"}); ok {
		t.Fatal("SubmitAsync(4) on full queue = true, want false")
	}
}

// TestWorkerPool_StopDrains is the falsifying regression test for a
// cancel-before-close race in Stop: canceling wp.ctx before closing the
// queue let a worker's select pseudo-randomly exit via ctx.Done() instead
// of draining a full, non-empty buffered queue, silently dropping
// already-Submitted events. Mirrors
// TestDeterministicExperimentEngine_ConcurrentAssignVariant's pattern of
// looping many iterations inside one test function, so a reintroduced
// race is caught reliably within a single `go test -race` invocation
// instead of depending on an external -count=N.
func TestWorkerPool_StopDrains(t *testing.T) {
	const iterations = 200
	for iter := 0; iter < iterations; iter++ {
		var processed atomic.Int64
		pool, err := NewWorkerPool(WorkerPoolDeps{
			Config: WorkerPoolConfig{Workers: 3, QueueSize: 100},
			Handler: func(context.Context, Event) error {
				processed.Add(1)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("NewWorkerPool: %v", err)
		}
		pool.Start()

		for i := 0; i < 50; i++ {
			_ = pool.Submit(Event{EventID: "e"})
		}
		pool.Stop()

		if got := processed.Load(); got != 50 {
			t.Fatalf("iteration %d: processed = %d after Stop, want 50 (queue must drain fully)", iter, got)
		}
	}
}

func TestWorkerPool_GetStats(t *testing.T) {
	block := make(chan struct{})
	pool, _ := NewWorkerPool(WorkerPoolDeps{
		Config: WorkerPoolConfig{Workers: 1, QueueSize: 10},
		Handler: func(context.Context, Event) error {
			<-block
			return nil
		},
	})
	pool.Start()
	defer func() {
		close(block)
		pool.Stop()
	}()

	_ = pool.Submit(Event{EventID: "1"})
	time.Sleep(20 * time.Millisecond)
	_ = pool.Submit(Event{EventID: "2"})
	_ = pool.Submit(Event{EventID: "3"})

	stats := pool.GetStats()
	if stats.Workers != 1 || stats.QueueSize != 10 {
		t.Fatalf("GetStats() = %+v, want Workers=1 QueueSize=10", stats)
	}
	if stats.QueuedEvents < 1 {
		t.Fatalf("GetStats().QueuedEvents = %d, want >= 1", stats.QueuedEvents)
	}
}

func TestWorkerPool_MissingHandler(t *testing.T) {
	if _, err := NewWorkerPool(WorkerPoolDeps{}); err == nil {
		t.Fatal("NewWorkerPool with nil Handler = nil error, want non-nil")
	}
}

// TestWorkerPool_ConcurrentSubmit proves Submit is safe under concurrent
// callers, matching the -race obligation for this layer.
func TestWorkerPool_ConcurrentSubmit(t *testing.T) {
	var processed atomic.Int64
	pool, _ := NewWorkerPool(WorkerPoolDeps{
		Config: WorkerPoolConfig{Workers: 4, QueueSize: 200},
		Handler: func(context.Context, Event) error {
			processed.Add(1)
			return nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pool.Submit(Event{EventID: "e"})
		}()
	}
	wg.Wait()
}
