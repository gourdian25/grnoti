// File: workerpool.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrWorkerPoolFull is returned by WorkerPool.Submit/SubmitAsync when the
// queue is full.
var ErrWorkerPoolFull = errors.New("grnoti: worker pool queue is full")

// WorkerPool is the bridge between event ingestion (EventConsumer) and
// event processing (NotificationService.ProcessEvent), decoupling the two
// via a bounded, non-blocking queue. Unlike the reference implementation
// (see docs/plan/grnoti-plan.md §3.1), grnoti's EventConsumer and
// NotificationService are wired through a WorkerPool by default — an
// ingestion handler that calls ProcessEvent directly, with no queue in
// between, is exactly the gap this type exists to close.
//
// WorkerPool is deliberately a concrete type, not an interface — it has
// exactly one implementation and no swappable backend, matching
// CircuitBreaker/RateLimiter's (local)/RetryStrategy's own treatment.
type WorkerPool struct {
	workers int
	queue   chan Event
	handler func(context.Context, Event) error
	logger  Logger
	metrics Metrics
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// WorkerPoolDeps configures a WorkerPool.
type WorkerPoolDeps struct {
	Config WorkerPoolConfig
	// Handler processes one Event — typically a thin wrapper around
	// NotificationService.ProcessEvent. Required.
	Handler func(context.Context, Event) error
	Logger  Logger
	// Metrics is optional; if set, IncEventsSkipped("backpressure") is
	// called whenever Submit/SubmitAsync rejects an Event for a full queue.
	Metrics Metrics
}

// NewWorkerPool constructs a WorkerPool. Call Start to begin processing and
// Stop (or Close) to shut down.
//
// Parameters:
//   - deps: WorkerPoolDeps — deps.Handler must be non-nil;
//     deps.Config.Workers defaults to 10 if <= 0, deps.Config.QueueSize
//     defaults to 1000 if <= 0
//
// Returns:
//   - *WorkerPool
//   - error: non-nil if deps.Handler is nil
func NewWorkerPool(deps WorkerPoolDeps) (*WorkerPool, error) {
	if deps.Handler == nil {
		return nil, errors.New("grnoti: WorkerPoolDeps.Handler is required")
	}
	workers := deps.Config.Workers
	if workers <= 0 {
		workers = 10
	}
	queueSize := deps.Config.QueueSize
	if queueSize <= 0 {
		queueSize = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		workers: workers,
		queue:   make(chan Event, queueSize),
		handler: deps.Handler,
		logger:  OrNop(deps.Logger),
		metrics: deps.Metrics,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start spawns the pool's worker goroutines. Safe to call at most once.
func (wp *WorkerPool) Start() {
	wp.logger.Infof("grnoti: worker pool starting with %d workers", wp.workers)
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker is one pool goroutine's dispatch loop: pull an event off the
// shared queue and run it through handler, exiting once the queue channel
// is closed and fully drained. The ctx.Done() case exists only as a
// defensive exit for a future caller that cancels wp.ctx from outside
// Stop — under Stop's current close-drain-then-cancel ordering (see
// Stop's own doc comment), wp.ctx is never canceled until every worker
// has already returned via the closed-queue case, so ctx.Done() does not
// fire in practice today. A handler error is logged, not retried or
// returned; retry policy belongs to handler itself (see
// NewNotificationService's use of this pool, which wraps processEvent).
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for {
		select {
		case event, ok := <-wp.queue:
			if !ok {
				return
			}
			if err := wp.handler(wp.ctx, event); err != nil {
				wp.logger.Errorf("grnoti: worker %d: processing event %s failed: %v", id, event.EventID, err)
			}
		case <-wp.ctx.Done():
			return
		}
	}
}

// Submit enqueues event without blocking. Returns ErrWorkerPoolFull
// (wrapped with occupancy detail) if the queue is full — this is
// backpressure-by-rejection, not backpressure-by-blocking; callers that
// need to block should retry with their own backoff.
func (wp *WorkerPool) Submit(event Event) error {
	select {
	case wp.queue <- event:
		return nil
	default:
		wp.logger.Warnf("grnoti: worker pool queue full (%d/%d), rejecting event %s", len(wp.queue), cap(wp.queue), event.EventID)
		if wp.metrics != nil {
			wp.metrics.IncEventsSkipped("backpressure")
		}
		return fmt.Errorf("%w: %d/%d", ErrWorkerPoolFull, len(wp.queue), cap(wp.queue))
	}
}

// SubmitAsync is Submit with a bool result instead of an error — true if
// enqueued, false if the queue was full.
func (wp *WorkerPool) SubmitAsync(event Event) bool {
	return wp.Submit(event) == nil
}

// Stop closes the queue, waits for every worker to fully drain it and
// exit, and only then cancels wp.ctx. This ordering is what makes drain
// deterministic: while the queue is closing but wp.ctx is not yet
// canceled, ctx.Done() is never ready, so each worker's select has
// exactly one viable case — read the queue until it reports empty-and-
// closed. Stop therefore guarantees every event successfully Submitted
// before Stop was called is delivered to a worker and run through
// handler before Stop returns. Canceling only after every worker has
// exited also means handler still runs with a live (non-canceled) ctx
// for every event drained during shutdown, rather than racing an
// already-canceled one.
func (wp *WorkerPool) Stop() {
	close(wp.queue)
	wp.wg.Wait()
	wp.cancel()
	wp.logger.Infof("grnoti: worker pool stopped")
}

// GetStats returns a point-in-time snapshot of the pool's queue occupancy.
func (wp *WorkerPool) GetStats() WorkerPoolStats {
	queued := len(wp.queue)
	capacity := cap(wp.queue)
	usage := 0.0
	if capacity > 0 {
		usage = float64(queued) / float64(capacity)
	}
	return WorkerPoolStats{
		Workers:      wp.workers,
		QueueSize:    capacity,
		QueuedEvents: queued,
		QueueUsage:   usage,
	}
}
