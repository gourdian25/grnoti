// File: dlq.postgres_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestPostgresDLQHandler(t *testing.T, maxRetries int, retryDelay time.Duration) DLQHandler {
	t.Helper()
	h, err := NewPostgresDLQHandler(PostgresDLQHandlerConfig{
		PostgresConfig: PostgresConfig{DSN: testPostgresDSN},
		MaxRetries:     maxRetries, RetryDelay: retryDelay, MaxRetryDelay: time.Second,
	})
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	t.Cleanup(func() {
		if hh, ok := h.(*postgresDLQHandler); ok {
			hh.pool.Exec(context.Background(), "DELETE FROM grnoti_dlq")
		}
		_ = h.Close()
	})
	return h
}

func TestPostgresDLQHandler_PublishAndClaim(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, 0)
	ctx := context.Background()

	if err := h.PublishToDLQ(ctx, Event{EventID: "pge1"}, "fcm unavailable"); err != nil {
		t.Fatalf("PublishToDLQ: %v", err)
	}

	claimed, err := h.ClaimRetryableEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimRetryableEvents: %v", err)
	}
	if len(claimed) != 1 || claimed[0].EventID != "pge1" {
		t.Fatalf("ClaimRetryableEvents() = %v, want [pge1]", claimed)
	}
	if claimed[0].Status != DLQStatusRetrying {
		t.Fatalf("claimed status = %s, want %s", claimed[0].Status, DLQStatusRetrying)
	}

	again, err := h.ClaimRetryableEvents(ctx, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("second ClaimRetryableEvents = (%v, %v), want empty", again, err)
	}
}

func TestPostgresDLQHandler_PublishToDLQ_DuplicateAppendsHistory(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, time.Hour)
	ctx := context.Background()

	_ = h.PublishToDLQ(ctx, Event{EventID: "pge2"}, "first failure")
	_ = h.PublishToDLQ(ctx, Event{EventID: "pge2"}, "second failure")

	got, err := h.GetEventByID(ctx, "pge2")
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if len(got.AttemptHistory) != 2 {
		t.Fatalf("AttemptHistory length = %d, want 2", len(got.AttemptHistory))
	}
	if got.FailureReason != "second failure" {
		t.Fatalf("FailureReason = %q, want second failure", got.FailureReason)
	}
	if got.RetryCount != 0 {
		t.Fatalf("RetryCount = %d, want 0", got.RetryCount)
	}
}

func TestPostgresDLQHandler_MarkRetried_RequiresClaim(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, time.Hour)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "pge3"}, "boom")

	if err := h.MarkRetried(ctx, "pge3", true, nil); err != ErrDLQEventNotClaimed {
		t.Fatalf("MarkRetried(unclaimed) error = %v, want ErrDLQEventNotClaimed", err)
	}
}

func TestPostgresDLQHandler_MarkRetried_NotFound(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, 0)
	if err := h.MarkRetried(context.Background(), "never-existed-pg", true, nil); err != ErrDLQEventNotFound {
		t.Fatalf("MarkRetried(nonexistent) error = %v, want ErrDLQEventNotFound", err)
	}
}

func TestPostgresDLQHandler_MarkRetried_Success(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "pge4"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "pge4", true, nil); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, err := h.GetEventByID(ctx, "pge4")
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if got.Status != DLQStatusResolved {
		t.Fatalf("Status = %s, want %s", got.Status, DLQStatusResolved)
	}
	if got.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1", got.RetryCount)
	}
}

func TestPostgresDLQHandler_MarkRetried_ExhaustsAfterMaxRetries(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 1, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "pge5"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "pge5", false, errors.New("still failing")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "pge5")
	if got.Status != DLQStatusExhausted {
		t.Fatalf("Status = %s, want %s", got.Status, DLQStatusExhausted)
	}
}

func TestPostgresDLQHandler_MarkRetried_GoesBackToPending(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 5, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "pge6"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "pge6", false, errors.New("retry me")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "pge6")
	if got.Status != DLQStatusPending {
		t.Fatalf("Status = %s, want %s", got.Status, DLQStatusPending)
	}
	if got.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1", got.RetryCount)
	}
	// retryDelay=0 (see newTestPostgresDLQHandler's second argument here)
	// makes FullJitterBackoff legitimately return 0 by design — "0 base
	// means no backoff," already covered by TestFullJitterBackoff_ZeroBase
	// — so NextRetryAt is not asserted to be strictly in the future here,
	// only that the finalize step actually set it (non-zero).
	if got.NextRetryAt.IsZero() {
		t.Fatal("NextRetryAt was never set by the pending-finalize path")
	}
}

func TestPostgresDLQHandler_PurgeExpiredEvents(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 1, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "pg-resolved"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)
	_ = h.MarkRetried(ctx, "pg-resolved", true, nil)

	_ = h.PublishToDLQ(ctx, Event{EventID: "pg-still-pending"}, "boom")

	purged, err := h.PurgeExpiredEvents(ctx, time.Hour)
	if err != nil {
		t.Fatalf("PurgeExpiredEvents: %v", err)
	}
	if purged != 1 {
		t.Fatalf("PurgeExpiredEvents() = %d, want 1", purged)
	}
}

// TestPostgresDLQHandler_ConcurrentClaimNeverDoubleClaims proves the FOR
// UPDATE SKIP LOCKED claim design against a real Postgres instance — the
// central correctness guarantee this backend exists to provide (docs/plan/
// grnoti-plan.md §1.3, §5).
func TestPostgresDLQHandler_ConcurrentClaimNeverDoubleClaims(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, 0)
	ctx := context.Background()

	const numEvents = 100
	for i := 0; i < numEvents; i++ {
		_ = h.PublishToDLQ(ctx, Event{EventID: fmt.Sprintf("pg-evt-%d", i)}, "boom")
	}

	const numWorkers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimedIDs := make(map[string]int)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := h.ClaimRetryableEvents(ctx, 20)
			if err != nil {
				t.Errorf("ClaimRetryableEvents: %v", err)
				return
			}
			mu.Lock()
			for _, e := range claimed {
				claimedIDs[e.EventID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for id, count := range claimedIDs {
		if count > 1 {
			t.Fatalf("event %s was claimed %d times, want at most 1", id, count)
		}
	}
	if len(claimedIDs) != numEvents {
		t.Fatalf("claimed %d distinct events across all workers, want %d", len(claimedIDs), numEvents)
	}
}

func TestPostgresDLQHandler_Close_Idempotent(t *testing.T) {
	h := newTestPostgresDLQHandler(t, 3, 0)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if _, err := h.GetEventByID(context.Background(), "pge1"); err != ErrClosed {
		t.Fatalf("GetEventByID after Close error = %v, want ErrClosed", err)
	}
}
