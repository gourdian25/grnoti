// File: dlq.mongo_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestMongoDLQHandler(t *testing.T, retryDelay time.Duration) DLQHandler {
	t.Helper()
	h, err := NewMongoDLQHandler(MongoDLQHandlerConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: fmt.Sprintf("dlq_%d", time.Now().UnixNano()),
		MaxRetries: 3, RetryDelay: retryDelay, MaxRetryDelay: time.Second,
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestNewMongoDLQHandler_DefaultsMaxRetries(t *testing.T) {
	h, err := NewMongoDLQHandler(MongoDLQHandlerConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: fmt.Sprintf("dlq_%d", time.Now().UnixNano()),
		MaxRetries: 0,
	})
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	defer h.Close()
	if got := h.(*mongoDLQHandler).maxRetries; got != 3 {
		t.Fatalf("NewMongoDLQHandler(MaxRetries<=0).maxRetries = %d, want 3 (the default)", got)
	}
}

func TestMongoDLQHandler_ClaimRetryableEvents_DefaultsLimit(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()
	if err := h.PublishToDLQ(ctx, Event{EventID: "e-limit"}, "boom"); err != nil {
		t.Fatalf("PublishToDLQ: %v", err)
	}
	claimed, err := h.ClaimRetryableEvents(ctx, 0) // <=0 -> defaults to 10
	if err != nil {
		t.Fatalf("ClaimRetryableEvents: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimRetryableEvents(limit=0) = %v, want 1 claimed event (default limit applied)", claimed)
	}
}

func TestNewMongoDLQHandler_EmptyURI(t *testing.T) {
	_, err := NewMongoDLQHandler(MongoDLQHandlerConfig{Database: "grnoti_test"})
	if err == nil {
		t.Fatal("NewMongoDLQHandler(empty URI) = nil error, want non-nil")
	}
}

func TestNewMongoDLQHandler_EmptyDatabase(t *testing.T) {
	_, err := NewMongoDLQHandler(MongoDLQHandlerConfig{URI: testMongoURI})
	if err == nil {
		t.Fatal("NewMongoDLQHandler(empty Database) = nil error, want non-nil")
	}
}

func TestMongoDLQHandler_PublishAndClaim(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()

	if err := h.PublishToDLQ(ctx, Event{EventID: "e1"}, "fcm unavailable"); err != nil {
		t.Fatalf("PublishToDLQ: %v", err)
	}

	claimed, err := h.ClaimRetryableEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimRetryableEvents: %v", err)
	}
	if len(claimed) != 1 || claimed[0].EventID != "e1" {
		t.Fatalf("ClaimRetryableEvents() = %v, want [e1]", claimed)
	}
	if claimed[0].Status != DLQStatusRetrying {
		t.Fatalf("claimed status = %s, want %s", claimed[0].Status, DLQStatusRetrying)
	}

	againClaimed, err := h.ClaimRetryableEvents(ctx, 10)
	if err != nil || len(againClaimed) != 0 {
		t.Fatalf("second ClaimRetryableEvents = (%v, %v), want empty (already claimed)", againClaimed, err)
	}
}

func TestMongoDLQHandler_PublishToDLQ_DuplicateAppendsHistory(t *testing.T) {
	h := newTestMongoDLQHandler(t, time.Hour) // long delay so it stays pending, not claimable
	ctx := context.Background()

	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "first failure")
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "second failure")

	got, err := h.GetEventByID(ctx, "e1")
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if len(got.AttemptHistory) != 2 {
		t.Fatalf("AttemptHistory length = %d, want 2", len(got.AttemptHistory))
	}
	if got.FailureReason != "second failure" {
		t.Fatalf("FailureReason = %q, want %q (most recent)", got.FailureReason, "second failure")
	}
	if got.RetryCount != 0 {
		t.Fatalf("RetryCount = %d, want 0 (PublishToDLQ never increments it, only MarkRetried does)", got.RetryCount)
	}
}

func TestMongoDLQHandler_MarkRetried_RequiresClaim(t *testing.T) {
	h := newTestMongoDLQHandler(t, time.Hour)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")

	if err := h.MarkRetried(ctx, "e1", true, nil); err != ErrDLQEventNotClaimed {
		t.Fatalf("MarkRetried(unclaimed) error = %v, want ErrDLQEventNotClaimed", err)
	}
}

func TestMongoDLQHandler_MarkRetried_NotFound(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	if err := h.MarkRetried(context.Background(), "never-existed", true, nil); err != ErrDLQEventNotFound {
		t.Fatalf("MarkRetried(nonexistent) error = %v, want ErrDLQEventNotFound", err)
	}
}

func TestMongoDLQHandler_MarkRetried_Success(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "e1", true, nil); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, err := h.GetEventByID(ctx, "e1")
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

func TestMongoDLQHandler_MarkRetried_ExhaustsAfterMaxRetries(t *testing.T) {
	h, err := NewMongoDLQHandler(MongoDLQHandlerConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: fmt.Sprintf("dlq_%d", time.Now().UnixNano()),
		MaxRetries: 1, RetryDelay: 0, MaxRetryDelay: time.Second,
	})
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "e1", false, errors.New("still failing")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "e1")
	if got.Status != DLQStatusExhausted {
		t.Fatalf("Status = %s, want %s", got.Status, DLQStatusExhausted)
	}
}

func TestMongoDLQHandler_MarkRetried_GoesBackToPending(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "e1", false, errors.New("retry me")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "e1")
	if got.Status != DLQStatusPending {
		t.Fatalf("Status = %s, want %s", got.Status, DLQStatusPending)
	}
	if got.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1", got.RetryCount)
	}
}

func TestMongoDLQHandler_PurgeExpiredEvents(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "resolved"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)
	_ = h.MarkRetried(ctx, "resolved", true, nil)

	_ = h.PublishToDLQ(ctx, Event{EventID: "still-pending"}, "boom")

	purged, err := h.PurgeExpiredEvents(ctx, time.Hour)
	if err != nil {
		t.Fatalf("PurgeExpiredEvents: %v", err)
	}
	if purged != 1 {
		t.Fatalf("PurgeExpiredEvents() = %d, want 1", purged)
	}
}

// TestMongoDLQHandler_ConcurrentClaimNeverDoubleClaims proves the atomic
// per-document claim design against a real MongoDB instance (docs/plan/
// grnoti-plan.md §1.3, §5) — not just the in-memory backend's mutex.
func TestMongoDLQHandler_ConcurrentClaimNeverDoubleClaims(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx := context.Background()

	const numEvents = 100
	for i := 0; i < numEvents; i++ {
		_ = h.PublishToDLQ(ctx, Event{EventID: fmt.Sprintf("evt-%d", i)}, "boom")
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

func TestMongoDLQHandler_Close_Idempotent(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if _, err := h.GetEventByID(context.Background(), "e1"); err != ErrClosed {
		t.Fatalf("GetEventByID after Close error = %v, want ErrClosed", err)
	}
}

// TestMongoDLQHandler_GenericQueryError uses an already-canceled context
// to force a real query-level error — see the analogous
// tokenstore.mongo_test.go comment for why this reaches a branch fault
// injection would otherwise require.
func TestMongoDLQHandler_GenericQueryError(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom"); err == nil {
		t.Error("PublishToDLQ(canceled ctx) = nil error, want non-nil")
	}
	if _, err := h.ClaimRetryableEvents(ctx, 10); err == nil {
		t.Error("ClaimRetryableEvents(canceled ctx) = nil error, want non-nil")
	}
	if _, err := h.GetEventByID(ctx, "e1"); err == nil {
		t.Error("GetEventByID(canceled ctx) = nil error, want non-nil")
	}
	if _, err := h.PurgeExpiredEvents(ctx, time.Hour); err == nil {
		t.Error("PurgeExpiredEvents(canceled ctx) = nil error, want non-nil")
	}
	if err := h.MarkRetried(ctx, "e1", true, nil); err == nil {
		t.Error("MarkRetried(canceled ctx) = nil error, want non-nil")
	}
}

func TestMongoDLQHandler_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	h := newTestMongoDLQHandler(t, 0)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if err := h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom"); err != ErrClosed {
		t.Errorf("PublishToDLQ after Close = %v, want ErrClosed", err)
	}
	if _, err := h.ClaimRetryableEvents(ctx, 10); err != ErrClosed {
		t.Errorf("ClaimRetryableEvents after Close = %v, want ErrClosed", err)
	}
	if err := h.MarkRetried(ctx, "e1", true, nil); err != ErrClosed {
		t.Errorf("MarkRetried after Close = %v, want ErrClosed", err)
	}
	if _, err := h.PurgeExpiredEvents(ctx, time.Hour); err != ErrClosed {
		t.Errorf("PurgeExpiredEvents after Close = %v, want ErrClosed", err)
	}
}
