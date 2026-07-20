// File: memory_test.go

package grnoti

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryTokenStore_SaveAndGet(t *testing.T) {
	store := NewMemoryTokenStore()
	ctx := context.Background()

	if err := store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1", Platform: PlatformAndroid}); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}

	tokens, err := store.GetActiveTokens(ctx, "u1")
	if err != nil {
		t.Fatalf("GetActiveTokens: %v", err)
	}
	if len(tokens) != 1 || tokens[0].Token != "t1" {
		t.Fatalf("GetActiveTokens() = %v, want [t1]", tokens)
	}
}

func TestMemoryTokenStore_MarkInvalid(t *testing.T) {
	store := NewMemoryTokenStore()
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})

	if err := store.MarkInvalid(ctx, "t1"); err != nil {
		t.Fatalf("MarkInvalid: %v", err)
	}
	tokens, _ := store.GetActiveTokens(ctx, "u1")
	if len(tokens) != 0 {
		t.Fatalf("GetActiveTokens after MarkInvalid = %v, want empty", tokens)
	}
	// Marking an already-invalid (or nonexistent) token is not an error.
	if err := store.MarkInvalid(ctx, "never-existed"); err != nil {
		t.Fatalf("MarkInvalid(nonexistent): %v", err)
	}
}

func TestMemoryTokenStore_GetActiveTokensBatch(t *testing.T) {
	store := NewMemoryTokenStore()
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})
	_ = store.SaveToken(ctx, DeviceToken{Token: "t2", UserID: "u2"})

	out, err := store.GetActiveTokensBatch(ctx, []string{"u1", "u2", "u3"})
	if err != nil {
		t.Fatalf("GetActiveTokensBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("GetActiveTokensBatch() = %v, want 2 users", out)
	}
}

func TestMemoryPreferencesStore_NotFoundThenSave(t *testing.T) {
	store := NewMemoryPreferencesStore()
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "u1"); err != ErrPreferencesNotFound {
		t.Fatalf("GetPreferences (unset) error = %v, want ErrPreferencesNotFound", err)
	}

	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1", GlobalEnabled: true}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}
	got, err := store.GetPreferences(ctx, "u1")
	if err != nil || !got.GlobalEnabled {
		t.Fatalf("GetPreferences() = (%+v, %v), want GlobalEnabled=true", got, err)
	}
}

func TestMemoryPreferencesStore_IsEventTypeEnabled_DefaultsToTrue(t *testing.T) {
	store := NewMemoryPreferencesStore()
	ctx := context.Background()
	// Unconfigured user: opted in by default.
	enabled, err := store.IsEventTypeEnabled(ctx, "never-seen", EventTypeSystemAlert)
	if err != nil || !enabled {
		t.Fatalf("IsEventTypeEnabled(unconfigured user) = (%v, %v), want (true, nil)", enabled, err)
	}
}

func TestMemoryPreferencesStore_IsEventTypeEnabled_GlobalDisable(t *testing.T) {
	store := NewMemoryPreferencesStore()
	ctx := context.Background()
	_ = store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1", GlobalEnabled: false})

	enabled, err := store.IsEventTypeEnabled(ctx, "u1", EventTypeSystemAlert)
	if err != nil || enabled {
		t.Fatalf("IsEventTypeEnabled(global disabled) = (%v, %v), want (false, nil)", enabled, err)
	}
}

func TestMemoryPreferencesStore_IsEventTypeEnabled_PerTypeOptOut(t *testing.T) {
	store := NewMemoryPreferencesStore()
	ctx := context.Background()
	_ = store.SavePreferences(ctx, &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true,
		EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
	})

	enabled, _ := store.IsEventTypeEnabled(ctx, "u1", EventTypeGenericMarketing)
	if enabled {
		t.Fatal("IsEventTypeEnabled(opted-out type) = true, want false")
	}
	enabled, _ = store.IsEventTypeEnabled(ctx, "u1", EventTypeSystemAlert)
	if !enabled {
		t.Fatal("IsEventTypeEnabled(no explicit setting) = false, want true (defaults to enabled)")
	}
}

// TestMemoryPreferencesStore_ConcurrentAccess is the falsifying test for
// the reference implementation's confirmed data race on
// InMemoryPreferencesStore (docs/plan/grnoti-plan.md §2 item 3): many
// goroutines reading and writing preferences for overlapping users, under
// -race.
func TestMemoryPreferencesStore_ConcurrentAccess(t *testing.T) {
	store := NewMemoryPreferencesStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid := "user-1"
			_ = store.SavePreferences(ctx, &NotificationPreferences{UserID: uid, GlobalEnabled: i%2 == 0})
			_, _ = store.GetPreferences(ctx, uid)
			_, _ = store.IsEventTypeEnabled(ctx, uid, EventTypeSystemAlert)
		}(i)
	}
	wg.Wait()
}

func TestMemoryDLQHandler_PublishAndClaim(t *testing.T) {
	h := NewMemoryDLQHandler(3, 0, 0) // retryDelay=0 so it's immediately claimable
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
		t.Fatalf("claimed event status = %s, want %s", claimed[0].Status, DLQStatusRetrying)
	}

	// A second claim must not re-claim the same event — it's already
	// DLQStatusRetrying, not DLQStatusPending.
	claimedAgain, err := h.ClaimRetryableEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimRetryableEvents (second call): %v", err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("ClaimRetryableEvents (second call) = %v, want empty (already claimed)", claimedAgain)
	}
}

func TestMemoryDLQHandler_MarkRetried_RequiresClaim(t *testing.T) {
	h := NewMemoryDLQHandler(3, 0, 0)
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")

	// e1 is DLQStatusPending, never claimed.
	err := h.MarkRetried(ctx, "e1", true, nil)
	if err != ErrDLQEventNotClaimed {
		t.Fatalf("MarkRetried(unclaimed event) error = %v, want ErrDLQEventNotClaimed", err)
	}
}

func TestMemoryDLQHandler_MarkRetried_Success(t *testing.T) {
	h := NewMemoryDLQHandler(3, 0, 0)
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
		t.Fatalf("Status after successful retry = %s, want %s", got.Status, DLQStatusResolved)
	}
}

func TestMemoryDLQHandler_MarkRetried_ExhaustsAfterMaxRetries(t *testing.T) {
	h := NewMemoryDLQHandler(1, 0, 0) // maxRetries=1: the first failed retry exhausts it
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "e1", false, errors.New("still failing")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "e1")
	if got.Status != DLQStatusExhausted {
		t.Fatalf("Status after exhausting retries = %s, want %s", got.Status, DLQStatusExhausted)
	}
}

func TestMemoryDLQHandler_MarkRetried_GoesBackToPending(t *testing.T) {
	h := NewMemoryDLQHandler(5, 0, time.Second) // retryDelay=0 so PublishToDLQ's event is immediately claimable
	ctx := context.Background()
	_ = h.PublishToDLQ(ctx, Event{EventID: "e1"}, "boom")
	_, _ = h.ClaimRetryableEvents(ctx, 10)

	if err := h.MarkRetried(ctx, "e1", false, errors.New("retry me")); err != nil {
		t.Fatalf("MarkRetried: %v", err)
	}
	got, _ := h.GetEventByID(ctx, "e1")
	if got.Status != DLQStatusPending {
		t.Fatalf("Status after a retryable failure = %s, want %s", got.Status, DLQStatusPending)
	}
	if got.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1", got.RetryCount)
	}
}

func TestMemoryDLQHandler_GetEventByID_NotFound(t *testing.T) {
	h := NewMemoryDLQHandler(3, 0, 0)
	if _, err := h.GetEventByID(context.Background(), "never-existed"); err != ErrDLQEventNotFound {
		t.Fatalf("GetEventByID(missing) error = %v, want ErrDLQEventNotFound", err)
	}
}

func TestMemoryDLQHandler_PurgeExpiredEvents(t *testing.T) {
	h := NewMemoryDLQHandler(1, 0, 0)
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
		t.Fatalf("PurgeExpiredEvents() = %d, want 1 (only the resolved event)", purged)
	}
	if _, err := h.GetEventByID(ctx, "still-pending"); err != nil {
		t.Fatalf("still-pending event was purged unexpectedly: %v", err)
	}
}

// TestMemoryDLQHandler_ConcurrentClaimNeverDoubleClaims is the core
// correctness proof for the atomic-claim redesign (docs/plan/grnoti-plan.md
// §1.3, §5): N workers concurrently calling ClaimRetryableEvents against
// the same pool of pending events must partition them disjointly — no
// event may ever be returned to two different callers.
func TestMemoryDLQHandler_ConcurrentClaimNeverDoubleClaims(t *testing.T) {
	h := NewMemoryDLQHandler(3, 0, 0)
	ctx := context.Background()

	const numEvents = 200
	for i := 0; i < numEvents; i++ {
		_ = h.PublishToDLQ(ctx, Event{EventID: assignmentKey("evt", string(rune(i)))}, "boom")
	}

	const numWorkers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimedIDs := make(map[string]int) // eventID -> claim count

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := h.ClaimRetryableEvents(ctx, 50)
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
}

func TestMemoryExperimentStore_CRUD(t *testing.T) {
	store := NewMemoryExperimentStore()
	ctx := context.Background()

	exp := &Experiment{ID: "exp-1", Name: "Test", Variants: []ExperimentVariant{{ID: "a"}}, Enabled: true}
	if err := store.CreateExperiment(ctx, exp); err != nil {
		t.Fatalf("CreateExperiment: %v", err)
	}

	got, err := store.GetExperiment(ctx, "exp-1")
	if err != nil || got.Name != "Test" {
		t.Fatalf("GetExperiment() = (%+v, %v), want Name=Test", got, err)
	}

	got.Name = "Updated"
	if err := store.UpdateExperiment(ctx, got); err != nil {
		t.Fatalf("UpdateExperiment: %v", err)
	}
	got2, _ := store.GetExperiment(ctx, "exp-1")
	if got2.Name != "Updated" {
		t.Fatalf("GetExperiment() after update = %+v, want Name=Updated", got2)
	}

	all, err := store.ListExperiments(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("ListExperiments() = (%v, %v), want 1 entry", all, err)
	}

	if err := store.DeleteExperiment(ctx, "exp-1"); err != nil {
		t.Fatalf("DeleteExperiment: %v", err)
	}
	if _, err := store.GetExperiment(ctx, "exp-1"); err != ErrExperimentNotFound {
		t.Fatalf("GetExperiment(deleted) error = %v, want ErrExperimentNotFound", err)
	}
}

func TestMemoryExperimentStore_UpdateNotFound(t *testing.T) {
	store := NewMemoryExperimentStore()
	err := store.UpdateExperiment(context.Background(), &Experiment{ID: "never-existed"})
	if err != ErrExperimentNotFound {
		t.Fatalf("UpdateExperiment(nonexistent) error = %v, want ErrExperimentNotFound", err)
	}
}
