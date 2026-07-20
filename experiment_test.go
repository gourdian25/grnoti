// File: experiment_test.go

package grnoti

import (
	"context"
	"sync"
	"testing"
)

func TestDeterministicExperimentEngine_AssignVariant_Deterministic(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	experiment := &Experiment{
		ID: "exp-1",
		Variants: []ExperimentVariant{
			{ID: "control", Weight: 50},
			{ID: "treatment", Weight: 50},
		},
	}

	first, err := engine.AssignVariant(context.Background(), "user-1", experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}

	for i := 0; i < 20; i++ {
		got, err := engine.AssignVariant(context.Background(), "user-1", experiment)
		if err != nil {
			t.Fatalf("AssignVariant (repeat %d): %v", i, err)
		}
		if got.ID != first.ID {
			t.Fatalf("AssignVariant not deterministic: first=%s, repeat %d=%s", first.ID, i, got.ID)
		}
	}
}

func TestDeterministicExperimentEngine_AssignVariant_NoVariants(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	_, err := engine.AssignVariant(context.Background(), "user-1", &Experiment{ID: "empty"})
	if err != ErrExperimentHasNoVariants {
		t.Fatalf("AssignVariant(no variants) error = %v, want ErrExperimentHasNoVariants", err)
	}
}

func TestDeterministicExperimentEngine_GetVariant_BeforeAssignment(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	v, err := engine.GetVariant(context.Background(), "user-1", "never-assigned")
	if err != nil {
		t.Fatalf("GetVariant: %v", err)
	}
	if v != nil {
		t.Fatalf("GetVariant before assignment = %+v, want nil", v)
	}
}

func TestDeterministicExperimentEngine_GetVariant_AfterAssignment(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	experiment := &Experiment{ID: "exp-1", Variants: []ExperimentVariant{{ID: "only", Weight: 1}}}

	assigned, err := engine.AssignVariant(context.Background(), "user-1", experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}

	got, err := engine.GetVariant(context.Background(), "user-1", "exp-1")
	if err != nil {
		t.Fatalf("GetVariant: %v", err)
	}
	if got == nil || got.ID != assigned.ID {
		t.Fatalf("GetVariant after assignment = %+v, want %+v", got, assigned)
	}
}

// TestDeterministicExperimentEngine_ConcurrentAssignVariant is the
// falsifying test for the reference implementation's confirmed data race
// (docs/plan/grnoti-plan.md §2 item 2): many goroutines calling
// AssignVariant/GetVariant for overlapping users on one engine, run under
// -race. It must both not race and converge on one consistent assignment
// per user.
func TestDeterministicExperimentEngine_ConcurrentAssignVariant(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	experiment := &Experiment{
		ID: "exp-stress",
		Variants: []ExperimentVariant{
			{ID: "a", Weight: 1},
			{ID: "b", Weight: 1},
			{ID: "c", Weight: 1},
		},
	}

	const users = 20
	const goroutinesPerUser = 25

	var wg sync.WaitGroup
	results := make([][]string, users)
	var resultsMu sync.Mutex
	for u := 0; u < users; u++ {
		results[u] = make([]string, 0, goroutinesPerUser)
	}

	for u := 0; u < users; u++ {
		userID := assignmentKey("user", string(rune('A'+u)))
		for g := 0; g < goroutinesPerUser; g++ {
			wg.Add(1)
			go func(userID string, u int) {
				defer wg.Done()
				variant, err := engine.AssignVariant(context.Background(), userID, experiment)
				if err != nil {
					t.Errorf("AssignVariant(%s): %v", userID, err)
					return
				}
				resultsMu.Lock()
				results[u] = append(results[u], variant.ID)
				resultsMu.Unlock()
			}(userID, u)
		}
	}
	wg.Wait()

	for u := 0; u < users; u++ {
		if len(results[u]) == 0 {
			continue
		}
		want := results[u][0]
		for _, got := range results[u] {
			if got != want {
				t.Fatalf("user %d got inconsistent variants across concurrent calls: %v", u, results[u])
			}
		}
	}
}

func TestDeterministicExperimentEngine_TrackImpression_NoPublisher(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	if err := engine.TrackImpression(context.Background(), "user-1", "exp-1", "control"); err != nil {
		t.Fatalf("TrackImpression with no publisher: %v", err)
	}
}

type stubAnalyticsPublisher struct {
	impressions int
	conversions int
}

func (s *stubAnalyticsPublisher) PublishImpression(context.Context, string, string, string) error {
	s.impressions++
	return nil
}
func (s *stubAnalyticsPublisher) PublishConversion(context.Context, string, string) error {
	s.conversions++
	return nil
}
func (s *stubAnalyticsPublisher) Close() error { return nil }

func TestDeterministicExperimentEngine_TrackImpression_WithPublisher(t *testing.T) {
	pub := &stubAnalyticsPublisher{}
	engine := NewDeterministicExperimentEngine(pub, nil, nil)

	if err := engine.TrackImpression(context.Background(), "user-1", "exp-1", "control"); err != nil {
		t.Fatalf("TrackImpression: %v", err)
	}
	if err := engine.TrackConversion(context.Background(), "user-1", "exp-1"); err != nil {
		t.Fatalf("TrackConversion: %v", err)
	}
	if pub.impressions != 1 || pub.conversions != 1 {
		t.Fatalf("publisher counts = (%d, %d), want (1, 1)", pub.impressions, pub.conversions)
	}
}

func TestDeterministicExperimentEngine_AssignVariant_PublishesOnceOnNewAssignment(t *testing.T) {
	bus := &stubBus{}
	engine := NewDeterministicExperimentEngine(nil, bus, nil)
	experiment := &Experiment{ID: "exp-1", Variants: []ExperimentVariant{{ID: "only", Weight: 1}}}

	assigned, err := engine.AssignVariant(context.Background(), "user-1", experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}

	// A repeat AssignVariant for the same (user, experiment) must NOT
	// publish again — only the genuinely new assignment does.
	for i := 0; i < 3; i++ {
		if _, err := engine.AssignVariant(context.Background(), "user-1", experiment); err != nil {
			t.Fatalf("AssignVariant (repeat %d): %v", i, err)
		}
	}

	events := bus.publishedEvents()
	if len(events) != 1 {
		t.Fatalf("Publish call count = %d, want exactly 1 (only the first, new assignment)", len(events))
	}
	payload, ok := events[0].Payload.(ExperimentAssignedPayload)
	if !ok || payload.UserID != "user-1" || payload.ExperimentID != "exp-1" || payload.VariantID != assigned.ID {
		t.Fatalf("Payload = %+v (ok=%v), want UserID=user-1 ExperimentID=exp-1 VariantID=%s", payload, ok, assigned.ID)
	}
}

func TestDeterministicExperimentEngine_AssignVariant_NilBusIsNoOp(t *testing.T) {
	engine := NewDeterministicExperimentEngine(nil, nil, nil)
	experiment := &Experiment{ID: "exp-1", Variants: []ExperimentVariant{{ID: "only", Weight: 1}}}
	if _, err := engine.AssignVariant(context.Background(), "user-1", experiment); err != nil {
		t.Fatalf("AssignVariant with nil bus: %v", err)
	}
}
