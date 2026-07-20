// File: types_test.go

package grnoti

import "testing"

func TestEvent_Validate(t *testing.T) {
	base := Event{
		EventID:  "evt-1",
		UserID:   "user-1",
		Type:     EventTypeSystemAlert,
		Priority: PriorityNormal,
	}

	if err := base.Validate(); err != nil {
		t.Fatalf("Validate(valid event) = %v, want nil", err)
	}

	t.Run("MissingEventID", func(t *testing.T) {
		e := base
		e.EventID = ""
		if err := e.Validate(); err != ErrInvalidEventID {
			t.Fatalf("Validate() = %v, want ErrInvalidEventID", err)
		}
	})

	t.Run("NoTarget", func(t *testing.T) {
		e := base
		e.UserID = ""
		if err := e.Validate(); err != ErrNoTargetSpecified {
			t.Fatalf("Validate() = %v, want ErrNoTargetSpecified", err)
		}
	})

	t.Run("AnonymousIDIsAValidTarget", func(t *testing.T) {
		e := base
		e.UserID = ""
		e.AnonymousID = "anon-1"
		if err := e.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil (AnonymousID alone is a valid target)", err)
		}
	})

	t.Run("DeviceTokensIsAValidTarget", func(t *testing.T) {
		e := base
		e.UserID = ""
		e.DeviceTokens = []string{"tok-1"}
		if err := e.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil (DeviceTokens alone is a valid target)", err)
		}
	})

	t.Run("InvalidEventType", func(t *testing.T) {
		e := base
		e.Type = ""
		if err := e.Validate(); err != ErrInvalidEventType {
			t.Fatalf("Validate() = %v, want ErrInvalidEventType", err)
		}
	})

	t.Run("InvalidPriority", func(t *testing.T) {
		e := base
		e.Priority = "urgent"
		if err := e.Validate(); err != ErrInvalidPriority {
			t.Fatalf("Validate() = %v, want ErrInvalidPriority", err)
		}
	})
}

func TestEvent_TargetHelpers(t *testing.T) {
	auth := Event{UserID: "u1"}
	if !auth.IsAuthenticated() || auth.IsAnonymous() || auth.GetTargetID() != "u1" {
		t.Fatalf("authenticated event helpers wrong: %+v", auth)
	}

	anon := Event{AnonymousID: "a1"}
	if anon.IsAuthenticated() || !anon.IsAnonymous() || anon.GetTargetID() != "a1" {
		t.Fatalf("anonymous event helpers wrong: %+v", anon)
	}

	direct := Event{DeviceTokens: []string{"t1"}}
	if !direct.HasDirectTokens() || direct.GetTargetID() != "direct" {
		t.Fatalf("direct-token event helpers wrong: %+v", direct)
	}
}

func TestPriority_IsValid(t *testing.T) {
	for _, p := range []Priority{PriorityHigh, PriorityNormal, PriorityLow} {
		if !p.IsValid() {
			t.Errorf("Priority(%s).IsValid() = false, want true", p)
		}
	}
	if Priority("urgent").IsValid() {
		t.Error(`Priority("urgent").IsValid() = true, want false`)
	}
}

func TestPlatform_IsValid(t *testing.T) {
	for _, p := range []Platform{PlatformAndroid, PlatformIOS, PlatformWeb} {
		if !p.IsValid() {
			t.Errorf("Platform(%s).IsValid() = false, want true", p)
		}
	}
	if Platform("blackberry").IsValid() {
		t.Error(`Platform("blackberry").IsValid() = true, want false`)
	}
}

func TestDispatchResult_Helpers(t *testing.T) {
	r := DispatchResult{SuccessCount: 3, FailureCount: 2}
	if r.TotalCount() != 5 {
		t.Errorf("TotalCount() = %d, want 5", r.TotalCount())
	}
	if !r.HasFailures() {
		t.Error("HasFailures() = false, want true")
	}
	if (DispatchResult{SuccessCount: 1}).HasFailures() {
		t.Error("HasFailures() = true, want false")
	}
}
