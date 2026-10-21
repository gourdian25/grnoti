// File: types_test.go

package grnoti

import (
	"strings"
	"testing"
	"time"
)

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

	t.Run("FIDsAreAValidTarget", func(t *testing.T) {
		e := base
		e.UserID = ""
		e.FIDs = []string{testFID("fid-1")}
		if err := e.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil (FIDs alone is a valid target)", err)
		}
	})

	t.Run("MalformedFID", func(t *testing.T) {
		e := base
		e.FIDs = []string{testFID("fid-1"), "not-an-fid"}
		if err := e.Validate(); err != ErrInvalidFID {
			t.Fatalf("Validate() = %v, want ErrInvalidFID", err)
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

	direct := Event{FIDs: []string{"t1"}}
	if !direct.HasDirectFIDs() || direct.GetTargetID() != "direct" {
		t.Fatalf("direct-FID event helpers wrong: %+v", direct)
	}
}

// legacyRegistrationToken has the shape of an FCM registration token as the
// deprecated client token APIs hand one out: an installation ID, a colon
// and a long APA91b... credential.
const legacyRegistrationToken = "dIsVQ2QVRT-TW7L6VfeAMh:APA91bHPRgkF3JUikC4ENAHEeMrd41Zxv3hVZjC9KtT8OvPVGJ-hQMRKRrZuJAEcl7B338qju59zJMknoScH12ZtE6Gg5NXsvjUsaJvL8CGLZ0zlBumbzCEWVaN7Ma0V3ssq6tMcbe3U"

func TestValidFID(t *testing.T) {
	tests := []struct {
		name string
		fid  string
		want bool
	}{
		{"Real", "dIsVQ2QVRT-TW7L6VfeAMh", true},
		{"EveryHeaderCharacter", "c" + strings.Repeat("A", 21), true},
		{"URLSafeSymbols", "f" + strings.Repeat("_", 10) + strings.Repeat("-", 11), true},
		{"Empty", "", false},
		{"LegacyRegistrationToken", legacyRegistrationToken, false},
		{"WrongHeader", "a" + strings.Repeat("A", 21), false},
		{"TooShort", "c" + strings.Repeat("A", 20), false},
		{"TooLong", "c" + strings.Repeat("A", 22), false},
		{"StandardBase64Symbol", "c" + strings.Repeat("A", 20) + "+", false},
		{"Padding", "c" + strings.Repeat("A", 20) + "=", false},
		{"TrailingNewline", "c" + strings.Repeat("A", 21) + "\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validFID(tt.fid); got != tt.want {
				t.Fatalf("validFID(%q) = %v, want %v", tt.fid, got, tt.want)
			}
		})
	}
}

func TestDevice_Validate(t *testing.T) {
	if err := (Device{FID: "dIsVQ2QVRT-TW7L6VfeAMh"}).Validate(); err != nil {
		t.Fatalf("Validate(valid FID) = %v, want nil", err)
	}
	if err := (Device{FID: legacyRegistrationToken}).Validate(); err != ErrInvalidFID {
		t.Fatalf("Validate(registration token) = %v, want ErrInvalidFID", err)
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

func TestPriority_String(t *testing.T) {
	if PriorityHigh.String() != "high" {
		t.Fatalf("PriorityHigh.String() = %q, want %q", PriorityHigh.String(), "high")
	}
}

func TestPlatform_String(t *testing.T) {
	if PlatformIOS.String() != "ios" {
		t.Fatalf("PlatformIOS.String() = %q, want %q", PlatformIOS.String(), "ios")
	}
}

func TestDefaultServiceConfig(t *testing.T) {
	cfg := DefaultServiceConfig()
	if cfg.IdempotencyTTL != 24*time.Hour {
		t.Fatalf("DefaultServiceConfig().IdempotencyTTL = %v, want 24h", cfg.IdempotencyTTL)
	}
	if cfg.MaxDevicesPerBatch != 500 {
		t.Fatalf("DefaultServiceConfig().MaxDevicesPerBatch = %d, want 500", cfg.MaxDevicesPerBatch)
	}
	if !cfg.EnableMetrics {
		t.Fatal("DefaultServiceConfig().EnableMetrics = false, want true")
	}
	if !cfg.EnableDLQ {
		t.Fatal("DefaultServiceConfig().EnableDLQ = false, want true")
	}
	if cfg.EnableTopicRouting || cfg.EnableBackpressure || cfg.EnableABTesting {
		t.Fatalf("DefaultServiceConfig() = %+v, want every other Enable* flag false (opt-in)", cfg)
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
