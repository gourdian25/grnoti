// File: preferencesfilter_test.go

package grnoti

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreferencesFilter_AnonymousAlwaysAllowed(t *testing.T) {
	filter := NewPreferencesFilter(NewMemoryPreferencesStore(), nil)
	allow, reason, err := filter.ShouldSendNotification(context.Background(), Event{AnonymousID: "anon-1", Type: EventTypeSystemAlert})
	if err != nil || !allow || reason != "" {
		t.Fatalf("ShouldSendNotification(anonymous) = (%v, %q, %v), want (true, \"\", nil)", allow, reason, err)
	}
}

func TestPreferencesFilter_UnconfiguredUserAllowed(t *testing.T) {
	filter := NewPreferencesFilter(NewMemoryPreferencesStore(), nil)
	allow, _, err := filter.ShouldSendNotification(context.Background(), Event{UserID: "never-seen", Type: EventTypeSystemAlert})
	if err != nil || !allow {
		t.Fatalf("ShouldSendNotification(unconfigured user) = (%v, %v), want (true, nil)", allow, err)
	}
}

func TestPreferencesFilter_GlobalDisabled(t *testing.T) {
	store := NewMemoryPreferencesStore()
	_ = store.SavePreferences(context.Background(), &NotificationPreferences{UserID: "u1", GlobalEnabled: false})
	filter := NewPreferencesFilter(store, nil)

	allow, reason, err := filter.ShouldSendNotification(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil || allow || reason != "global_disabled" {
		t.Fatalf("ShouldSendNotification(global disabled) = (%v, %q, %v), want (false, global_disabled, nil)", allow, reason, err)
	}
}

func TestPreferencesFilter_EventTypeDisabled(t *testing.T) {
	store := NewMemoryPreferencesStore()
	_ = store.SavePreferences(context.Background(), &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true,
		EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
	})
	filter := NewPreferencesFilter(store, nil)

	allow, reason, err := filter.ShouldSendNotification(context.Background(), Event{UserID: "u1", Type: EventTypeGenericMarketing})
	if err != nil || allow || reason != "event_type_disabled" {
		t.Fatalf("ShouldSendNotification(event type disabled) = (%v, %q, %v), want (false, event_type_disabled, nil)", allow, reason, err)
	}
}

func TestPreferencesFilter_QuietHours_NonWrapping(t *testing.T) {
	store := NewMemoryPreferencesStore()
	_ = store.SavePreferences(context.Background(), &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true, QuietHoursEnabled: true,
		QuietHoursStart: "09:00", QuietHoursEnd: "17:00", Timezone: "UTC",
	})
	filter := NewPreferencesFilter(store, nil).(*preferencesFilter)

	inside := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	outside := time.Date(2026, 1, 1, 20, 0, 0, 0, time.UTC)

	prefs, _ := filter.store.GetPreferences(context.Background(), "u1")

	if in, err := isWithinQuietHours(prefs, inside); err != nil || !in {
		t.Fatalf("isWithinQuietHours(12:00, window 09:00-17:00) = (%v, %v), want (true, nil)", in, err)
	}
	if in, err := isWithinQuietHours(prefs, outside); err != nil || in {
		t.Fatalf("isWithinQuietHours(20:00, window 09:00-17:00) = (%v, %v), want (false, nil)", in, err)
	}
}

func TestPreferencesFilter_QuietHours_WrappingMidnight(t *testing.T) {
	store := NewMemoryPreferencesStore()
	_ = store.SavePreferences(context.Background(), &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true, QuietHoursEnabled: true,
		QuietHoursStart: "22:00", QuietHoursEnd: "06:00", Timezone: "UTC",
	})
	prefs, _ := store.GetPreferences(context.Background(), "u1")

	cases := []struct {
		name string
		at   time.Time
		want bool
	}{
		{"LateEvening", time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC), true},
		{"EarlyMorning", time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC), true},
		{"Midday", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), false},
		{"ExactStart", time.Date(2026, 1, 1, 22, 0, 0, 0, time.UTC), true},
		{"ExactEnd", time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := isWithinQuietHours(prefs, tc.at)
			if err != nil {
				t.Fatalf("isWithinQuietHours: %v", err)
			}
			if got != tc.want {
				t.Fatalf("isWithinQuietHours(%s) = %v, want %v", tc.at.Format("15:04"), got, tc.want)
			}
		})
	}
}

func TestPreferencesFilter_QuietHoursDisabled(t *testing.T) {
	store := NewMemoryPreferencesStore()
	_ = store.SavePreferences(context.Background(), &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true, QuietHoursEnabled: false,
		QuietHoursStart: "00:00", QuietHoursEnd: "23:59", Timezone: "UTC",
	})
	filter := NewPreferencesFilter(store, nil)
	allow, _, err := filter.ShouldSendNotification(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil || !allow {
		t.Fatalf("ShouldSendNotification(quiet hours disabled) = (%v, %v), want (true, nil)", allow, err)
	}
}

func TestPreferencesFilter_InvalidTimezoneFailsOpen(t *testing.T) {
	prefs := &NotificationPreferences{QuietHoursEnabled: true, QuietHoursStart: "09:00", QuietHoursEnd: "17:00", Timezone: "Not/A/Real/Zone"}
	_, err := isWithinQuietHours(prefs, time.Now())
	if err == nil {
		t.Fatal("isWithinQuietHours(invalid timezone) = nil error, want non-nil")
	}
}

func TestIsWithinQuietHours_InvalidStart(t *testing.T) {
	prefs := &NotificationPreferences{QuietHoursEnabled: true, QuietHoursStart: "not-a-time", QuietHoursEnd: "17:00", Timezone: "UTC"}
	if _, err := isWithinQuietHours(prefs, time.Now()); err == nil {
		t.Fatal("isWithinQuietHours(invalid start) = nil error, want non-nil")
	}
}

func TestIsWithinQuietHours_InvalidEnd(t *testing.T) {
	prefs := &NotificationPreferences{QuietHoursEnabled: true, QuietHoursStart: "09:00", QuietHoursEnd: "not-a-time", Timezone: "UTC"}
	if _, err := isWithinQuietHours(prefs, time.Now()); err == nil {
		t.Fatal("isWithinQuietHours(invalid end) = nil error, want non-nil")
	}
}

// erroringPreferencesStore always fails with a generic (non-
// ErrPreferencesNotFound) error — used to exercise
// ShouldSendNotification's fail-open branch, distinct from the
// unconfigured-user (ErrPreferencesNotFound) case already covered above.
type erroringPreferencesStore struct{ err error }

func (s erroringPreferencesStore) GetPreferences(context.Context, string) (*NotificationPreferences, error) {
	return nil, s.err
}
func (erroringPreferencesStore) SavePreferences(context.Context, *NotificationPreferences) error {
	return nil
}
func (erroringPreferencesStore) IsEventTypeEnabled(context.Context, string, EventType) (bool, error) {
	return false, nil
}
func (erroringPreferencesStore) Close() error { return nil }

func TestPreferencesFilter_GenericStoreErrorFailsOpen(t *testing.T) {
	store := erroringPreferencesStore{err: errors.New("store unavailable")}
	filter := NewPreferencesFilter(store, nil)

	allow, reason, err := filter.ShouldSendNotification(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if !allow || reason != "" {
		t.Fatalf("ShouldSendNotification(store error) = (%v, %q), want (true, \"\") — fail open", allow, reason)
	}
	if err == nil {
		t.Fatal("ShouldSendNotification(store error) error = nil, want the underlying store error surfaced for logging")
	}
}
