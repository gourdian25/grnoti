// File: preferencesfilter.go

package grnoti

import (
	"context"
	"fmt"
	"time"
)

type preferencesFilter struct {
	store  PreferencesStore
	logger Logger
}

var _ PreferencesFilter = (*preferencesFilter)(nil)

// NewPreferencesFilter constructs the default PreferencesFilter, evaluating
// global enable/disable, per-event-type opt-out, and quiet hours against
// store.
func NewPreferencesFilter(store PreferencesStore, logger Logger) PreferencesFilter {
	return &preferencesFilter{store: store, logger: OrNop(logger)}
}

func (f *preferencesFilter) ShouldSendNotification(ctx context.Context, event Event) (bool, string, error) {
	if !event.IsAuthenticated() {
		// Anonymous/direct-token targets have no PreferencesStore entry to
		// evaluate against.
		return true, "", nil
	}

	prefs, err := f.store.GetPreferences(ctx, event.UserID)
	if err != nil {
		if err == ErrPreferencesNotFound {
			return true, "", nil // unconfigured user is opted in, not opted out
		}
		// Fail open: a PreferencesStore outage should not silently drop
		// notifications for every user. The error is still returned so the
		// caller can log/alert on it.
		f.logger.Warnf("grnoti: preferences lookup for %s failed, failing open: %v", event.UserID, err)
		return true, "", err
	}

	if !prefs.GlobalEnabled {
		return false, "global_disabled", nil
	}
	if !prefs.IsEventTypeEnabled(event.Type) {
		return false, "event_type_disabled", nil
	}

	inQuietHours, qhErr := isWithinQuietHours(prefs, time.Now())
	if qhErr != nil {
		f.logger.Warnf("grnoti: quiet-hours evaluation for %s failed, ignoring quiet hours: %v", event.UserID, qhErr)
	} else if inQuietHours {
		return false, "quiet_hours", nil
	}

	return true, "", nil
}

// isWithinQuietHours reports whether now falls within prefs's configured
// quiet-hours window, evaluated in prefs.Timezone. Handles a window that
// crosses midnight (e.g. "22:00"-"06:00") correctly — a window is
// "wrapping" whenever its parsed start time is not before its end time.
func isWithinQuietHours(prefs *NotificationPreferences, now time.Time) (bool, error) {
	if !prefs.QuietHoursEnabled {
		return false, nil
	}

	loc, err := time.LoadLocation(prefs.Timezone)
	if err != nil {
		return false, fmt.Errorf("grnoti: invalid quiet-hours timezone %q: %w", prefs.Timezone, err)
	}
	localNow := now.In(loc)

	start, err := time.ParseInLocation("15:04", prefs.QuietHoursStart, loc)
	if err != nil {
		return false, fmt.Errorf("grnoti: invalid quiet-hours start %q: %w", prefs.QuietHoursStart, err)
	}
	end, err := time.ParseInLocation("15:04", prefs.QuietHoursEnd, loc)
	if err != nil {
		return false, fmt.Errorf("grnoti: invalid quiet-hours end %q: %w", prefs.QuietHoursEnd, err)
	}

	startToday := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), start.Hour(), start.Minute(), 0, 0, loc)
	endToday := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), end.Hour(), end.Minute(), 0, 0, loc)

	if startToday.Before(endToday) {
		// Non-wrapping window, e.g. 09:00-17:00.
		return !localNow.Before(startToday) && localNow.Before(endToday), nil
	}
	// Wrapping window (crosses midnight), e.g. 22:00-06:00, or a
	// degenerate equal start/end (never quiet — see doc note below).
	if startToday.Equal(endToday) {
		return false, nil
	}
	return !localNow.Before(startToday) || localNow.Before(endToday), nil
}
