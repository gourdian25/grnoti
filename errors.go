// File: errors.go

package grnoti

import "errors"

// Sentinel errors for use with errors.Is. Backend implementations translate
// their own native errors (mongo.ErrNoDocuments, gorm.ErrRecordNotFound,
// redis.Nil, ...) into these sentinels before wrapping with
// fmt.Errorf("...: %w", ...) — a backend-native error must never leak
// through a grnoti interface unwrapped, matching grcache's and graudit's
// own documented rule.
//
// There is deliberately no IsX(err error) bool helper: callers use
// errors.Is(err, grnoti.ErrClosed) directly, consistent with every other
// gourdian repo's sentinel-error convention.
//
// Two sentinels replace error reuse found in the reference implementation
// (see docs/plan/grnoti-plan.md §2 item 6): ErrNoTargetSpecified used to be
// ErrInvalidUserID reused for a semantically different condition, and
// ErrExperimentNotFound/ErrExperimentHasNoVariants used to both be
// ErrTemplateNotFound reused.
var (
	// ErrClosed indicates a method was called after Close.
	ErrClosed = errors.New("grnoti: closed")

	// ErrBackendUnavailable indicates a storage backend could not be
	// reached (connection failure, timeout, transaction failure, etc.).
	ErrBackendUnavailable = errors.New("grnoti: backend unavailable")

	// ErrInvalidEventID indicates Event.EventID was empty.
	ErrInvalidEventID = errors.New("grnoti: event id is required")

	// ErrNoTargetSpecified indicates an Event has none of UserID,
	// AnonymousID, or DeviceTokens set — there is no way to resolve who
	// should receive it.
	ErrNoTargetSpecified = errors.New("grnoti: at least one of user id, anonymous id, or device tokens is required")

	// ErrInvalidEventType indicates Event.Type failed EventType.IsValid().
	ErrInvalidEventType = errors.New("grnoti: invalid event type")

	// ErrInvalidPriority indicates Event.Priority failed Priority.IsValid().
	ErrInvalidPriority = errors.New("grnoti: invalid priority")

	// ErrTemplateNotFound indicates no MessageTemplate is registered for
	// an event type, and no EventTypeCustom fallback is registered either.
	ErrTemplateNotFound = errors.New("grnoti: template not found for event type")

	// ErrPreferencesNotFound indicates no NotificationPreferences exist
	// for a user. Callers generally treat this as "use defaults," not a
	// hard failure — see PreferencesStore.IsEventTypeEnabled.
	ErrPreferencesNotFound = errors.New("grnoti: preferences not found")

	// ErrExperimentNotFound indicates no Experiment is registered under
	// the requested ID.
	ErrExperimentNotFound = errors.New("grnoti: experiment not found")

	// ErrExperimentHasNoVariants indicates an Experiment exists but has
	// zero ExperimentVariants, so no assignment can be made.
	ErrExperimentHasNoVariants = errors.New("grnoti: experiment has no variants")

	// ErrDLQEventNotFound indicates a DLQHandler lookup found no
	// DLQEvent for the requested event ID.
	ErrDLQEventNotFound = errors.New("grnoti: dead-letter event not found")

	// ErrDLQEventNotClaimed indicates MarkRetried was called for an event
	// that is not currently in the "retrying" (claimed) state — either it
	// was never claimed via ClaimRetryableEvents, or a concurrent caller
	// already resolved/exhausted it. See docs/plan/grnoti-plan.md §5 for
	// why this replaces the source's unguarded read-then-write update.
	ErrDLQEventNotClaimed = errors.New("grnoti: dead-letter event is not in a claimed (retrying) state")

	// ErrFCMClientNil indicates a PushDispatcher was constructed with a
	// nil FCM client.
	ErrFCMClientNil = errors.New("grnoti: fcm client is nil")

	// ErrFCMPayloadTooLarge indicates a Message's estimated FCM payload
	// size exceeds FCMMaxPayloadSize.
	ErrFCMPayloadTooLarge = errors.New("grnoti: fcm payload exceeds maximum size")
)

// FCMErrorCode classifies an FCM send failure into a small set of
// well-known categories, used to decide retryability without callers
// needing to string-match raw FCM SDK error text themselves.
type FCMErrorCode string

const (
	FCMErrorCodeUnspecified       FCMErrorCode = "unspecified"
	FCMErrorCodeInvalidArgument   FCMErrorCode = "invalid_argument"
	FCMErrorCodeUnregistered      FCMErrorCode = "unregistered"
	FCMErrorCodeSenderIDMismatch  FCMErrorCode = "sender_id_mismatch"
	FCMErrorCodeQuotaExceeded     FCMErrorCode = "quota_exceeded"
	FCMErrorCodeUnavailable       FCMErrorCode = "unavailable"
	FCMErrorCodeInternal          FCMErrorCode = "internal"
	FCMErrorCodeThirdPartyAuthErr FCMErrorCode = "third_party_auth_error"
)

// IsRetryable reports whether a failure of this class is worth retrying
// (transient server-side conditions), as opposed to a condition that will
// never succeed no matter how many times it's retried.
func (c FCMErrorCode) IsRetryable() bool {
	switch c {
	case FCMErrorCodeUnavailable, FCMErrorCodeInternal, FCMErrorCodeQuotaExceeded:
		return true
	default:
		return false
	}
}

// IsPermanent reports whether a failure of this class means the token
// itself is dead and should be removed via TokenStore.MarkInvalid, rather
// than retried.
func (c FCMErrorCode) IsPermanent() bool {
	switch c {
	case FCMErrorCodeUnregistered, FCMErrorCodeInvalidArgument, FCMErrorCodeSenderIDMismatch:
		return true
	default:
		return false
	}
}

// FCMError wraps a single FCM send failure for one token with its
// classified FCMErrorCode.
type FCMError struct {
	Code    FCMErrorCode
	Token   string
	Message string
	Err     error
}

func (e *FCMError) Error() string {
	if e.Err != nil {
		return "grnoti: fcm send failed (" + string(e.Code) + "): " + e.Message + ": " + e.Err.Error()
	}
	return "grnoti: fcm send failed (" + string(e.Code) + "): " + e.Message
}

func (e *FCMError) Unwrap() error { return e.Err }

// IsRetryable delegates to e.Code.IsRetryable.
func (e *FCMError) IsRetryable() bool { return e.Code.IsRetryable() }

// IsPermanent delegates to e.Code.IsPermanent.
func (e *FCMError) IsPermanent() bool { return e.Code.IsPermanent() }

// NewFCMError constructs an FCMError.
func NewFCMError(code FCMErrorCode, token, message string, err error) *FCMError {
	return &FCMError{Code: code, Token: token, Message: message, Err: err}
}
