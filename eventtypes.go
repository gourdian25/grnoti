// File: eventtypes.go

package grnoti

import "sync"

// EventType identifies what kind of notification an Event represents. It is
// a plain string type, not a closed/sealed enum — grnoti ships a small,
// domain-neutral vocabulary (below) plus EventTypeCustom as an escape
// hatch, and consumers register their own application-specific types (and
// optional metadata) via EventTypeRegistry rather than grnoti maintaining
// an exhaustive catalog.
//
// This is a deliberate departure from the reference implementation, which
// compiled ~130 e-commerce-specific constants directly into the library and
// spread each type's behavioral traits (default priority, category,
// retryability, ...) across eight separately-maintained exhaustive switch
// statements that had to be extended in lockstep for every new type. Here,
// a trait is one field in one EventTypeMetadata value, registered once.
type EventType string

// String returns the underlying string value.
func (e EventType) String() string { return string(e) }

// IsValid reports whether e is structurally usable as an event type — a
// non-empty string. This is intentionally not "is this a known/registered
// type": an Event carrying an application-specific EventType that was never
// registered with an EventTypeRegistry is still a structurally valid
// Event. Registration is opt-in metadata for consumers who want
// priority/category/retry defaults driven by event type, not a gate on
// which types may be used at all.
func (e EventType) IsValid() bool { return e != "" }

// A small, generic starter vocabulary. Deliberately not domain-specific —
// contrast with the reference implementation's ~130 e-commerce constants
// (order lifecycle, returns, loyalty, EMI due dates, ...), which belong in
// a consumer-side package, not here.
const (
	// EventTypeCustom is the fallback type for anything not otherwise
	// registered. TemplateEngine implementations fall back to a template
	// registered under this type when an event's exact Type has none of
	// its own.
	EventTypeCustom EventType = "custom"

	// EventTypeSystemAlert is a generic operational/system notification
	// (e.g. a security alert, a service disruption notice).
	EventTypeSystemAlert EventType = "system_alert"

	// EventTypeAccountVerification is a generic "verify your account"
	// notification.
	EventTypeAccountVerification EventType = "account_verification"

	// EventTypePasswordReset is a generic "reset your password"
	// notification.
	EventTypePasswordReset EventType = "password_reset"

	// EventTypeGenericTransactional is a generic transactional
	// notification with no more specific type registered — expected to be
	// delivered promptly and not batched into a digest.
	EventTypeGenericTransactional EventType = "generic_transactional"

	// EventTypeGenericMarketing is a generic promotional/marketing
	// notification — expected to respect quiet hours and be eligible for
	// digesting, unlike transactional types.
	EventTypeGenericMarketing EventType = "generic_marketing"
)

// EventTypeMetadata describes the behavioral traits associated with an
// EventType, registered via EventTypeRegistry.Register. Replaces the
// reference implementation's eight separate exhaustive switch statements
// (one per trait) with a single data value per type.
type EventTypeMetadata struct {
	// DefaultPriority is used when an Event of this type doesn't specify
	// its own Priority.
	DefaultPriority Priority

	// Category classifies the type for consumers that branch on it (e.g.
	// PreferencesFilter's per-category opt-out).
	Category NotificationCategory

	// Transactional marks a type as transactional (order confirmations,
	// security alerts) as opposed to marketing.
	Transactional bool

	// RequiresImmediateDelivery marks a type that should never be delayed
	// or batched.
	RequiresImmediateDelivery bool

	// CanBeScheduled marks a type eligible for a future-scheduled send.
	CanBeScheduled bool

	// ShouldIncludeInDigest marks a type eligible for batching into a
	// periodic digest notification instead of sending immediately.
	ShouldIncludeInDigest bool

	// MaxRetries and RetryDelayMultiplier tune dispatch retry behavior for
	// this type; a zero MaxRetries means "use the dispatcher's default."
	MaxRetries           int
	RetryDelayMultiplier float64

	// Description is a short human-readable description, useful for admin
	// tooling listing registered types.
	Description string
}

// EventTypeRegistry tracks EventTypeMetadata for known EventTypes.
// Consumer applications register their own types (and grnoti's own small
// starter vocabulary is pre-registered by NewEventTypeRegistry) instead of
// grnoti maintaining an exhaustive built-in catalog.
type EventTypeRegistry interface {
	// Register associates meta with t, overwriting any existing
	// registration for t.
	//
	// Parameters:
	//   - t: EventType — must satisfy IsValid()
	//   - meta: EventTypeMetadata
	//
	// Returns:
	//   - error: non-nil if t is not IsValid()
	Register(t EventType, meta EventTypeMetadata) error

	// Lookup returns the registered metadata for t, if any.
	//
	// Parameters:
	//   - t: EventType
	//
	// Returns:
	//   - EventTypeMetadata: the zero value if not found
	//   - bool: true if t has a registration
	Lookup(t EventType) (EventTypeMetadata, bool)

	// All returns every currently-registered EventType, in no particular
	// order.
	All() []EventType
}

type eventTypeRegistry struct {
	mu    sync.RWMutex
	table map[EventType]EventTypeMetadata
}

// NewEventTypeRegistry constructs an EventTypeRegistry pre-seeded with
// grnoti's own small generic vocabulary (EventTypeSystemAlert,
// EventTypeAccountVerification, EventTypePasswordReset,
// EventTypeGenericTransactional, EventTypeGenericMarketing) plus
// EventTypeCustom. Consumers call Register to add their own types on top.
func NewEventTypeRegistry() EventTypeRegistry {
	r := &eventTypeRegistry{table: make(map[EventType]EventTypeMetadata)}
	defaults := map[EventType]EventTypeMetadata{
		EventTypeCustom: {
			DefaultPriority: PriorityNormal,
			Category:        CategoryTransactional,
			Description:     "Fallback type for anything not otherwise registered.",
		},
		EventTypeSystemAlert: {
			DefaultPriority:           PriorityHigh,
			Category:                  CategoryAlert,
			Transactional:             true,
			RequiresImmediateDelivery: true,
			Description:               "Generic operational/system notification.",
		},
		EventTypeAccountVerification: {
			DefaultPriority:           PriorityHigh,
			Category:                  CategoryTransactional,
			Transactional:             true,
			RequiresImmediateDelivery: true,
			Description:               "Generic account-verification notification.",
		},
		EventTypePasswordReset: {
			DefaultPriority:           PriorityHigh,
			Category:                  CategoryTransactional,
			Transactional:             true,
			RequiresImmediateDelivery: true,
			Description:               "Generic password-reset notification.",
		},
		EventTypeGenericTransactional: {
			DefaultPriority:           PriorityNormal,
			Category:                  CategoryTransactional,
			Transactional:             true,
			RequiresImmediateDelivery: true,
			Description:               "Generic transactional notification with no more specific type registered.",
		},
		EventTypeGenericMarketing: {
			DefaultPriority:       PriorityLow,
			Category:              CategoryMarketing,
			CanBeScheduled:        true,
			ShouldIncludeInDigest: true,
			Description:           "Generic promotional/marketing notification.",
		},
	}
	for t, meta := range defaults {
		r.table[t] = meta
	}
	return r
}

func (r *eventTypeRegistry) Register(t EventType, meta EventTypeMetadata) error {
	if !t.IsValid() {
		return ErrInvalidEventType
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.table[t] = meta
	return nil
}

func (r *eventTypeRegistry) Lookup(t EventType) (EventTypeMetadata, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.table[t]
	return meta, ok
}

func (r *eventTypeRegistry) All() []EventType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]EventType, 0, len(r.table))
	for t := range r.table {
		out = append(out, t)
	}
	return out
}
