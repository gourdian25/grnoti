// File: types.go

package grnoti

import "time"

// Priority is a notification's delivery priority.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// String returns the underlying string value.
func (p Priority) String() string { return string(p) }

// IsValid reports whether p is one of the defined Priority constants.
func (p Priority) IsValid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

// Platform is the device platform a DeviceToken/DispatchResult applies to.
type Platform string

const (
	PlatformAndroid Platform = "android"
	PlatformIOS     Platform = "ios"
	PlatformWeb     Platform = "web"
)

// String returns the underlying string value.
func (p Platform) String() string { return string(p) }

// IsValid reports whether p is one of the defined Platform constants.
func (p Platform) IsValid() bool {
	switch p {
	case PlatformAndroid, PlatformIOS, PlatformWeb:
		return true
	default:
		return false
	}
}

// NotificationCategory classifies a Message for filtering/preference
// purposes (e.g. a user opting out of marketing but not transactional
// notifications).
type NotificationCategory string

const (
	CategoryTransactional NotificationCategory = "transactional"
	CategoryMarketing     NotificationCategory = "marketing"
	CategorySocial        NotificationCategory = "social"
	CategoryAlert         NotificationCategory = "alert"
)

// Event is the unit of work a NotificationService processes: a single
// notification-worthy occurrence for one target (an authenticated user, an
// anonymous visitor, or a fixed set of device tokens).
type Event struct {
	EventID      string            `json:"event_id" bson:"event_id"`
	UserID       string            `json:"user_id,omitempty" bson:"user_id,omitempty"`
	AnonymousID  string            `json:"anonymous_id,omitempty" bson:"anonymous_id,omitempty"`
	DeviceTokens []string          `json:"device_tokens,omitempty" bson:"device_tokens,omitempty"`
	Type         EventType         `json:"type" bson:"type"`
	Payload      map[string]string `json:"payload" bson:"payload"`
	Priority     Priority          `json:"priority" bson:"priority"`
	Timestamp    time.Time         `json:"timestamp" bson:"timestamp"`
	ExperimentID string            `json:"experiment_id,omitempty" bson:"experiment_id,omitempty"`
}

// IsAuthenticated reports whether e targets a known user (UserID set).
func (e Event) IsAuthenticated() bool { return e.UserID != "" }

// IsAnonymous reports whether e targets an anonymous visitor (AnonymousID
// set, UserID not).
func (e Event) IsAnonymous() bool { return e.AnonymousID != "" }

// HasDirectTokens reports whether e targets an explicit set of device
// tokens, bypassing TokenStore lookup entirely.
func (e Event) HasDirectTokens() bool { return len(e.DeviceTokens) > 0 }

// GetTargetID returns whichever identifier best names e's recipient, for
// logging/metrics — UserID, else AnonymousID, else the literal "direct" for
// a token-only Event.
func (e Event) GetTargetID() string {
	if e.UserID != "" {
		return e.UserID
	}
	if e.AnonymousID != "" {
		return e.AnonymousID
	}
	return "direct"
}

// Validate checks that e has the minimum fields required to be processed.
//
// Returns:
//   - error: ErrInvalidEventID, ErrNoTargetSpecified, ErrInvalidEventType,
//     or ErrInvalidPriority — each a distinct sentinel for a distinct
//     condition (the reference implementation reused one sentinel for two
//     of these; see docs/plan/grnoti-plan.md §2 item 6)
func (e Event) Validate() error {
	switch {
	case e.EventID == "":
		return ErrInvalidEventID
	case e.UserID == "" && e.AnonymousID == "" && len(e.DeviceTokens) == 0:
		return ErrNoTargetSpecified
	case !e.Type.IsValid():
		return ErrInvalidEventType
	case !e.Priority.IsValid():
		return ErrInvalidPriority
	}
	return nil
}

// DeviceToken is a single registered push-notification destination.
type DeviceToken struct {
	Token       string    `json:"token" bson:"token"`
	Platform    Platform  `json:"platform" bson:"platform"`
	UserID      string    `json:"user_id,omitempty" bson:"user_id,omitempty"`
	AnonymousID string    `json:"anonymous_id,omitempty" bson:"anonymous_id,omitempty"`
	DeviceID    string    `json:"device_id,omitempty" bson:"device_id,omitempty"`
	AppVersion  string    `json:"app_version,omitempty" bson:"app_version,omitempty"`
	IsActive    bool      `json:"is_active" bson:"is_active"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" bson:"updated_at"`
}

// NotificationAction is one rich-push action button.
type NotificationAction struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Icon  string `json:"icon,omitempty"`
	URL   string `json:"url,omitempty"`
}

// Message is the fully-rendered, platform-agnostic notification content a
// PushDispatcher sends.
type Message struct {
	Title       string
	Body        string
	Data        map[string]string
	ImageURL    string
	Priority    Priority
	TTL         time.Duration
	CollapseKey string
	ChannelID   string
	Badge       *int
	Sound       string
	Actions     []NotificationAction
	DeepLink    string
	Category    NotificationCategory
}

// DispatchResult summarizes the outcome of a PushDispatcher.Send call.
type DispatchResult struct {
	SuccessCount    int
	FailureCount    int
	InvalidTokens   []string
	RetryableErrors int
	Errors          []error

	// SuccessByPlatform/FailureByPlatform break SuccessCount/FailureCount
	// down per Platform — populated by dispatcher.fcm.go's Send, which
	// dispatches each platform group separately before merging into the
	// aggregate counts above. NotificationService uses this breakdown to
	// call Metrics.IncNotificationsSent/Failed and
	// Metrics.ObserveDispatchLatency with a real per-call Platform label
	// (required by the Metrics interface) instead of guessing one.
	SuccessByPlatform map[Platform]int
	FailureByPlatform map[Platform]int
}

// TotalCount returns SuccessCount + FailureCount.
func (d DispatchResult) TotalCount() int { return d.SuccessCount + d.FailureCount }

// HasFailures reports whether FailureCount > 0.
func (d DispatchResult) HasFailures() bool { return d.FailureCount > 0 }

// ProcessingResult summarizes the outcome of NotificationService.ProcessEvent.
type ProcessingResult struct {
	EventID        string
	UserID         string
	TokenCount     int
	DispatchResult DispatchResult
	ProcessedAt    time.Time
	Duration       time.Duration
	Skipped        bool
	SkipReason     string
}

// IdempotencyRecord is what an IdempotencyStore persists for a processed
// event.
type IdempotencyRecord struct {
	EventID     string    `json:"event_id" bson:"event_id"`
	ProcessedAt time.Time `json:"processed_at" bson:"processed_at"`
	ExpiresAt   time.Time `json:"expires_at" bson:"expires_at"`
}

// NotificationPreferences holds one user's notification settings.
type NotificationPreferences struct {
	UserID            string
	GlobalEnabled     bool
	QuietHoursEnabled bool
	QuietHoursStart   string // "HH:MM", in Timezone
	QuietHoursEnd     string // "HH:MM", in Timezone
	Timezone          string // IANA tz name, e.g. "America/New_York"
	Locale            string
	EventTypeSettings map[EventType]bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// IsEventTypeEnabled evaluates whether eventType should be sent under p:
// false if globally disabled, else the explicit per-type setting if one
// exists, else true (an unconfigured event type defaults to enabled, not
// disabled). Shared by every PreferencesStore implementation's own
// IsEventTypeEnabled method so the "unconfigured defaults to enabled" rule
// is defined exactly once.
func (p *NotificationPreferences) IsEventTypeEnabled(eventType EventType) bool {
	if !p.GlobalEnabled {
		return false
	}
	if enabled, ok := p.EventTypeSettings[eventType]; ok {
		return enabled
	}
	return true
}

// MessageTemplate is what callers register with a TemplateEngine to define
// how an EventType renders into a Message.
type MessageTemplate struct {
	TitleTemplate string
	BodyTemplate  string
	DefaultData   map[string]string
	DefaultTTL    time.Duration
	CollapseKey   string
	ChannelID     string
	Sound         string
	Actions       []NotificationAction
	DeepLink      string
	Category      NotificationCategory
}

// LocalizedTemplate holds per-locale MessageTemplate variants for one
// EventType, plus which locale to fall back to when a requested locale
// isn't registered.
type LocalizedTemplate struct {
	DefaultLocale string
	Templates     map[string]MessageTemplate // locale -> template
}

// DLQStatus is a DLQEvent's lifecycle state.
type DLQStatus string

const (
	// DLQStatusPending is newly-recorded or awaiting its next retry.
	DLQStatusPending DLQStatus = "pending"
	// DLQStatusRetrying means a worker currently holds an atomic claim on
	// this event (see DLQHandler.ClaimRetryableEvents) and is attempting
	// delivery — not a status any caller sets directly.
	DLQStatusRetrying DLQStatus = "retrying"
	// DLQStatusExhausted means retries were exhausted without success.
	DLQStatusExhausted DLQStatus = "exhausted"
	// DLQStatusResolved means a retry eventually succeeded.
	DLQStatusResolved DLQStatus = "resolved"
)

// DLQRetryAttempt records the outcome of one retry attempt for a DLQEvent.
type DLQRetryAttempt struct {
	AttemptNumber int
	AttemptedAt   time.Time
	Success       bool
	ErrorMessage  string
}

// DLQEvent is one durably-tracked failed-delivery record.
type DLQEvent struct {
	EventID        string
	Event          Event
	FailureReason  string
	RetryCount     int
	MaxRetries     int
	FirstFailureAt time.Time
	LastAttemptAt  time.Time
	NextRetryAt    time.Time
	Status         DLQStatus
	AttemptHistory []DLQRetryAttempt
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ExperimentVariant is one arm of an Experiment.
type ExperimentVariant struct {
	ID      string
	Name    string
	Weight  int // relative weight for deterministic bucketing; weights need not sum to 100
	Payload map[string]string
}

// Experiment is an A/B (or A/B/n) test definition.
type Experiment struct {
	ID        string
	Name      string
	Variants  []ExperimentVariant
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ExperimentAssignment is one user's deterministic variant assignment for
// one Experiment.
type ExperimentAssignment struct {
	UserID       string
	ExperimentID string
	VariantID    string
	AssignedAt   time.Time
}

// CircuitState is a CircuitBreaker's current state.
type CircuitState string

const (
	CircuitStateClosed   CircuitState = "closed"
	CircuitStateOpen     CircuitState = "open"
	CircuitStateHalfOpen CircuitState = "half_open"
)

// CircuitBreakerConfig configures a CircuitBreaker.
type CircuitBreakerConfig struct {
	// MaxFailures is the number of consecutive failures that trips the
	// breaker from closed to open.
	MaxFailures int
	// Timeout is how long the breaker stays open before allowing a trial
	// request through (transitioning to half-open).
	Timeout time.Duration
	// ResetTimeout is how long a closed breaker must go without a failure
	// before its consecutive-failure counter resets to zero.
	ResetTimeout time.Duration
	// MaxHalfOpenRequests bounds concurrent trial requests while
	// half-open. Defaults to 1 if <= 0.
	MaxHalfOpenRequests int
}

// CircuitBreakerStats is a point-in-time snapshot of a CircuitBreaker's
// counters.
type CircuitBreakerStats struct {
	State                CircuitState
	ConsecutiveFailures  int
	TotalSuccesses       int64
	TotalFailures        int64
	TotalRejections      int64
	LastFailureTime      time.Time
	LastStateChange      time.Time
	OpenedAt             time.Time
	TimeUntilNextAttempt time.Duration
}

// RateLimiterStats is a point-in-time snapshot of a RateLimiter's counters.
type RateLimiterStats struct {
	RequestsPerSecond int
	BurstSize         int
	AllowedCount      int64
	BlockedCount      int64
	WaitCount         int64
	LastAllowedAt     time.Time
}

// WorkerPoolConfig configures a WorkerPool.
type WorkerPoolConfig struct {
	// Workers is the number of worker goroutines. Defaults to 10 if <= 0.
	Workers int
	// QueueSize is the buffered-channel capacity. Defaults to 1000 if <= 0.
	QueueSize int
}

// WorkerPoolStats is a point-in-time snapshot of a WorkerPool's queue.
type WorkerPoolStats struct {
	Workers      int
	QueueSize    int
	QueuedEvents int
	QueueUsage   float64 // QueuedEvents / QueueSize
}

// ServiceConfig configures a NotificationService's optional behaviors. All
// Enable* flags default to false ("disabled by default for predictability
// and backward compatibility" — matching the reference implementation's own
// convention) except where DefaultServiceConfig says otherwise.
type ServiceConfig struct {
	IdempotencyTTL           time.Duration
	MaxTokensPerBatch        int
	EnableMetrics            bool
	SkipInvalidEvents        bool
	EnableTokenDeduplication bool
	EnforceBatching          bool
	EnablePreferencesFilter  bool
	EnableTopicRouting       bool
	// EnableRichPush, EnableLocalization, EnableABTesting are
	// composition-time flags, not read anywhere in
	// notificationService.processEvent (service.go) directly — rich-push
	// fields live on Message and are populated by whichever
	// TemplateEngine ServiceDeps.Templates is; localization is a
	// TemplateEngine decorator (localizedTemplateEngine, see
	// localization.go) the caller wraps ServiceDeps.Templates in; A/B
	// assignment (ExperimentEngine.AssignVariant) happens before an Event
	// is even constructed, to decide what goes in its Payload. These
	// three flags exist for a caller's own bookkeeping/documentation of
	// which optional pieces a given ServiceDeps wiring includes, not as
	// live branches in the pipeline.
	EnableRichPush     bool
	EnableLocalization bool
	EnableBackpressure bool
	EnableABTesting    bool
	// EnableDLQ gates whether ProcessEvent publishes exhausted-retry
	// dispatch failures to the configured DLQHandler. The reference
	// implementation had no such wiring at all (see
	// docs/plan/grnoti-plan.md §3.6) — this flag exists so the fix is
	// opt-in-by-default-on rather than silently always-on, matching how
	// every other integration point in this config is a flag.
	EnableDLQ bool
	// EnableEventBus gates whether ProcessEvent/ExperimentEngine publish
	// lifecycle events via the configured grevents.Bus (see
	// docs/plan/grnoti-plan.md §1.2).
	EnableEventBus bool
}

// DefaultServiceConfig returns a ServiceConfig with sane production
// defaults: metrics on, DLQ publishing on, everything else opt-in.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		IdempotencyTTL:    24 * time.Hour,
		MaxTokensPerBatch: 500, // FCM's own per-multicast-call limit
		EnableMetrics:     true,
		EnableDLQ:         true,
	}
}
