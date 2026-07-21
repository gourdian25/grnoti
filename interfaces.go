// File: interfaces.go

package grnoti

import (
	"context"
	"time"
)

// TokenStore manages device-token registration and lookup. Every backend
// (mongo.go, postgres.go, memory.go) implements this identically so
// application code depends on the interface alone.
type TokenStore interface {
	// GetActiveTokens returns every active DeviceToken registered for
	// userID.
	GetActiveTokens(ctx context.Context, userID string) ([]DeviceToken, error)

	// GetActiveTokensBatch is the multi-user form of GetActiveTokens,
	// returning a map keyed by userID. A userID with no active tokens is
	// simply absent from the result map, not an error.
	GetActiveTokensBatch(ctx context.Context, userIDs []string) (map[string][]DeviceToken, error)

	// GetActiveTokensByAnonymousID is GetActiveTokens for an anonymous
	// (pre-authentication) visitor.
	GetActiveTokensByAnonymousID(ctx context.Context, anonymousID string) ([]DeviceToken, error)

	// MarkInvalid deactivates token (e.g. after FCM reports it as
	// unregistered). Marking an already-inactive or nonexistent token is
	// not an error.
	MarkInvalid(ctx context.Context, token string) error

	// SaveToken upserts token: creates it if new, refreshes its metadata
	// and reactivates it (IsActive=true) if it already exists.
	SaveToken(ctx context.Context, token DeviceToken) error

	// DeleteToken permanently removes token. Deleting a nonexistent token
	// is not an error.
	DeleteToken(ctx context.Context, token string) error

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// IdempotencyStore records which events have already been processed, so a
// redelivered Event (e.g. from Kafka consumer-group rebalance) is not
// dispatched twice.
type IdempotencyStore interface {
	// IsProcessed reports whether eventID has already been marked
	// processed and that mark has not yet expired.
	IsProcessed(ctx context.Context, eventID string) (bool, error)

	// MarkProcessed records eventID as processed for ttl. ttl<=0 means no
	// expiry. Calling MarkProcessed twice for the same eventID is not an
	// error — implementations treat it as idempotent by design.
	MarkProcessed(ctx context.Context, eventID string, ttl time.Duration) error

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// PreferencesStore persists per-user notification preferences (global
// on/off, quiet hours, per-event-type opt-out, locale).
type PreferencesStore interface {
	// GetPreferences returns userID's preferences.
	//
	// Returns:
	//   - error: satisfies errors.Is(err, ErrPreferencesNotFound) if none
	//     exist yet — callers generally treat this as "use defaults," not a
	//     hard failure; check via errors.Is, not direct equality, since an
	//     implementation may wrap it with additional context
	GetPreferences(ctx context.Context, userID string) (*NotificationPreferences, error)

	// SavePreferences upserts prefs. prefs.UserID must be non-empty (see
	// ErrPreferencesUserIDRequired).
	SavePreferences(ctx context.Context, prefs *NotificationPreferences) error

	// IsEventTypeEnabled reports whether userID should receive
	// notifications of eventType, applying defaults ("enabled") when the
	// user has no preferences record or no explicit setting for
	// eventType — an unconfigured user is opted in, not opted out.
	IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error)

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// PreferencesFilter decides whether an Event should be sent at all, given
// the target user's preferences (global toggle, quiet hours, per-type
// opt-out).
type PreferencesFilter interface {
	// ShouldSendNotification evaluates event against its target user's
	// preferences.
	//
	// Returns:
	//   - bool: false means "do not send"
	//   - string: a short machine-readable reason when bool is false (e.g.
	//     "quiet_hours", "global_disabled", "event_type_disabled"),
	//     surfaced as ProcessingResult.SkipReason; empty when bool is true
	//   - error: a genuine operational failure (e.g. PreferencesStore
	//     unreachable) — implementations should fail open (allow the send)
	//     rather than silently drop a notification when preferences can't
	//     be evaluated, and still return the error so the caller can log it
	ShouldSendNotification(ctx context.Context, event Event) (bool, string, error)
}

// ExperimentStore persists Experiment definitions. Assignment (which
// variant a given user gets) is deliberately not part of this interface —
// see ExperimentEngine, which computes assignment as a pure function of an
// Experiment fetched from here, rather than this store owning mutable
// per-user assignment state itself.
type ExperimentStore interface {
	CreateExperiment(ctx context.Context, experiment *Experiment) error

	// GetExperiment returns experimentID's definition.
	//
	// Returns:
	//   - error: wraps ErrExperimentNotFound if no such experiment exists
	GetExperiment(ctx context.Context, experimentID string) (*Experiment, error)

	UpdateExperiment(ctx context.Context, experiment *Experiment) error

	// DeleteExperiment removes an experiment definition. Deleting a
	// nonexistent experiment is not an error.
	DeleteExperiment(ctx context.Context, experimentID string) error

	ListExperiments(ctx context.Context) ([]*Experiment, error)

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// ExperimentEngine computes deterministic variant assignment and records
// impression/conversion analytics. Unlike the reference implementation,
// this interface takes an *Experiment as input to AssignVariant rather than
// owning a mutable map of experiment definitions itself — assignment is a
// pure function of (userID, experiment), so a correct implementation needs
// no internal synchronization for the assignment computation itself; any
// caching of the result (see NewCachedExperimentEngine) is a separate,
// explicitly-synchronized concern.
type ExperimentEngine interface {
	// GetVariant returns userID's existing assignment for experimentID, if
	// one has already been made and cached.
	//
	// Returns:
	//   - *ExperimentVariant: nil if no assignment exists yet — this is not
	//     an error; the caller should call AssignVariant to create one
	//   - error: only for a genuine operational failure reading the cache
	GetVariant(ctx context.Context, userID string, experimentID string) (*ExperimentVariant, error)

	// AssignVariant deterministically computes (and caches) userID's
	// variant within experiment: the same (userID, experiment.ID,
	// experiment.Variants) always produces the same variant, so repeated
	// calls are stable even without the cache.
	//
	// Returns:
	//   - error: wraps ErrExperimentHasNoVariants if experiment.Variants is
	//     empty
	AssignVariant(ctx context.Context, userID string, experiment *Experiment) (*ExperimentVariant, error)

	// TrackImpression records that userID was shown variantID of
	// experimentID, publishing a real analytics event (see
	// AnalyticsPublisher) — unlike the reference implementation, this is
	// not a no-op.
	TrackImpression(ctx context.Context, userID string, experimentID string, variantID string) error

	// TrackConversion records that userID converted while assigned to
	// experimentID.
	TrackConversion(ctx context.Context, userID string, experimentID string) error
}

// AnalyticsPublisher publishes experiment impression/conversion events for
// external analysis. New relative to the reference implementation, whose
// TrackImpression/TrackConversion were hardcoded no-ops — see
// docs/plan/grnoti-plan.md §2 item 9.
type AnalyticsPublisher interface {
	PublishImpression(ctx context.Context, userID, experimentID, variantID string) error
	PublishConversion(ctx context.Context, userID, experimentID string) error

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// DLQHandler durably tracks push-delivery failures across retries and
// process restarts — a different durability contract than grevents' own
// in-memory DeadLetterSink, see docs/plan/grnoti-plan.md §1.2.
type DLQHandler interface {
	// PublishToDLQ records a new failure for event, or (if event.EventID
	// already has a pending/retrying record) appends to its existing
	// attempt history.
	PublishToDLQ(ctx context.Context, event Event, failureReason string) error

	// ClaimRetryableEvents atomically selects up to limit events whose
	// NextRetryAt has passed and whose Status is DLQStatusPending, and
	// transitions each claimed event to DLQStatusRetrying as part of the
	// same operation — so that N concurrent callers (e.g. N worker
	// replicas) each claim disjoint events, never the same one twice.
	// This replaces the reference implementation's GetRetryableEvents,
	// which was a plain read with no claim semantics at all (see
	// docs/plan/grnoti-plan.md §1.3, §2 item 4). Every event returned here
	// must eventually be resolved via MarkRetried — an event claimed but
	// never marked stays in DLQStatusRetrying until a backend-specific
	// claim-timeout sweep (if configured) reclaims it.
	//
	// On error, the returned slice is not necessarily empty and callers
	// must still process it: some backends (e.g. the Mongo implementation,
	// which claims one document per iteration rather than in a single
	// atomic statement) can already have durably transitioned a prefix of
	// events to DLQStatusRetrying before hitting a failure on a later one.
	// Discarding a non-nil slice just because err != nil would orphan
	// those already-claimed events — there is no reclaim-timeout sweep in
	// this package to recover them otherwise. Backends whose claim is a
	// single atomic statement (e.g. Postgres) are all-or-nothing by
	// necessity and always return a nil slice on error; that is a
	// backend-specific limitation, not the general contract.
	ClaimRetryableEvents(ctx context.Context, limit int) ([]*DLQEvent, error)

	// MarkRetried records the outcome of a retry attempt for eventID and
	// transitions it out of DLQStatusRetrying: to DLQStatusResolved on
	// success, DLQStatusExhausted if retries are exhausted, or back to
	// DLQStatusPending (with a recomputed NextRetryAt) otherwise.
	//
	// Returns:
	//   - error: wraps ErrDLQEventNotClaimed if eventID is not currently
	//     DLQStatusRetrying (already resolved by a concurrent caller, or
	//     never claimed) — implementations must scope their update to the
	//     claimed state rather than unconditionally overwriting, see
	//     docs/plan/grnoti-plan.md §5
	MarkRetried(ctx context.Context, eventID string, success bool, attemptErr error) error

	// GetEventByID returns a specific DLQEvent by ID regardless of status.
	//
	// Returns:
	//   - error: wraps ErrDLQEventNotFound if no such event exists
	GetEventByID(ctx context.Context, eventID string) (*DLQEvent, error)

	// PurgeExpiredEvents deletes DLQStatusResolved/DLQStatusExhausted
	// events, and any event older than maxAge regardless of status.
	//
	// Returns:
	//   - int64: number of events deleted
	PurgeExpiredEvents(ctx context.Context, maxAge time.Duration) (int64, error)

	// Close releases any underlying connections. Idempotent.
	Close() error
}

// PushDispatcher sends rendered Messages to devices or topics via FCM.
type PushDispatcher interface {
	// Send delivers msg to every token in tokens, batching/fanning out by
	// platform internally. A partial failure (some tokens succeed, others
	// don't) is reported via the returned DispatchResult, not a non-nil
	// error — Send returns a non-nil error only when it could not attempt
	// delivery at all (e.g. msg fails payload-size validation).
	Send(ctx context.Context, tokens []DeviceToken, msg Message) (DispatchResult, error)

	// SendToToken sends msg to a single token, with no batching.
	SendToToken(ctx context.Context, token DeviceToken, msg Message) error

	// SendToTopic sends msg to every device subscribed to topic via FCM's
	// own topic-messaging feature. FCM does not report per-recipient
	// results for topic sends, so success/failure here means "the FCM API
	// call itself succeeded/failed," not delivery to any specific device.
	SendToTopic(ctx context.Context, topic string, msg Message) error
}

// EventConsumer ingests Events from an external source (Kafka) and invokes
// handler for each.
type EventConsumer interface {
	// Start begins consuming and invoking handler for each Event, blocking
	// until ctx is canceled or an unrecoverable error occurs.
	Start(ctx context.Context, handler func(context.Context, Event) error) error

	// Close stops consuming and releases underlying connections.
	// Idempotent.
	Close() error
}

// Metrics receives counters/observations from dispatch and processing.
// Unlike the reference implementation, the by-type/by-platform variants
// take both labels together in one call rather than three separate methods
// that each only populate one label dimension — see
// docs/plan/grnoti-plan.md §2 item 10 for why the split version
// triple-counted.
type Metrics interface {
	IncNotificationsProcessed()
	IncNotificationsSent(eventType EventType, platform Platform, count int)
	IncNotificationsFailed(eventType EventType, platform Platform, count int)
	IncInvalidTokens(count int)
	IncEventsSkipped(reason string)
	ObserveDispatchLatency(eventType EventType, platform Platform, duration time.Duration)
	ObserveProcessingLatency(duration time.Duration)
}

// RateLimiter bounds outbound FCM request rate. See ratelimiter.go (local,
// per-process) and ratelimiter.redis.go (distributed) for the two
// implementations — deliberately different backends behind one interface,
// see docs/plan/grnoti-plan.md §1.1.
//
// The interface deliberately has no Close(): localRateLimiter owns no
// resource, so requiring it would force a no-op on every implementation.
// redisRateLimiter does own a *redis.Client and exposes a Close() error
// method on its concrete type — callers using the Redis-backed variant
// type-assert to it (or to an io.Closer) when they need to shut it down,
// the same pattern already used for UpdateLimit.
type RateLimiter interface {
	// Allow reports whether a request may proceed right now, without
	// blocking. Consumes a token if true.
	Allow(ctx context.Context) (bool, error)

	// Wait blocks until a token is available or ctx is done.
	Wait(ctx context.Context) error

	// GetStats returns a point-in-time snapshot of this limiter's counters.
	GetStats(ctx context.Context) (RateLimiterStats, error)
}

// CircuitBreaker wraps calls to an unreliable dependency (FCM) so
// persistent failures stop being retried immediately and instead fail fast
// for a cooldown period.
type CircuitBreaker interface {
	// Execute runs fn if the breaker's current state allows it.
	//
	// Returns:
	//   - error: ErrCircuitOpen if the breaker is open and its Timeout
	//     hasn't elapsed; ErrTooManyRequests if half-open and
	//     MaxHalfOpenRequests trial requests are already in flight;
	//     otherwise fn's own return value
	Execute(ctx context.Context, fn func() error) error

	State() CircuitState

	GetStats() CircuitBreakerStats

	// Reset forces the breaker back to CircuitStateClosed, for
	// administrative use.
	Reset()
}

// TemplateEngine renders an Event into a Message using registered
// MessageTemplates.
type TemplateEngine interface {
	// BuildMessage renders event into a Message using the MessageTemplate
	// registered for event.Type, falling back to the template registered
	// under EventTypeCustom if event.Type has none of its own.
	//
	// Returns:
	//   - error: wraps ErrTemplateNotFound if neither event.Type nor
	//     EventTypeCustom has a registered template
	BuildMessage(event Event) (Message, error)

	RegisterTemplate(eventType EventType, template MessageTemplate) error
}

// LocalizationStore holds per-locale MessageTemplate variants.
type LocalizationStore interface {
	// GetLocalizedTemplate returns eventType's template in locale, falling
	// back to the LocalizedTemplate's own DefaultLocale if locale isn't
	// registered for eventType.
	//
	// Returns:
	//   - error: wraps ErrTemplateNotFound if eventType has no
	//     LocalizedTemplate registered at all
	GetLocalizedTemplate(eventType EventType, locale string) (MessageTemplate, error)

	RegisterLocalizedTemplate(eventType EventType, locale string, template MessageTemplate) error

	// GetSupportedLocales returns every locale registered for eventType,
	// or an empty (non-nil) slice if none are.
	GetSupportedLocales(eventType EventType) []string
}

// LocaleResolver determines which locale to render a notification in.
type LocaleResolver interface {
	ResolveLocale(ctx context.Context, userID string) (string, error)
	ResolveLocaleForAnonymous(ctx context.Context, anonymousID string) (string, error)
	GetDefaultLocale() string
}

// NotificationTarget is where a resolved Event should be sent — either a
// fixed set of device tokens, or an FCM topic.
type NotificationTarget interface {
	IsTopicBased() bool
	GetTopicName() string
	GetTokens() []DeviceToken
}

// TopicRouter resolves an Event to a NotificationTarget.
type TopicRouter interface {
	ResolveTarget(ctx context.Context, event Event) (NotificationTarget, error)
}

// BatchSplitter deduplicates and chunks DeviceTokens for dispatch.
type BatchSplitter interface {
	// Split partitions tokens into batches of at most maxBatchSize each.
	// maxBatchSize<=0 or an empty tokens returns tokens as one batch.
	Split(tokens []DeviceToken, maxBatchSize int) [][]DeviceToken

	// Deduplicate removes tokens with a repeated Token value, keeping the
	// first occurrence (order-preserving).
	Deduplicate(tokens []DeviceToken) []DeviceToken
}

// RetryStrategy decides whether and how long to wait before retrying a
// failed FCM send.
type RetryStrategy interface {
	// ShouldRetry reports whether attempt (0-indexed) should be retried
	// given err.
	ShouldRetry(attempt int, err error) bool

	// GetDelay returns how long to wait before attempt (0-indexed).
	GetDelay(attempt int) time.Duration
}

// PayloadValidator checks a Message against FCM's payload size limit
// before attempting to send it.
type PayloadValidator interface {
	// ValidateSize returns ErrFCMPayloadTooLarge if msg's estimated
	// serialized size exceeds FCMMaxPayloadSize.
	ValidateSize(msg Message) error

	// EstimateSize returns msg's estimated serialized size in bytes.
	EstimateSize(msg Message) int
}

// NotificationService is the top-level orchestrator: validates an Event,
// checks preferences and idempotency, renders it, resolves recipients,
// dispatches, and records the outcome (metrics, invalid-token cleanup, DLQ
// on exhausted failure).
type NotificationService interface {
	ProcessEvent(ctx context.Context, event Event) (ProcessingResult, error)

	// Submit is the ingestion-bridge entrypoint (docs/plan/grnoti-plan.md
	// §3.1): pass it directly as an EventConsumer's handler —
	// consumer.Start(ctx, service.Submit) — to wire Kafka ingestion
	// straight into this service. When the service was constructed with
	// ServiceConfig.EnableBackpressure, Submit enqueues onto the
	// service's own bounded WorkerPool (non-blocking, ErrWorkerPoolFull
	// on a full queue) instead of processing on the caller's goroutine;
	// otherwise it's equivalent to ProcessEvent with the ProcessingResult
	// discarded. Its signature matches WorkerPool's own Handler shape
	// exactly, which is why it has no ProcessingResult return — use
	// ProcessEvent directly when the caller needs that.
	Submit(ctx context.Context, event Event) error

	// Close stops any background workers (see WorkerPool) this service
	// owns and releases resources. Idempotent. The reference
	// implementation's NotificationService had no Close at all — see
	// docs/plan/grnoti-plan.md §3.1/§3.6 for why this service now owns a
	// WorkerPool that needs one.
	Close() error
}
