// File: service.go

package grnoti

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gourdian25/grevents"
)

// ServiceDeps configures a NotificationService constructed by
// NewNotificationService.
type ServiceDeps struct {
	// TokenStore, Dispatcher, Templates, Idempotency are required.
	TokenStore  TokenStore
	Dispatcher  PushDispatcher
	Templates   TemplateEngine
	Idempotency IdempotencyStore

	// PreferencesFilter, if set and Config.EnablePreferencesFilter, gates
	// authenticated-user dispatch on ShouldSendNotification.
	PreferencesFilter PreferencesFilter
	// TopicRouter, if set and Config.EnableTopicRouting, resolves each
	// Event's NotificationTarget instead of the default direct-tokens
	// resolution (resolveTokensForEvent in topicrouter.go).
	TopicRouter TopicRouter
	// DLQHandler, if set and Config.EnableDLQ, receives events whose
	// dispatch has unresolved failures after the dispatcher's own retry
	// is exhausted — the reference implementation built a whole DLQ
	// subsystem nothing ever called (docs/plan/grnoti-plan.md §3.6); this
	// is that missing call.
	DLQHandler DLQHandler
	// EventBus, if set and Config.EnableEventBus, receives
	// TopicNotificationSent/TopicNotificationFailed lifecycle events (see
	// events.go, §1.2).
	EventBus grevents.Bus
	// Metrics is optional. NotificationService only calls the
	// per-event/per-platform counters (IncNotificationsProcessed/Sent/
	// Failed, ObserveDispatchLatency/ProcessingLatency, IncEventsSkipped)
	// — it deliberately does NOT call IncInvalidTokens, since
	// dispatcher.fcm.go already calls that itself when the same Metrics
	// instance is wired into FCMDispatcherDeps.Metrics; calling it again
	// here would double-count.
	Metrics Metrics

	// Config toggles pipeline behavior. See DefaultServiceConfig.
	Config ServiceConfig
	// WorkerPoolConfig is used only when Config.EnableBackpressure is
	// true, to build the service's own internal *WorkerPool (see Submit).
	WorkerPoolConfig WorkerPoolConfig

	Logger Logger
}

// notificationService implements NotificationService: validates an Event,
// checks idempotency and preferences, renders it, resolves recipients,
// dispatches, and records the outcome. See ProcessEvent's own doc comment
// for the full pipeline and how it differs from the reference
// implementation's ordering (docs/plan/grnoti-plan.md's Stage 12
// implementation log).
type notificationService struct {
	tokenStore  TokenStore
	dispatcher  PushDispatcher
	templates   TemplateEngine
	idempotency IdempotencyStore

	preferencesFilter PreferencesFilter
	topicRouter       TopicRouter
	dlqHandler        DLQHandler
	bus               grevents.Bus
	metrics           Metrics

	batchSplitter BatchSplitter
	config        ServiceConfig
	logger        Logger

	// workerPool is non-nil only when config.EnableBackpressure was set
	// at construction — see Submit.
	workerPool *WorkerPool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ NotificationService = (*notificationService)(nil)

// NewNotificationService constructs a NotificationService.
//
// Parameters:
//   - deps: ServiceDeps — TokenStore, Dispatcher, Templates, Idempotency
//     are required; everything else is optional and nil-safe
//
// Returns:
//   - NotificationService: ready to use immediately; if
//     deps.Config.EnableBackpressure is set, its internal WorkerPool is
//     already started
//   - error: non-nil if a required dependency is missing, or if building
//     the internal WorkerPool fails
func NewNotificationService(deps ServiceDeps) (NotificationService, error) {
	if deps.TokenStore == nil {
		return nil, fmt.Errorf("grnoti: ServiceDeps.TokenStore is required")
	}
	if deps.Dispatcher == nil {
		return nil, fmt.Errorf("grnoti: ServiceDeps.Dispatcher is required")
	}
	if deps.Templates == nil {
		return nil, fmt.Errorf("grnoti: ServiceDeps.Templates is required")
	}
	if deps.Idempotency == nil {
		return nil, fmt.Errorf("grnoti: ServiceDeps.Idempotency is required")
	}

	s := &notificationService{
		tokenStore:        deps.TokenStore,
		dispatcher:        deps.Dispatcher,
		templates:         deps.Templates,
		idempotency:       deps.Idempotency,
		preferencesFilter: deps.PreferencesFilter,
		topicRouter:       deps.TopicRouter,
		dlqHandler:        deps.DLQHandler,
		bus:               deps.EventBus,
		metrics:           deps.Metrics,
		batchSplitter:     NewBatchSplitter(),
		config:            deps.Config,
		logger:            OrNop(deps.Logger),
	}

	if s.config.EnableBackpressure {
		pool, err := NewWorkerPool(WorkerPoolDeps{
			Config: deps.WorkerPoolConfig,
			Handler: func(ctx context.Context, event Event) error {
				_, err := s.processEvent(ctx, event)
				return err
			},
			Logger:  s.logger,
			Metrics: s.metrics,
		})
		if err != nil {
			return nil, fmt.Errorf("grnoti: build worker pool: %w", err)
		}
		pool.Start()
		s.workerPool = pool
	}

	return s, nil
}

// ProcessEvent implements NotificationService.ProcessEvent: always
// synchronous, running the full pipeline on the calling goroutine
// regardless of whether backpressure is enabled. See processEvent for the
// pipeline itself.
func (s *notificationService) ProcessEvent(ctx context.Context, event Event) (ProcessingResult, error) {
	if s.closed.Load() {
		return ProcessingResult{}, ErrClosed
	}
	return s.processEvent(ctx, event)
}

// Submit implements NotificationService.Submit.
func (s *notificationService) Submit(ctx context.Context, event Event) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if s.workerPool != nil {
		return s.workerPool.Submit(event)
	}
	_, err := s.processEvent(ctx, event)
	return err
}

// Close implements NotificationService.Close.
func (s *notificationService) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.workerPool != nil {
			s.workerPool.Stop()
		}
		s.logger.Infof("grnoti: notification service closed")
	})
	return nil
}

// processEvent is the actual ProcessEvent pipeline:
//
//	Validate -> Idempotency check -> Preferences check -> Build message ->
//	Resolve target -> Dispatch -> Mark invalid tokens -> DLQ ->
//	Metrics -> Lifecycle events -> Mark processed
//
// Two ordering changes from the reference implementation
// (docs/plan/grnoti-plan.md's Stage 12 implementation log has the full
// account):
//
//  1. Idempotency now runs before the preferences check, not after. The
//     reference paid for a full PreferencesStore round-trip on every
//     redelivered duplicate (e.g. Kafka at-least-once redelivery) before
//     ever discovering the event didn't need any work at all — idempotency
//     is the cheaper, more decisive check and should short-circuit first.
//  2. DLQHandler.PublishToDLQ is now actually called. The reference built
//     the entire DLQ subsystem but never wired it into ProcessEvent at all
//     (§3.6) — a dispatch that exhausted the dispatcher's own retries
//     (Stage 10) was logged and nothing more. Here, any dispatch failure
//     not accounted for by an already-marked-invalid token is published to
//     the DLQ when Config.EnableDLQ is set.
func (s *notificationService) processEvent(ctx context.Context, event Event) (ProcessingResult, error) {
	startTime := time.Now()
	result := ProcessingResult{
		EventID:     event.EventID,
		UserID:      event.GetTargetID(),
		ProcessedAt: startTime,
	}

	s.logger.Infof("grnoti: processing event %s (target=%s type=%s)", event.EventID, event.GetTargetID(), event.Type)

	if err := event.Validate(); err != nil {
		s.logger.Warnf("grnoti: invalid event %s: %v", event.EventID, err)
		if s.config.SkipInvalidEvents {
			return s.skip(result, startTime, "invalid_event: "+err.Error()), nil
		}
		return result, err
	}

	processed, err := s.idempotency.IsProcessed(ctx, event.EventID)
	if err != nil {
		return result, err
	}
	if processed {
		return s.skip(result, startTime, "already_processed"), nil
	}

	if s.preferencesFilter != nil && s.config.EnablePreferencesFilter && event.IsAuthenticated() {
		shouldSend, skipReason, prefErr := s.preferencesFilter.ShouldSendNotification(ctx, event)
		if prefErr != nil {
			s.logger.Warnf("grnoti: preferences check failed for event %s, proceeding anyway: %v", event.EventID, prefErr)
		} else if !shouldSend {
			return s.skip(result, startTime, "preferences_"+skipReason), nil
		}
	}

	msg, err := s.templates.BuildMessage(event)
	if err != nil {
		return result, err
	}

	dispatchStart := time.Now()
	dispatchResult, dispatchErr := s.dispatch(ctx, event, msg, &result)
	dispatchDuration := time.Since(dispatchStart)

	// A token-based dispatch that resolved zero recipients never called
	// the dispatcher at all (dispatchToTokens's own early return), so
	// dispatchResult is entirely zero-valued here. A topic-based dispatch
	// always has TotalCount() >= 1 (its synthetic 1-success/1-failure
	// result), so this check can't misfire on that path even though it
	// also leaves result.TokenCount at 0.
	if result.TokenCount == 0 && dispatchResult.TotalCount() == 0 && dispatchErr == nil {
		if err := s.idempotency.MarkProcessed(ctx, event.EventID, s.config.IdempotencyTTL); err != nil {
			s.logger.Warnf("grnoti: failed to mark event %s processed: %v", event.EventID, err)
		}
		return s.skip(result, startTime, "no_active_tokens"), nil
	}

	if dispatchErr != nil {
		// Matches the reference: a dispatch-level error doesn't abort the
		// pipeline. dispatchResult (returned alongside it per
		// PushDispatcher.Send's contract) still carries whatever
		// partial/total-failure detail there is, so DLQ/metrics/mark-
		// processed below all still run on real data instead of being
		// skipped outright.
		s.logger.Errorf("grnoti: dispatch failed for event %s: %v", event.EventID, dispatchErr)
	}
	result.DispatchResult = dispatchResult

	s.markInvalidTokens(ctx, event, dispatchResult)
	s.publishToDLQIfUnresolved(ctx, event, dispatchResult)
	s.recordMetrics(event, dispatchResult, dispatchDuration)
	s.publishLifecycleEvent(ctx, event, dispatchResult)

	if err := s.idempotency.MarkProcessed(ctx, event.EventID, s.config.IdempotencyTTL); err != nil {
		s.logger.Warnf("grnoti: failed to mark event %s processed: %v", event.EventID, err)
	}

	result.Duration = time.Since(startTime)
	if s.metrics != nil && s.config.EnableMetrics {
		s.metrics.ObserveProcessingLatency(result.Duration)
	}
	s.logger.Infof("grnoti: processed event %s: tokens=%d success=%d failure=%d duration=%s",
		event.EventID, result.TokenCount, dispatchResult.SuccessCount, dispatchResult.FailureCount, result.Duration)

	return result, nil
}

// skip finalizes result as a skipped outcome and reports it via Metrics.
func (s *notificationService) skip(result ProcessingResult, startTime time.Time, reason string) ProcessingResult {
	result.Skipped = true
	result.SkipReason = reason
	result.Duration = time.Since(startTime)
	if s.metrics != nil && s.config.EnableMetrics {
		s.metrics.IncEventsSkipped(reason)
	}
	s.logger.Infof("grnoti: skipped event %s: %s", result.EventID, reason)
	return result
}

// dispatch resolves event's target (topic or tokens, honoring TopicRouter
// when configured) and sends msg, updating result.TokenCount along the
// way. A topic-based dispatch reports a synthetic 1-success/1-failure
// DispatchResult, matching the reference, since FCM's topic-send API
// reports no per-recipient detail at all (see PushDispatcher.SendToTopic's
// own doc comment).
func (s *notificationService) dispatch(ctx context.Context, event Event, msg Message, result *ProcessingResult) (DispatchResult, error) {
	if s.topicRouter != nil && s.config.EnableTopicRouting {
		target, err := s.topicRouter.ResolveTarget(ctx, event)
		if err != nil {
			return DispatchResult{}, err
		}
		if target.IsTopicBased() {
			s.logger.Infof("grnoti: dispatching event %s to topic %s", event.EventID, target.GetTopicName())
			if err := s.dispatcher.SendToTopic(ctx, target.GetTopicName(), msg); err != nil {
				return DispatchResult{FailureCount: 1, Errors: []error{err}}, err
			}
			return DispatchResult{SuccessCount: 1}, nil
		}
		return s.dispatchToTokens(ctx, event, target.GetTokens(), msg, result)
	}

	tokens, err := resolveTokensForEvent(ctx, event, s.tokenStore)
	if err != nil {
		return DispatchResult{}, err
	}
	return s.dispatchToTokens(ctx, event, tokens, msg, result)
}

// dispatchToTokens deduplicates (if configured), records result.TokenCount,
// and sends msg — pre-splitting into Config.MaxTokensPerBatch-sized calls
// when Config.EnforceBatching is set (a coarser, caller-controlled batch
// size distinct from dispatcher.fcm.go's own fixed FCMMaxBatchSize
// internal batching; the two compose, they don't conflict).
func (s *notificationService) dispatchToTokens(ctx context.Context, event Event, tokens []DeviceToken, msg Message, result *ProcessingResult) (DispatchResult, error) {
	if s.config.EnableTokenDeduplication {
		tokens = s.batchSplitter.Deduplicate(tokens)
	}
	result.TokenCount = len(tokens)

	if len(tokens) == 0 {
		return DispatchResult{}, nil
	}

	if !s.config.EnforceBatching || s.config.MaxTokensPerBatch <= 0 {
		return s.dispatcher.Send(ctx, tokens, msg)
	}

	var aggregated DispatchResult
	aggregated.SuccessByPlatform = make(map[Platform]int)
	aggregated.FailureByPlatform = make(map[Platform]int)
	var lastErr error
	for _, batch := range s.batchSplitter.Split(tokens, s.config.MaxTokensPerBatch) {
		batchResult, err := s.dispatcher.Send(ctx, batch, msg)
		if err != nil {
			lastErr = err
		}
		mergeDispatchResult(&aggregated, batchResult)
	}
	return aggregated, lastErr
}

// mergeDispatchResult accumulates src into dst in place.
func mergeDispatchResult(dst *DispatchResult, src DispatchResult) {
	dst.SuccessCount += src.SuccessCount
	dst.FailureCount += src.FailureCount
	dst.InvalidTokens = append(dst.InvalidTokens, src.InvalidTokens...)
	dst.RetryableErrors += src.RetryableErrors
	dst.Errors = append(dst.Errors, src.Errors...)
	for platform, count := range src.SuccessByPlatform {
		dst.SuccessByPlatform[platform] += count
	}
	for platform, count := range src.FailureByPlatform {
		dst.FailureByPlatform[platform] += count
	}
}

func (s *notificationService) markInvalidTokens(ctx context.Context, event Event, dispatchResult DispatchResult) {
	if len(dispatchResult.InvalidTokens) == 0 {
		return
	}
	s.logger.Infof("grnoti: marking %d invalid token(s) for event %s", len(dispatchResult.InvalidTokens), event.EventID)
	for _, token := range dispatchResult.InvalidTokens {
		if err := s.tokenStore.MarkInvalid(ctx, token); err != nil {
			s.logger.Warnf("grnoti: failed to mark token invalid for event %s: %v", event.EventID, err)
		}
	}
}

// publishToDLQIfUnresolved is the §3.6 fix: any dispatch failure not
// already accounted for by an invalid (permanently bad) token is a
// candidate for later retry, so it's published to the DLQ rather than
// silently dropped. unresolved intentionally covers both a partial
// per-token retryable failure and a total request-level dispatch error —
// see dispatch's own doc comment on why a total failure still deserves a
// DLQ entry even when it happens to be non-retryable (e.g. a payload that
// will always be too large): the DLQ's own retry-exhaustion path
// (DLQStatusExhausted) is exactly the "needs a human, not just more
// retries" signal for that case, not a defect in publishing it there.
func (s *notificationService) publishToDLQIfUnresolved(ctx context.Context, event Event, dispatchResult DispatchResult) {
	if s.dlqHandler == nil || !s.config.EnableDLQ {
		return
	}
	unresolved := dispatchResult.FailureCount - len(dispatchResult.InvalidTokens)
	if unresolved <= 0 {
		return
	}
	reason := joinDispatchErrors(dispatchResult.Errors)
	if err := s.dlqHandler.PublishToDLQ(ctx, event, reason); err != nil {
		s.logger.Errorf("grnoti: publish to DLQ failed for event %s: %v", event.EventID, err)
	}
}

func joinDispatchErrors(errs []error) string {
	if len(errs) == 0 {
		return "dispatch failed"
	}
	msg := errs[0].Error()
	if len(errs) > 1 {
		msg = fmt.Sprintf("%s (and %d more)", msg, len(errs)-1)
	}
	return msg
}

// recordMetrics reports per-event/per-platform outcomes. See ServiceDeps.Metrics'
// doc comment for why IncInvalidTokens is deliberately not called here.
func (s *notificationService) recordMetrics(event Event, dispatchResult DispatchResult, dispatchDuration time.Duration) {
	if s.metrics == nil || !s.config.EnableMetrics {
		return
	}
	s.metrics.IncNotificationsProcessed()
	for platform, count := range dispatchResult.SuccessByPlatform {
		if count > 0 {
			s.metrics.IncNotificationsSent(event.Type, platform, count)
			s.metrics.ObserveDispatchLatency(event.Type, platform, dispatchDuration)
		}
	}
	for platform, count := range dispatchResult.FailureByPlatform {
		if count > 0 {
			s.metrics.IncNotificationsFailed(event.Type, platform, count)
		}
	}
}

// publishLifecycleEvent publishes TopicNotificationSent/
// TopicNotificationFailed per PublishSent/PublishFailed's own nil-bus/
// best-effort contract. A dispatch with any success at all counts as
// "sent" (matching graudit's own precedent of publishing on the durable
// outcome, not requiring perfection); a dispatch with zero successes and
// at least one failure counts as "failed". A dispatch with neither (e.g.
// zero tokens resolved) publishes nothing — that's a skip, already
// reported via IncEventsSkipped, not a lifecycle event.
func (s *notificationService) publishLifecycleEvent(ctx context.Context, event Event, dispatchResult DispatchResult) {
	if !s.config.EnableEventBus {
		return
	}
	switch {
	case dispatchResult.SuccessCount > 0:
		PublishSent(ctx, s.bus, s.logger, NotificationSentPayload{
			EventID: event.EventID, UserID: event.UserID, AnonymousID: event.AnonymousID,
			EventType: event.Type, SuccessCount: dispatchResult.SuccessCount, FailureCount: dispatchResult.FailureCount,
		})
	case dispatchResult.FailureCount > 0:
		PublishFailed(ctx, s.bus, s.logger, NotificationFailedPayload{
			EventID: event.EventID, UserID: event.UserID, AnonymousID: event.AnonymousID,
			EventType: event.Type, FailureCount: dispatchResult.FailureCount, Reason: joinDispatchErrors(dispatchResult.Errors),
		})
	}
}
