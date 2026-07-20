// File: service_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gourdian25/grcache/memory"
)

// --- test doubles ---

type stubDispatcher struct {
	mu               sync.Mutex
	sendCalls        [][]DeviceToken
	sendToTopicCalls []string

	SendFunc        func(ctx context.Context, tokens []DeviceToken, msg Message) (DispatchResult, error)
	SendToTokenFunc func(ctx context.Context, token DeviceToken, msg Message) error
	SendToTopicFunc func(ctx context.Context, topic string, msg Message) error
}

func (d *stubDispatcher) Send(ctx context.Context, tokens []DeviceToken, msg Message) (DispatchResult, error) {
	d.mu.Lock()
	d.sendCalls = append(d.sendCalls, tokens)
	d.mu.Unlock()
	if d.SendFunc != nil {
		return d.SendFunc(ctx, tokens, msg)
	}
	return DispatchResult{
		SuccessCount:      len(tokens),
		SuccessByPlatform: map[Platform]int{PlatformAndroid: len(tokens)},
	}, nil
}

func (d *stubDispatcher) SendToToken(ctx context.Context, token DeviceToken, msg Message) error {
	if d.SendToTokenFunc != nil {
		return d.SendToTokenFunc(ctx, token, msg)
	}
	return nil
}

func (d *stubDispatcher) SendToTopic(ctx context.Context, topic string, msg Message) error {
	d.mu.Lock()
	d.sendToTopicCalls = append(d.sendToTopicCalls, topic)
	d.mu.Unlock()
	if d.SendToTopicFunc != nil {
		return d.SendToTopicFunc(ctx, topic, msg)
	}
	return nil
}

func (d *stubDispatcher) sendCallCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sendCalls)
}

type stubMetricsFull struct {
	mu                  sync.Mutex
	processed           int
	sent, failed        int
	invalidTokens       int
	skipped             []string
	dispatchLatencies   int
	processingLatencies int
}

func (m *stubMetricsFull) IncNotificationsProcessed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
}
func (m *stubMetricsFull) IncNotificationsSent(EventType, Platform, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent++
}
func (m *stubMetricsFull) IncNotificationsFailed(EventType, Platform, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
}
func (m *stubMetricsFull) IncInvalidTokens(int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invalidTokens++
}
func (m *stubMetricsFull) IncEventsSkipped(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skipped = append(m.skipped, reason)
}
func (m *stubMetricsFull) ObserveDispatchLatency(EventType, Platform, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchLatencies++
}
func (m *stubMetricsFull) ObserveProcessingLatency(time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processingLatencies++
}

var _ Metrics = (*stubMetricsFull)(nil)

// blockingPreferencesFilter fails the test if ShouldSendNotification is
// ever called — used to prove idempotency short-circuits before it.
type blockingPreferencesFilter struct{ t *testing.T }

func (f *blockingPreferencesFilter) ShouldSendNotification(context.Context, Event) (bool, string, error) {
	f.t.Helper()
	f.t.Fatal("ShouldSendNotification called for an already-processed event — idempotency must short-circuit first")
	return false, "", nil
}

func newTestIdempotencyStore(t *testing.T) IdempotencyStore {
	t.Helper()
	cache, err := memory.NewMemoryCache()
	if err != nil {
		t.Fatalf("memory.NewMemoryCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return NewCacheIdempotencyStore(cache)
}

func baseServiceDeps(t *testing.T) ServiceDeps {
	t.Helper()
	return ServiceDeps{
		TokenStore:  NewMemoryTokenStore(),
		Dispatcher:  &stubDispatcher{},
		Templates:   NewTemplateEngine(),
		Idempotency: newTestIdempotencyStore(t),
		Config:      ServiceConfig{IdempotencyTTL: time.Hour},
	}
}

func validEvent(id string) Event {
	return Event{EventID: id, UserID: "u1", Type: EventTypeSystemAlert, Priority: PriorityNormal}
}

// --- construction ---

func TestNewNotificationService_RequiredDeps(t *testing.T) {
	full := baseServiceDeps(t)

	cases := []struct {
		name string
		deps ServiceDeps
	}{
		{"missing TokenStore", ServiceDeps{Dispatcher: full.Dispatcher, Templates: full.Templates, Idempotency: full.Idempotency}},
		{"missing Dispatcher", ServiceDeps{TokenStore: full.TokenStore, Templates: full.Templates, Idempotency: full.Idempotency}},
		{"missing Templates", ServiceDeps{TokenStore: full.TokenStore, Dispatcher: full.Dispatcher, Idempotency: full.Idempotency}},
		{"missing Idempotency", ServiceDeps{TokenStore: full.TokenStore, Dispatcher: full.Dispatcher, Templates: full.Templates}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewNotificationService(tc.deps); err == nil {
				t.Fatalf("NewNotificationService(%s) = nil error, want non-nil", tc.name)
			}
		})
	}
}

func TestNewNotificationService_MinimalDepsSucceed(t *testing.T) {
	svc, err := NewNotificationService(baseServiceDeps(t))
	if err != nil {
		t.Fatalf("NewNotificationService: %v", err)
	}
	defer svc.Close()
}

// --- happy path ---

func TestProcessEvent_HappyPath(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1", Platform: PlatformAndroid})
	svc, err := NewNotificationService(deps)
	if err != nil {
		t.Fatalf("NewNotificationService: %v", err)
	}
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.Skipped {
		t.Fatalf("ProcessEvent() = %+v, want not skipped", result)
	}
	if result.TokenCount != 1 || result.DispatchResult.SuccessCount != 1 {
		t.Fatalf("ProcessEvent() = %+v, want TokenCount=1 SuccessCount=1", result)
	}

	processed, err := deps.Idempotency.IsProcessed(context.Background(), "e1")
	if err != nil || !processed {
		t.Fatalf("IsProcessed(e1) = (%v, %v), want (true, nil) after ProcessEvent", processed, err)
	}
}

// --- validation ---

func TestProcessEvent_InvalidEvent_PropagatesByDefault(t *testing.T) {
	svc, _ := NewNotificationService(baseServiceDeps(t))
	defer svc.Close()
	_, err := svc.ProcessEvent(context.Background(), Event{}) // no EventID
	if err != ErrInvalidEventID {
		t.Fatalf("ProcessEvent(invalid) error = %v, want ErrInvalidEventID", err)
	}
}

func TestProcessEvent_InvalidEvent_SkippedWhenConfigured(t *testing.T) {
	deps := baseServiceDeps(t)
	deps.Config.SkipInvalidEvents = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), Event{})
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if !result.Skipped || result.SkipReason == "" {
		t.Fatalf("ProcessEvent(invalid, SkipInvalidEvents=true) = %+v, want Skipped with a reason", result)
	}
}

// --- ordering: idempotency before preferences ---

func TestProcessEvent_IdempotencyShortCircuitsBeforePreferences(t *testing.T) {
	deps := baseServiceDeps(t)
	deps.PreferencesFilter = &blockingPreferencesFilter{t: t}
	deps.Config.EnablePreferencesFilter = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	ctx := context.Background()
	if err := deps.Idempotency.MarkProcessed(ctx, "e1", time.Hour); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	result, err := svc.ProcessEvent(ctx, validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if !result.Skipped || result.SkipReason != "already_processed" {
		t.Fatalf("ProcessEvent(duplicate) = %+v, want Skipped/already_processed", result)
	}
	// blockingPreferencesFilter itself fails the test if reached — an
	// explicit reachability check here would be redundant, but a passing
	// test with no t.Fatal from it IS the assertion.
}

// --- preferences ---

type stubPreferencesFilter struct {
	shouldSend bool
	reason     string
	err        error
}

func (f *stubPreferencesFilter) ShouldSendNotification(context.Context, Event) (bool, string, error) {
	return f.shouldSend, f.reason, f.err
}

func TestProcessEvent_PreferencesBlocks(t *testing.T) {
	deps := baseServiceDeps(t)
	deps.PreferencesFilter = &stubPreferencesFilter{shouldSend: false, reason: "quiet_hours"}
	deps.Config.EnablePreferencesFilter = true
	dispatcher := deps.Dispatcher.(*stubDispatcher)
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if !result.Skipped || result.SkipReason != "preferences_quiet_hours" {
		t.Fatalf("ProcessEvent(blocked) = %+v, want Skipped/preferences_quiet_hours", result)
	}
	if dispatcher.sendCallCount() != 0 {
		t.Fatalf("dispatcher.Send called %d times, want 0 (blocked by preferences)", dispatcher.sendCallCount())
	}
}

func TestProcessEvent_PreferencesFailsOpen(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	deps.PreferencesFilter = &stubPreferencesFilter{err: errors.New("preferences store unreachable")}
	deps.Config.EnablePreferencesFilter = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.Skipped {
		t.Fatalf("ProcessEvent(preferences error) = %+v, want NOT skipped (fail open)", result)
	}
}

// --- no tokens ---

func TestProcessEvent_NoActiveTokens(t *testing.T) {
	deps := baseServiceDeps(t)
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if !result.Skipped {
		t.Fatalf("ProcessEvent(no tokens) = %+v, want Skipped", result)
	}
	processed, _ := deps.Idempotency.IsProcessed(context.Background(), "e1")
	if !processed {
		t.Fatal("event not marked processed after no-tokens skip — reprocessing would repeat the no-op forever")
	}
}

// --- direct tokens ---

func TestProcessEvent_DirectTokens_BypassesTokenStore(t *testing.T) {
	deps := baseServiceDeps(t)
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	event := Event{EventID: "e1", Type: EventTypeSystemAlert, Priority: PriorityNormal, DeviceTokens: []string{"direct-1", "direct-2"}}
	result, err := svc.ProcessEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.TokenCount != 2 || result.DispatchResult.SuccessCount != 2 {
		t.Fatalf("ProcessEvent(direct tokens) = %+v, want TokenCount=2 SuccessCount=2", result)
	}
}

// --- anonymous events (topicrouter.go regression, exercised end-to-end) ---

func TestProcessEvent_AnonymousEvent(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", AnonymousID: "a1"})
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	event := Event{EventID: "e1", AnonymousID: "a1", Type: EventTypeSystemAlert, Priority: PriorityNormal}
	result, err := svc.ProcessEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.TokenCount != 1 {
		t.Fatalf("ProcessEvent(anonymous) TokenCount = %d, want 1", result.TokenCount)
	}
}

// --- topic routing ---

func TestProcessEvent_TopicRouting_SendsToTopicNotTokens(t *testing.T) {
	deps := baseServiceDeps(t)
	deps.TopicRouter = NewStaticTopicRouter("broadcast-topic")
	deps.Config.EnableTopicRouting = true
	dispatcher := deps.Dispatcher.(*stubDispatcher)
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.DispatchResult.SuccessCount != 1 {
		t.Fatalf("ProcessEvent(topic) DispatchResult = %+v, want SuccessCount=1", result.DispatchResult)
	}
	if dispatcher.sendCallCount() != 0 {
		t.Fatalf("dispatcher.Send called %d times, want 0 (topic-based dispatch)", dispatcher.sendCallCount())
	}
	dispatcher.mu.Lock()
	topicCalls := dispatcher.sendToTopicCalls
	dispatcher.mu.Unlock()
	if len(topicCalls) != 1 || topicCalls[0] != "broadcast-topic" {
		t.Fatalf("sendToTopicCalls = %v, want [broadcast-topic]", topicCalls)
	}
}

// --- DLQ wiring (§3.6) ---

func TestProcessEvent_UnresolvedFailure_PublishesToDLQ(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	dispatcher := &stubDispatcher{SendFunc: func(context.Context, []DeviceToken, Message) (DispatchResult, error) {
		return DispatchResult{FailureCount: 1, Errors: []error{errors.New("fcm unavailable")}}, nil
	}}
	deps.Dispatcher = dispatcher
	dlq := NewMemoryDLQHandler(3, 0, time.Second)
	deps.DLQHandler = dlq
	deps.Config.EnableDLQ = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	got, err := dlq.GetEventByID(context.Background(), "e1")
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if got.FailureReason != "fcm unavailable" {
		t.Fatalf("DLQ entry FailureReason = %q, want %q", got.FailureReason, "fcm unavailable")
	}
}

func TestProcessEvent_InvalidTokenOnly_DoesNotPublishToDLQ(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "bad-token", UserID: "u1"})
	dispatcher := &stubDispatcher{SendFunc: func(_ context.Context, tokens []DeviceToken, _ Message) (DispatchResult, error) {
		return DispatchResult{FailureCount: 1, InvalidTokens: []string{"bad-token"}}, nil
	}}
	deps.Dispatcher = dispatcher
	dlq := NewMemoryDLQHandler(3, 0, time.Second)
	deps.DLQHandler = dlq
	deps.Config.EnableDLQ = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	if _, err := dlq.GetEventByID(context.Background(), "e1"); err != ErrDLQEventNotFound {
		t.Fatalf("GetEventByID error = %v, want ErrDLQEventNotFound (an all-invalid-token failure shouldn't reach the DLQ)", err)
	}

	tokens, err := deps.TokenStore.GetActiveTokens(context.Background(), "u1")
	if err != nil {
		t.Fatalf("GetActiveTokens: %v", err)
	}
	if len(tokens) != 0 {
		t.Fatalf("GetActiveTokens after invalid-token dispatch = %v, want empty (token marked invalid)", tokens)
	}
}

func TestProcessEvent_DLQDisabled_NoPublish(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	deps.Dispatcher = &stubDispatcher{SendFunc: func(context.Context, []DeviceToken, Message) (DispatchResult, error) {
		return DispatchResult{FailureCount: 1, Errors: []error{errors.New("boom")}}, nil
	}}
	dlq := NewMemoryDLQHandler(3, 0, time.Second)
	deps.DLQHandler = dlq
	deps.Config.EnableDLQ = false // explicitly disabled despite a DLQHandler being wired
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if _, err := dlq.GetEventByID(context.Background(), "e1"); err != ErrDLQEventNotFound {
		t.Fatalf("GetEventByID error = %v, want ErrDLQEventNotFound (EnableDLQ=false)", err)
	}
}

// --- lifecycle events (grevents) ---

func TestProcessEvent_EventBus_PublishesSentOnSuccess(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	bus := &stubBus{}
	deps.EventBus = bus
	deps.Config.EnableEventBus = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	events := bus.publishedEvents()
	if len(events) != 1 || events[0].Topic != TopicNotificationSent {
		t.Fatalf("published events = %+v, want exactly 1 on topic %q", events, TopicNotificationSent)
	}
}

func TestProcessEvent_EventBus_PublishesFailedOnTotalFailure(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	deps.Dispatcher = &stubDispatcher{SendFunc: func(context.Context, []DeviceToken, Message) (DispatchResult, error) {
		return DispatchResult{FailureCount: 1, Errors: []error{errors.New("boom")}}, nil
	}}
	bus := &stubBus{}
	deps.EventBus = bus
	deps.Config.EnableEventBus = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	events := bus.publishedEvents()
	if len(events) != 1 || events[0].Topic != TopicNotificationFailed {
		t.Fatalf("published events = %+v, want exactly 1 on topic %q", events, TopicNotificationFailed)
	}
}

func TestProcessEvent_EventBus_DisabledPublishesNothing(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	bus := &stubBus{}
	deps.EventBus = bus
	deps.Config.EnableEventBus = false
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if got := len(bus.publishedEvents()); got != 0 {
		t.Fatalf("published event count = %d, want 0 (EnableEventBus=false)", got)
	}
}

// --- metrics ---

func TestProcessEvent_Metrics(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	m := &stubMetricsFull{}
	deps.Metrics = m
	deps.Config.EnableMetrics = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processed != 1 {
		t.Fatalf("IncNotificationsProcessed calls = %d, want 1", m.processed)
	}
	if m.sent != 1 {
		t.Fatalf("IncNotificationsSent calls = %d, want 1", m.sent)
	}
	if m.dispatchLatencies != 1 {
		t.Fatalf("ObserveDispatchLatency calls = %d, want 1", m.dispatchLatencies)
	}
	if m.processingLatencies != 1 {
		t.Fatalf("ObserveProcessingLatency calls = %d, want 1", m.processingLatencies)
	}
	if m.invalidTokens != 0 {
		t.Fatalf("IncInvalidTokens calls = %d, want 0 (service.go must not double-count what the dispatcher already reports)", m.invalidTokens)
	}
}

func TestProcessEvent_Metrics_SkippedEventRecordsSkip(t *testing.T) {
	deps := baseServiceDeps(t)
	m := &stubMetricsFull{}
	deps.Metrics = m
	deps.Config.EnableMetrics = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil { // no tokens saved -> skip
		t.Fatalf("ProcessEvent: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.skipped) != 1 || m.skipped[0] != "no_active_tokens" {
		t.Fatalf("IncEventsSkipped calls = %v, want [no_active_tokens]", m.skipped)
	}
}

// --- batching ---

func TestProcessEvent_EnforceBatching_SplitsAcrossMultipleSendCalls(t *testing.T) {
	deps := baseServiceDeps(t)
	dispatcher := &stubDispatcher{}
	deps.Dispatcher = dispatcher
	deps.Config.EnforceBatching = true
	deps.Config.MaxTokensPerBatch = 2
	for i := 0; i < 5; i++ {
		_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: fmt.Sprintf("t%d", i), UserID: "u1"})
	}
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if result.DispatchResult.SuccessCount != 5 {
		t.Fatalf("aggregated SuccessCount = %d, want 5", result.DispatchResult.SuccessCount)
	}
	if got := dispatcher.sendCallCount(); got != 3 { // 2+2+1
		t.Fatalf("dispatcher.Send call count = %d, want 3 (5 tokens split at MaxTokensPerBatch=2)", got)
	}
}

func TestProcessEvent_WithoutEnforceBatching_SendsOnce(t *testing.T) {
	deps := baseServiceDeps(t)
	dispatcher := &stubDispatcher{}
	deps.Dispatcher = dispatcher
	deps.Config.MaxTokensPerBatch = 2 // set but EnforceBatching left false
	for i := 0; i < 5; i++ {
		_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: fmt.Sprintf("t%d", i), UserID: "u1"})
	}
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	if got := dispatcher.sendCallCount(); got != 1 {
		t.Fatalf("dispatcher.Send call count = %d, want 1 (EnforceBatching disabled)", got)
	}
}

// --- backpressure / WorkerPool ownership / Submit ---

func TestSubmit_WithoutBackpressure_BehavesLikeProcessEvent(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	if err := svc.Submit(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	processed, _ := deps.Idempotency.IsProcessed(context.Background(), "e1")
	if !processed {
		t.Fatal("Submit (no backpressure) did not process the event synchronously")
	}
}

func TestSubmit_WithBackpressure_ProcessesAsyncViaWorkerPool(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	deps.Config.EnableBackpressure = true
	deps.WorkerPoolConfig = WorkerPoolConfig{Workers: 2, QueueSize: 10}
	svc, err := NewNotificationService(deps)
	if err != nil {
		t.Fatalf("NewNotificationService: %v", err)
	}
	defer svc.Close()

	if err := svc.Submit(context.Background(), validEvent("e1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processed, _ := deps.Idempotency.IsProcessed(context.Background(), "e1"); processed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("event was not processed by the internal WorkerPool within the deadline")
}

func TestProcessEvent_AlwaysSynchronousRegardlessOfBackpressure(t *testing.T) {
	deps := baseServiceDeps(t)
	_ = deps.TokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1"})
	deps.Config.EnableBackpressure = true
	svc, _ := NewNotificationService(deps)
	defer svc.Close()

	result, err := svc.ProcessEvent(context.Background(), validEvent("e1"))
	if err != nil {
		t.Fatalf("ProcessEvent: %v", err)
	}
	// If this returned before the work was done, TokenCount/DispatchResult
	// would still be zero-valued — proving ProcessEvent bypasses the pool
	// entirely and runs on the calling goroutine.
	if result.DispatchResult.SuccessCount != 1 {
		t.Fatalf("ProcessEvent() = %+v, want a fully-populated synchronous result", result)
	}
}

// --- Close / ErrClosed ---

func TestNotificationService_Close_Idempotent(t *testing.T) {
	svc, _ := NewNotificationService(baseServiceDeps(t))
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}

func TestNotificationService_ProcessEvent_AfterClose(t *testing.T) {
	svc, _ := NewNotificationService(baseServiceDeps(t))
	_ = svc.Close()
	if _, err := svc.ProcessEvent(context.Background(), validEvent("e1")); err != ErrClosed {
		t.Fatalf("ProcessEvent after Close error = %v, want ErrClosed", err)
	}
}

func TestNotificationService_Submit_AfterClose(t *testing.T) {
	svc, _ := NewNotificationService(baseServiceDeps(t))
	_ = svc.Close()
	if err := svc.Submit(context.Background(), validEvent("e1")); err != ErrClosed {
		t.Fatalf("Submit after Close error = %v, want ErrClosed", err)
	}
}
