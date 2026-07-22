// File: dispatcher.fcm_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"firebase.google.com/go/v4/messaging"
)

// fakeFCMClient is a controllable stand-in for the real Firebase Admin SDK
// client — the one deliberate exception to this repo's real-services
// testing policy, since FCM has no local emulator (see FCMClient's doc
// comment in dispatcher.fcm.go).
type fakeFCMClient struct {
	mu sync.Mutex

	multicastCalls   int
	sendCalls        int
	multicastBatches [][]string // tokens seen per SendEachForMulticast call
	multicastConfigs []*messaging.MulticastMessage

	// multicastErr, if set, makes SendEachForMulticast fail outright
	// (simulating a total request failure, not a per-token error).
	multicastErr error
	// perTokenError maps a token to the error FCM should report for it in
	// that batch's response (partial failure).
	perTokenError map[string]error
	// failMulticastUntilCall makes SendEachForMulticast return
	// multicastErr for calls 1..N, then succeed from call N+1 onward —
	// used to prove retry actually recovers.
	failMulticastUntilCall int

	sendErr error
}

// msg.Tokens (not the SDK's newer Fids field) is deliberate throughout this
// file: Fids addresses Firebase Installation IDs, a different concept from
// the FCM registration tokens DeviceToken.Token represents everywhere else
// in this codebase — switching would be an unrequested addressing-scheme
// change, not a like-for-like fix of a deprecation warning.
func (f *fakeFCMClient) SendEachForMulticast(_ context.Context, msg *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	f.mu.Lock()
	f.multicastCalls++
	call := f.multicastCalls
	f.multicastBatches = append(f.multicastBatches, append([]string(nil), msg.Tokens...)) //nolint:staticcheck // see file-level note on Tokens vs Fids above
	f.multicastConfigs = append(f.multicastConfigs, msg)
	f.mu.Unlock()

	if f.multicastErr != nil && call <= f.failMulticastUntilCall {
		return nil, f.multicastErr
	}
	if f.multicastErr != nil && f.failMulticastUntilCall == 0 {
		return nil, f.multicastErr
	}

	resp := &messaging.BatchResponse{}
	for _, tok := range msg.Tokens { //nolint:staticcheck // see file-level note on Tokens vs Fids above
		if err, ok := f.perTokenError[tok]; ok {
			resp.Responses = append(resp.Responses, &messaging.SendResponse{Success: false, Error: err})
			resp.FailureCount++
			continue
		}
		resp.Responses = append(resp.Responses, &messaging.SendResponse{Success: true, MessageID: "msg-" + tok})
		resp.SuccessCount++
	}
	return resp, nil
}

func (f *fakeFCMClient) Send(_ context.Context, msg *messaging.Message) (string, error) {
	f.mu.Lock()
	f.sendCalls++
	f.mu.Unlock()
	if f.sendErr != nil {
		return "", f.sendErr
	}
	return "msg-1", nil
}

func (f *fakeFCMClient) callCount() (multicast, send int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.multicastCalls, f.sendCalls
}

func (f *fakeFCMClient) batches() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.multicastBatches
}

type stubMetrics struct {
	mu            sync.Mutex
	invalidTokens int
}

func (s *stubMetrics) IncNotificationsProcessed()                      {}
func (s *stubMetrics) IncNotificationsSent(EventType, Platform, int)   {}
func (s *stubMetrics) IncNotificationsFailed(EventType, Platform, int) {}
func (s *stubMetrics) IncInvalidTokens(count int) {
	s.mu.Lock()
	s.invalidTokens += count
	s.mu.Unlock()
}
func (s *stubMetrics) IncEventsSkipped(string)                                   {}
func (s *stubMetrics) ObserveDispatchLatency(EventType, Platform, time.Duration) {}
func (s *stubMetrics) ObserveProcessingLatency(time.Duration)                    {}
func (s *stubMetrics) get() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalidTokens
}

// countingRateLimiter proves sendBatch actually calls Wait before hitting
// the client, without depending on ratelimiter.go/ratelimiter.redis.go's
// own timing behavior.
type countingRateLimiter struct {
	waits   atomic.Int32
	waitErr error // if set, Wait returns this instead of incrementing waits/nil
}

func (c *countingRateLimiter) Allow(context.Context) (bool, error) { return true, nil }
func (c *countingRateLimiter) Wait(context.Context) error {
	if c.waitErr != nil {
		return c.waitErr
	}
	c.waits.Add(1)
	return nil
}
func (c *countingRateLimiter) GetStats(context.Context) (RateLimiterStats, error) {
	return RateLimiterStats{}, nil
}

func androidToken(tok string) DeviceToken { return DeviceToken{Token: tok, Platform: PlatformAndroid} }

func TestDefaultFCMDispatcherConfig(t *testing.T) {
	cfg := DefaultFCMDispatcherConfig()
	if !cfg.EnableRetry {
		t.Fatal("DefaultFCMDispatcherConfig().EnableRetry = false, want true")
	}
	if cfg.MaxRetryAttempts <= 0 {
		t.Fatalf("DefaultFCMDispatcherConfig().MaxRetryAttempts = %d, want > 0", cfg.MaxRetryAttempts)
	}
}

func TestFCMDispatcher_NilClient(t *testing.T) {
	if _, err := NewFCMDispatcher(FCMDispatcherDeps{}); err != ErrFCMClientNil {
		t.Fatalf("NewFCMDispatcher(nil client) error = %v, want ErrFCMClientNil", err)
	}
}

func TestFCMDispatcher_InvalidRetryConfig(t *testing.T) {
	_, err := NewFCMDispatcher(FCMDispatcherDeps{
		Client: &fakeFCMClient{},
		Config: FCMDispatcherConfig{EnableRetry: true, MaxRetryAttempts: 0},
	})
	if err == nil {
		t.Fatal("NewFCMDispatcher(EnableRetry=true, MaxRetryAttempts=0) = nil error, want non-nil")
	}
}

func TestFCMDispatcher_Send_EmptyTokens(t *testing.T) {
	d, err := NewFCMDispatcher(FCMDispatcherDeps{Client: &fakeFCMClient{}})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}
	result, err := d.Send(context.Background(), nil, Message{Title: "t"})
	if err != nil {
		t.Fatalf("Send(empty tokens): %v", err)
	}
	if result.TotalCount() != 0 {
		t.Fatalf("Send(empty tokens) result = %+v, want zero", result)
	}
}

func TestFCMDispatcher_Send_PayloadTooLarge(t *testing.T) {
	d, err := NewFCMDispatcher(FCMDispatcherDeps{Client: &fakeFCMClient{}})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}
	hugeData := map[string]string{"blob": strings.Repeat("x", FCMMaxPayloadSize*2)}
	tokens := []DeviceToken{androidToken("t1")}
	result, err := d.Send(context.Background(), tokens, Message{Title: "t", Data: hugeData})
	if !errors.Is(err, ErrFCMPayloadTooLarge) {
		t.Fatalf("Send(oversized payload) error = %v, want ErrFCMPayloadTooLarge", err)
	}
	if result.FailureCount != len(tokens) {
		t.Fatalf("Send(oversized payload) FailureCount = %d, want %d", result.FailureCount, len(tokens))
	}
}

func TestFCMDispatcher_Send_AllSuccess(t *testing.T) {
	client := &fakeFCMClient{}
	d, err := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}
	tokens := []DeviceToken{androidToken("t1"), androidToken("t2"), androidToken("t3")}
	result, err := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.SuccessCount != 3 || result.FailureCount != 0 {
		t.Fatalf("Send() = %+v, want SuccessCount=3 FailureCount=0", result)
	}
}

func TestFCMDispatcher_Send_DeduplicatesTokens(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	tokens := []DeviceToken{androidToken("t1"), androidToken("t1"), androidToken("t2")}
	result, err := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.SuccessCount != 2 {
		t.Fatalf("Send() SuccessCount = %d, want 2 (deduplicated)", result.SuccessCount)
	}
}

func TestFCMDispatcher_Send_ClassifiesInvalidTokensAndReportsMetrics(t *testing.T) {
	client := &fakeFCMClient{perTokenError: map[string]error{
		"bad-token": errors.New("NotRegistered: this token is no longer valid"),
	}}
	metrics := &stubMetrics{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, Metrics: metrics})

	tokens := []DeviceToken{androidToken("good-token"), androidToken("bad-token")}
	result, err := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(result.InvalidTokens) != 1 || result.InvalidTokens[0] != "bad-token" {
		t.Fatalf("Send() InvalidTokens = %v, want [bad-token]", result.InvalidTokens)
	}
	if got := metrics.get(); got != 1 {
		t.Fatalf("Metrics.IncInvalidTokens total = %d, want 1", got)
	}
}

func TestFCMDispatcher_Send_RetriesRetryableErrorsAndRecovers(t *testing.T) {
	client := &fakeFCMClient{multicastErr: errors.New("unavailable: try again"), failMulticastUntilCall: 1}
	d, err := NewFCMDispatcher(FCMDispatcherDeps{
		Client: client,
		Config: FCMDispatcherConfig{EnableRetry: true, MaxRetryAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}
	tokens := []DeviceToken{androidToken("t1")}
	result, sendErr := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if sendErr != nil {
		t.Fatalf("Send: %v", sendErr)
	}
	if result.SuccessCount != 1 {
		t.Fatalf("Send() SuccessCount = %d, want 1 (should recover after retry)", result.SuccessCount)
	}
	multicastCalls, _ := client.callCount()
	if multicastCalls < 2 {
		t.Fatalf("multicast calls = %d, want >= 2 (first attempt fails, retry succeeds)", multicastCalls)
	}
}

func TestFCMDispatcher_Send_NoRetryWhenDisabled(t *testing.T) {
	client := &fakeFCMClient{perTokenError: map[string]error{"t1": errors.New("unavailable")}}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, Config: FCMDispatcherConfig{EnableRetry: false}})
	tokens := []DeviceToken{androidToken("t1")}
	if _, err := d.Send(context.Background(), tokens, Message{Title: "hi"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	multicastCalls, _ := client.callCount()
	if multicastCalls != 1 {
		t.Fatalf("multicast calls = %d, want exactly 1 (retry disabled)", multicastCalls)
	}
}

func TestFCMDispatcher_Send_BatchesAtFCMMaxBatchSize(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})

	const totalTokens = FCMMaxBatchSize + 200
	tokens := make([]DeviceToken, totalTokens)
	for i := range tokens {
		tokens[i] = androidToken(fmt.Sprintf("t-%d", i))
	}
	result, err := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.SuccessCount != totalTokens {
		t.Fatalf("Send() SuccessCount = %d, want %d", result.SuccessCount, totalTokens)
	}

	batches := client.batches()
	if len(batches) != 2 {
		t.Fatalf("multicast call count = %d, want 2 batches for %d tokens at max %d", len(batches), totalTokens, FCMMaxBatchSize)
	}
	for _, b := range batches {
		if len(b) > FCMMaxBatchSize {
			t.Fatalf("batch size = %d, want <= %d", len(b), FCMMaxBatchSize)
		}
	}
}

func TestFCMDispatcher_Send_GroupsTokensByPlatform(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})

	tokens := []DeviceToken{
		{Token: "a1", Platform: PlatformAndroid},
		{Token: "i1", Platform: PlatformIOS},
		{Token: "w1", Platform: PlatformWeb},
	}
	if _, err := d.Send(context.Background(), tokens, Message{Title: "hi"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	client.mu.Lock()
	configs := client.multicastConfigs
	client.mu.Unlock()
	if len(configs) != 3 {
		t.Fatalf("multicast call count = %d, want 3 (one per platform group)", len(configs))
	}
	var sawAndroid, sawAPNS, sawWebpush bool
	for _, cfg := range configs {
		if cfg.Android != nil {
			sawAndroid = true
		}
		if cfg.APNS != nil {
			sawAPNS = true
		}
		if cfg.Webpush != nil {
			sawWebpush = true
		}
	}
	if !sawAndroid || !sawAPNS || !sawWebpush {
		t.Fatalf("platform configs seen: android=%v apns=%v webpush=%v, want all true", sawAndroid, sawAPNS, sawWebpush)
	}
}

func TestFCMDispatcher_Send_PopulatesPerPlatformBreakdown(t *testing.T) {
	client := &fakeFCMClient{perTokenError: map[string]error{"i1": errors.New("NotRegistered")}}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})

	tokens := []DeviceToken{
		{Token: "a1", Platform: PlatformAndroid},
		{Token: "a2", Platform: PlatformAndroid},
		{Token: "i1", Platform: PlatformIOS},
	}
	result, err := d.Send(context.Background(), tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.SuccessByPlatform[PlatformAndroid] != 2 {
		t.Fatalf("SuccessByPlatform[android] = %d, want 2", result.SuccessByPlatform[PlatformAndroid])
	}
	if result.FailureByPlatform[PlatformIOS] != 1 {
		t.Fatalf("FailureByPlatform[ios] = %d, want 1", result.FailureByPlatform[PlatformIOS])
	}
	if result.SuccessByPlatform[PlatformIOS] != 0 || result.FailureByPlatform[PlatformAndroid] != 0 {
		t.Fatalf("cross-platform counts leaked: SuccessByPlatform=%v FailureByPlatform=%v", result.SuccessByPlatform, result.FailureByPlatform)
	}
}

func TestFCMDispatcher_Send_RateLimiterGatesEachBatch(t *testing.T) {
	client := &fakeFCMClient{}
	rl := &countingRateLimiter{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, RateLimiter: rl})

	const totalTokens = FCMMaxBatchSize + 1 // forces 2 batches
	tokens := make([]DeviceToken, totalTokens)
	for i := range tokens {
		tokens[i] = androidToken(fmt.Sprintf("t-%d", i))
	}
	if _, err := d.Send(context.Background(), tokens, Message{Title: "hi"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := rl.waits.Load(); got != 2 {
		t.Fatalf("RateLimiter.Wait call count = %d, want 2 (one per batch)", got)
	}
}

func TestFCMDispatcher_Send_CircuitBreakerOpensAndShortCircuits(t *testing.T) {
	client := &fakeFCMClient{multicastErr: errors.New("internal server error")}
	cb, err := NewCircuitBreaker(2, time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}
	d, err := NewFCMDispatcher(FCMDispatcherDeps{Client: client, CircuitBreaker: cb, Config: FCMDispatcherConfig{EnableRetry: false}})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}

	// Two failures trip the breaker (maxFailures=2). Send's contract is
	// that per-token/per-batch failures surface via DispatchResult, not
	// the returned error (that's reserved for "couldn't attempt delivery
	// at all", e.g. payload-too-large) — so assert on FailureCount here,
	// not the returned err.
	for i := 0; i < 2; i++ {
		result, err := d.Send(context.Background(), []DeviceToken{androidToken(fmt.Sprintf("t%d", i))}, Message{Title: "hi"})
		if err != nil {
			t.Fatalf("Send #%d: unexpected top-level error: %v", i, err)
		}
		if result.FailureCount != 1 {
			t.Fatalf("Send #%d: FailureCount = %d, want 1 (client always errors)", i, result.FailureCount)
		}
	}
	multicastCallsAfterTrip, _ := client.callCount()

	// The breaker should now be open: a further Send must not reach the client at all.
	result, err := d.Send(context.Background(), []DeviceToken{androidToken("t-blocked")}, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send() after breaker trip: unexpected top-level error: %v", err)
	}
	if len(result.Errors) == 0 || !errors.Is(result.Errors[0], ErrCircuitOpen) {
		t.Fatalf("Send() after breaker trip: result.Errors=%v, want ErrCircuitOpen", result.Errors)
	}
	multicastCallsAfterBlocked, _ := client.callCount()
	if multicastCallsAfterBlocked != multicastCallsAfterTrip {
		t.Fatalf("multicast calls after breaker opened: before=%d after=%d, want unchanged (short-circuited)", multicastCallsAfterTrip, multicastCallsAfterBlocked)
	}
}

func TestFCMDispatcher_SendToToken_Success(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	if err := d.SendToToken(context.Background(), androidToken("t1"), Message{Title: "hi"}); err != nil {
		t.Fatalf("SendToToken: %v", err)
	}
	_, sendCalls := client.callCount()
	if sendCalls != 1 {
		t.Fatalf("Send call count = %d, want 1", sendCalls)
	}
}

func TestFCMDispatcher_SendToToken_ClassifiesError(t *testing.T) {
	client := &fakeFCMClient{sendErr: errors.New("NotRegistered")}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	err := d.SendToToken(context.Background(), androidToken("t1"), Message{Title: "hi"})
	var fcmErr *FCMError
	if !errors.As(err, &fcmErr) {
		t.Fatalf("SendToToken error = %v, want an *FCMError", err)
	}
	if fcmErr.Code != FCMErrorCodeUnregistered {
		t.Fatalf("FCMError.Code = %s, want %s", fcmErr.Code, FCMErrorCodeUnregistered)
	}
	if !fcmErr.IsPermanent() {
		t.Fatal("FCMError.IsPermanent() = false, want true for an unregistered token")
	}
}

func TestFCMDispatcher_SendToTopic_CircuitBreakerWraps(t *testing.T) {
	client := &fakeFCMClient{sendErr: errors.New("boom")}
	cb, _ := NewCircuitBreaker(1, time.Hour, time.Hour)
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, CircuitBreaker: cb})

	if err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi"}); err == nil {
		t.Fatal("SendToTopic(client error) = nil error, want non-nil")
	}
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("circuit breaker State() after 1 failure (MaxFailures=1) = %s, want %s", got, CircuitStateOpen)
	}
	// A second call must be short-circuited by the now-open breaker instead
	// of reaching the client again.
	if err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi"}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("SendToTopic (breaker open) error = %v, want ErrCircuitOpen", err)
	}
}

func TestFCMDispatcher_SendToToken_CircuitBreakerWraps(t *testing.T) {
	client := &fakeFCMClient{sendErr: errors.New("boom")}
	cb, _ := NewCircuitBreaker(1, time.Hour, time.Hour)
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, CircuitBreaker: cb})

	_ = d.SendToToken(context.Background(), androidToken("t1"), Message{Title: "hi"})
	if got := cb.State(); got != CircuitStateOpen {
		t.Fatalf("circuit breaker State() after 1 failure (MaxFailures=1) = %s, want %s", got, CircuitStateOpen)
	}
}

func TestFCMDispatcher_SendToTopic_Success(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	if err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi"}); err != nil {
		t.Fatalf("SendToTopic: %v", err)
	}
}

func TestFCMDispatcher_SendToToken_RateLimiterError(t *testing.T) {
	client := &fakeFCMClient{}
	limiter := &countingRateLimiter{waitErr: errors.New("rate limited")}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, RateLimiter: limiter})
	err := d.SendToToken(context.Background(), androidToken("t1"), Message{Title: "hi"})
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("SendToToken error = %v, want it to wrap the rate limiter error", err)
	}
	if _, sendCalls := client.callCount(); sendCalls != 0 {
		t.Fatalf("Send call count = %d, want 0 (never reached client)", sendCalls)
	}
}

func TestFCMDispatcher_SendToTopic_RateLimiterError(t *testing.T) {
	client := &fakeFCMClient{}
	limiter := &countingRateLimiter{waitErr: errors.New("rate limited")}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client, RateLimiter: limiter})
	err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi"})
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("SendToTopic error = %v, want it to wrap the rate limiter error", err)
	}
	if _, sendCalls := client.callCount(); sendCalls != 0 {
		t.Fatalf("Send call count = %d, want 0 (never reached client)", sendCalls)
	}
}

func TestFCMDispatcher_SendToTopic_PayloadTooLarge(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	hugeData := map[string]string{"blob": strings.Repeat("x", FCMMaxPayloadSize*2)}
	if err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi", Data: hugeData}); !errors.Is(err, ErrFCMPayloadTooLarge) {
		t.Fatalf("SendToTopic(oversized) error = %v, want ErrFCMPayloadTooLarge", err)
	}
}

func TestFCMDispatcher_Send_ContextCanceledStopsFurtherBatches(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before Send even starts

	const totalTokens = FCMMaxBatchSize + 1
	tokens := make([]DeviceToken, totalTokens)
	for i := range tokens {
		tokens[i] = androidToken(fmt.Sprintf("t-%d", i))
	}
	result, err := d.Send(ctx, tokens, Message{Title: "hi"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.FailureCount != totalTokens {
		t.Fatalf("Send() with pre-canceled ctx FailureCount = %d, want %d (all batches skipped)", result.FailureCount, totalTokens)
	}
	multicastCalls, _ := client.callCount()
	if multicastCalls != 0 {
		t.Fatalf("multicast calls = %d, want 0 (ctx already canceled)", multicastCalls)
	}
}

func newTestFCMDispatcher(t *testing.T) *fcmDispatcher {
	t.Helper()
	d, err := NewFCMDispatcher(FCMDispatcherDeps{Client: &fakeFCMClient{}})
	if err != nil {
		t.Fatalf("NewFCMDispatcher: %v", err)
	}
	return d.(*fcmDispatcher)
}

func TestBuildAndroidConfig_AllOptionalFields(t *testing.T) {
	d := newTestFCMDispatcher(t)
	badge := 3
	msg := Message{
		Title: "t", Body: "b", Priority: PriorityHigh, TTL: time.Hour,
		CollapseKey: "ck", ChannelID: "chan", Sound: "chime", Badge: &badge,
		DeepLink: "app://open", Category: CategoryAlert,
		Actions: []NotificationAction{{ID: "a1", Title: "Open"}},
	}
	cfg := d.buildAndroidConfig(msg)
	if cfg.Priority != "high" {
		t.Errorf("Priority = %q, want high", cfg.Priority)
	}
	if cfg.CollapseKey != "ck" {
		t.Errorf("CollapseKey = %q, want ck", cfg.CollapseKey)
	}
	if cfg.Notification.ChannelID != "chan" || cfg.Notification.Sound != "chime" {
		t.Errorf("Notification = %+v, want ChannelID=chan Sound=chime", cfg.Notification)
	}
	if cfg.Data["deep_link"] != "app://open" || cfg.Data["click_action"] != "app://open" {
		t.Errorf("Data = %+v, want deep_link/click_action = app://open", cfg.Data)
	}
	if cfg.Data["category"] != "alert" {
		t.Errorf("Data[category] = %q, want alert", cfg.Data["category"])
	}
	if cfg.Data["actions"] == "" {
		t.Error("Data[actions] is empty, want marshaled actions JSON")
	}
}

func TestBuildAndroidConfig_DefaultsWhenUnset(t *testing.T) {
	d := newTestFCMDispatcher(t)
	cfg := d.buildAndroidConfig(Message{Title: "t", Body: "b"})
	if cfg.Priority != "normal" {
		t.Errorf("Priority = %q, want normal", cfg.Priority)
	}
	if *cfg.TTL != DefaultTTL {
		t.Errorf("TTL = %v, want DefaultTTL", *cfg.TTL)
	}
	if cfg.Data != nil {
		t.Errorf("Data = %+v, want nil when no deep link/actions/category set", cfg.Data)
	}
}

func TestBuildAPNSConfig_AllOptionalFields(t *testing.T) {
	d := newTestFCMDispatcher(t)
	badge := 5
	msg := Message{
		Title: "t", Body: "b", Priority: PriorityHigh, TTL: time.Hour,
		CollapseKey: "ck", Sound: "chime", Badge: &badge,
		DeepLink: "app://open", Category: CategoryAlert,
		Actions: []NotificationAction{{ID: "a1", Title: "Open"}},
	}
	cfg := d.buildAPNSConfig(msg)
	if cfg.Headers["apns-priority"] != "10" {
		t.Errorf(`Headers["apns-priority"] = %q, want "10"`, cfg.Headers["apns-priority"])
	}
	if cfg.Headers["apns-collapse-id"] != "ck" {
		t.Errorf(`Headers["apns-collapse-id"] = %q, want "ck"`, cfg.Headers["apns-collapse-id"])
	}
	if _, ok := cfg.Headers["apns-expiration"]; !ok {
		t.Error(`Headers["apns-expiration"] missing, want it set when TTL > 0`)
	}
	if cfg.Payload.Aps.Sound != "chime" {
		t.Errorf("Aps.Sound = %v, want chime", cfg.Payload.Aps.Sound)
	}
	if cfg.Payload.Aps.Badge == nil || *cfg.Payload.Aps.Badge != 5 {
		t.Errorf("Aps.Badge = %v, want 5", cfg.Payload.Aps.Badge)
	}
	if cfg.Payload.Aps.Category != "alert" {
		t.Errorf("Aps.Category = %q, want alert", cfg.Payload.Aps.Category)
	}
	if cfg.Payload.CustomData["deep_link"] != "app://open" {
		t.Errorf("CustomData[deep_link] = %v, want app://open", cfg.Payload.CustomData["deep_link"])
	}
	if cfg.Payload.CustomData["actions"] == nil {
		t.Error("CustomData[actions] is nil, want the Actions slice")
	}
}

func TestBuildAPNSConfig_DefaultsWhenUnset(t *testing.T) {
	d := newTestFCMDispatcher(t)
	cfg := d.buildAPNSConfig(Message{Title: "t", Body: "b"})
	if cfg.Headers["apns-priority"] != "5" {
		t.Errorf(`Headers["apns-priority"] = %q, want "5" (normal)`, cfg.Headers["apns-priority"])
	}
	if _, ok := cfg.Headers["apns-expiration"]; ok {
		t.Error(`Headers["apns-expiration"] set, want absent when TTL == 0`)
	}
	if cfg.Payload.Aps.Sound != "default" {
		t.Errorf("Aps.Sound = %v, want the default fallback", cfg.Payload.Aps.Sound)
	}
	if cfg.Payload.CustomData != nil {
		t.Errorf("CustomData = %+v, want nil when no deep link/actions set", cfg.Payload.CustomData)
	}
}

func TestBuildWebpushConfig_UrgencyMapping(t *testing.T) {
	d := newTestFCMDispatcher(t)
	cases := []struct {
		priority Priority
		want     string
	}{
		{PriorityHigh, "high"},
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{"", "normal"},
	}
	for _, tc := range cases {
		cfg := d.buildWebpushConfig(Message{Title: "t", Body: "b", Priority: tc.priority})
		if cfg.Headers["Urgency"] != tc.want {
			t.Errorf("Priority=%q: Headers[Urgency] = %q, want %q", tc.priority, cfg.Headers["Urgency"], tc.want)
		}
	}
}

func TestBuildWebpushConfig_AllOptionalFields(t *testing.T) {
	d := newTestFCMDispatcher(t)
	msg := Message{
		Title: "t", Body: "b", TTL: time.Hour, ImageURL: "https://img", DeepLink: "app://open",
		Category: CategoryAlert, Actions: []NotificationAction{{ID: "a1", Title: "Open", Icon: "icon.png"}},
	}
	cfg := d.buildWebpushConfig(msg)
	if cfg.Headers["TTL"] == "" {
		t.Error(`Headers["TTL"] empty, want it set when TTL > 0`)
	}
	if cfg.Notification.Image != "https://img" {
		t.Errorf("Notification.Image = %q, want https://img", cfg.Notification.Image)
	}
	if len(cfg.Notification.Actions) != 1 || cfg.Notification.Actions[0].Action != "a1" {
		t.Errorf("Notification.Actions = %+v, want one action with Action=a1", cfg.Notification.Actions)
	}
	if cfg.Data["deep_link"] != "app://open" || cfg.Data["category"] != "alert" {
		t.Errorf("Data = %+v, want deep_link=app://open category=alert", cfg.Data)
	}
}

func TestBuildFCMMessage_PerPlatformConfig(t *testing.T) {
	d := newTestFCMDispatcher(t)
	msg := Message{Title: "t", Body: "b", ImageURL: "https://img"}

	android := d.buildFCMMessage("tok", msg, PlatformAndroid)
	if android.Android == nil || android.APNS != nil || android.Webpush != nil {
		t.Fatalf("buildFCMMessage(Android) set the wrong platform config: %+v", android)
	}
	ios := d.buildFCMMessage("tok", msg, PlatformIOS)
	if ios.APNS == nil || ios.Android != nil || ios.Webpush != nil {
		t.Fatalf("buildFCMMessage(iOS) set the wrong platform config: %+v", ios)
	}
	web := d.buildFCMMessage("tok", msg, PlatformWeb)
	if web.Webpush == nil || web.Android != nil || web.APNS != nil {
		t.Fatalf("buildFCMMessage(Web) set the wrong platform config: %+v", web)
	}
	if android.Notification.ImageURL != "https://img" {
		t.Errorf("Notification.ImageURL = %q, want https://img", android.Notification.ImageURL)
	}
}

func TestBuildTopicMessage_SetsAllPlatformConfigs(t *testing.T) {
	d := newTestFCMDispatcher(t)
	msg := d.buildTopicMessage("topic-1", Message{Title: "t", Body: "b", ImageURL: "https://img"})
	if msg.Topic != "topic-1" {
		t.Errorf("Topic = %q, want topic-1", msg.Topic)
	}
	if msg.Android == nil || msg.APNS == nil || msg.Webpush == nil {
		t.Fatalf("buildTopicMessage() = %+v, want all three platform configs set (a topic fans out to every platform)", msg)
	}
	if msg.Notification.ImageURL != "https://img" {
		t.Errorf("Notification.ImageURL = %q, want https://img", msg.Notification.ImageURL)
	}
}

func TestClassifyFCMError_Table(t *testing.T) {
	cases := []struct {
		errMsg        string
		wantCode      FCMErrorCode
		wantPermanent bool
		wantRetryable bool
	}{
		{"NotRegistered", FCMErrorCodeUnregistered, true, false},
		{"invalid-argument: bad token", FCMErrorCodeInvalidArgument, true, false},
		{"MismatchSenderId", FCMErrorCodeSenderIDMismatch, true, false},
		{"quota-exceeded", FCMErrorCodeQuotaExceeded, false, true},
		{"service unavailable", FCMErrorCodeUnavailable, false, true},
		{"InternalError occurred", FCMErrorCodeInternal, false, true},
		{"third-party-auth-error", FCMErrorCodeThirdPartyAuthErr, false, false},
		{"something totally unexpected", FCMErrorCodeUnspecified, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.errMsg, func(t *testing.T) {
			fcmErr := classifyFCMError(errors.New(tc.errMsg), "tok")
			if fcmErr.Code != tc.wantCode {
				t.Fatalf("classifyFCMError(%q).Code = %s, want %s", tc.errMsg, fcmErr.Code, tc.wantCode)
			}
			if fcmErr.IsPermanent() != tc.wantPermanent {
				t.Fatalf("classifyFCMError(%q).IsPermanent() = %v, want %v", tc.errMsg, fcmErr.IsPermanent(), tc.wantPermanent)
			}
			if fcmErr.IsRetryable() != tc.wantRetryable {
				t.Fatalf("classifyFCMError(%q).IsRetryable() = %v, want %v", tc.errMsg, fcmErr.IsRetryable(), tc.wantRetryable)
			}
		})
	}
}

func TestClassifyFCMError_Nil(t *testing.T) {
	if err := classifyFCMError(nil, "tok"); err != nil {
		t.Fatalf("classifyFCMError(nil) = %v, want nil", err)
	}
}

func TestMaskToken(t *testing.T) {
	if got := maskToken("short"); got != "***" {
		t.Fatalf("maskToken(short) = %q, want ***", got)
	}
	long := "abcdefghijklmnopqrstuvwxyz"
	got := maskToken(long)
	if !strings.HasPrefix(got, "abcdefgh") || !strings.HasSuffix(got, "wxyz") {
		t.Fatalf("maskToken(long) = %q, want prefix abcdefgh and suffix wxyz", got)
	}
}
