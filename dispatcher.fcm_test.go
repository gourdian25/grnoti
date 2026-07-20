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
	waits atomic.Int32
}

func (c *countingRateLimiter) Allow(context.Context) (bool, error) { return true, nil }
func (c *countingRateLimiter) Wait(context.Context) error          { c.waits.Add(1); return nil }
func (c *countingRateLimiter) GetStats(context.Context) (RateLimiterStats, error) {
	return RateLimiterStats{}, nil
}

func androidToken(tok string) DeviceToken { return DeviceToken{Token: tok, Platform: PlatformAndroid} }

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

func TestFCMDispatcher_SendToTopic_Success(t *testing.T) {
	client := &fakeFCMClient{}
	d, _ := NewFCMDispatcher(FCMDispatcherDeps{Client: client})
	if err := d.SendToTopic(context.Background(), "topic-1", Message{Title: "hi"}); err != nil {
		t.Fatalf("SendToTopic: %v", err)
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
