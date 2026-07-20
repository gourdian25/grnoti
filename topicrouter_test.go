// File: topicrouter_test.go

package grnoti

import (
	"context"
	"errors"
	"testing"
)

type stubTokenStore struct {
	tokens []DeviceToken
	err    error

	// markInvalidErr, if set, is returned by MarkInvalid instead of nil —
	// used to exercise callers' handling of a MarkInvalid failure (e.g.
	// service.go's markInvalidTokens, which logs and continues).
	markInvalidErr error
}

func (s *stubTokenStore) GetActiveTokens(context.Context, string) ([]DeviceToken, error) {
	return s.tokens, s.err
}
func (s *stubTokenStore) GetActiveTokensBatch(context.Context, []string) (map[string][]DeviceToken, error) {
	return nil, nil
}
func (s *stubTokenStore) GetActiveTokensByAnonymousID(context.Context, string) ([]DeviceToken, error) {
	return s.tokens, s.err
}
func (s *stubTokenStore) MarkInvalid(context.Context, string) error    { return s.markInvalidErr }
func (s *stubTokenStore) SaveToken(context.Context, DeviceToken) error { return nil }
func (s *stubTokenStore) DeleteToken(context.Context, string) error    { return nil }
func (s *stubTokenStore) Close() error                                 { return nil }

func TestTokenTarget_GetTopicName_AlwaysEmpty(t *testing.T) {
	target := TokenTarget{Tokens: []DeviceToken{{Token: "t1"}}}
	if target.GetTopicName() != "" {
		t.Fatalf("TokenTarget.GetTopicName() = %q, want empty (a token target has no topic)", target.GetTopicName())
	}
}

func TestTopicTarget_GetTokens_AlwaysNil(t *testing.T) {
	target := TopicTarget{Topic: "some-topic"}
	if got := target.GetTokens(); got != nil {
		t.Fatalf("TopicTarget.GetTokens() = %v, want nil (a topic target has no tokens)", got)
	}
}

func TestEventTypeTopicRouter_PayloadOverrideWins(t *testing.T) {
	router := NewEventTypeTopicRouter(map[EventType]string{EventTypeSystemAlert: "alerts-topic"}, &stubTokenStore{}, nil)
	target, err := router.ResolveTarget(context.Background(), Event{
		Type:    EventTypeSystemAlert,
		Payload: map[string]string{"topic": "override-topic"},
	})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if !target.IsTopicBased() || target.GetTopicName() != "override-topic" {
		t.Fatalf("ResolveTarget() = %+v, want topic override-topic", target)
	}
}

func TestEventTypeTopicRouter_TypeMapping(t *testing.T) {
	router := NewEventTypeTopicRouter(map[EventType]string{EventTypeSystemAlert: "alerts-topic"}, &stubTokenStore{}, nil)
	target, err := router.ResolveTarget(context.Background(), Event{Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if !target.IsTopicBased() || target.GetTopicName() != "alerts-topic" {
		t.Fatalf("ResolveTarget() = %+v, want topic alerts-topic", target)
	}
}

func TestEventTypeTopicRouter_FallsBackToTokens(t *testing.T) {
	store := &stubTokenStore{tokens: []DeviceToken{{Token: "t1"}}}
	router := NewEventTypeTopicRouter(nil, store, nil)
	target, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetTokens()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want token-based target with 1 token", target)
	}
}

func TestStaticTopicRouter(t *testing.T) {
	router := NewStaticTopicRouter("fixed-topic")
	target, err := router.ResolveTarget(context.Background(), Event{Type: EventTypeSystemAlert})
	if err != nil || target.GetTopicName() != "fixed-topic" {
		t.Fatalf("ResolveTarget() = (%+v, %v), want fixed-topic", target, err)
	}
}

func TestTokenOnlyRouter(t *testing.T) {
	store := &stubTokenStore{tokens: []DeviceToken{{Token: "t1"}, {Token: "t2"}}}
	router := NewTokenOnlyRouter(store)
	target, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert, Payload: map[string]string{"topic": "ignored"}})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetTokens()) != 2 {
		t.Fatalf("ResolveTarget() = %+v, want token-based target ignoring any topic payload", target)
	}
}

// TestEventTypeTopicRouter_FallsBackToTokens_AnonymousEvent is the
// regression test for a real gap found while wiring Stage 12: the fallback
// branch originally called GetActiveTokens(ctx, event.UserID)
// unconditionally, so an anonymous event (empty UserID) silently resolved
// to zero tokens instead of using GetActiveTokensByAnonymousID.
func TestEventTypeTopicRouter_FallsBackToTokens_AnonymousEvent(t *testing.T) {
	store := &stubTokenStore{tokens: []DeviceToken{{Token: "t1"}}}
	router := NewEventTypeTopicRouter(nil, store, nil)
	target, err := router.ResolveTarget(context.Background(), Event{AnonymousID: "a1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetTokens()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want token-based target with 1 token for an anonymous event", target)
	}
}

// TestEventTypeTopicRouter_FallsBackToTokens_DirectTokens is the same
// regression, for an event carrying direct device tokens with no
// UserID/AnonymousID at all.
func TestEventTypeTopicRouter_FallsBackToTokens_DirectTokens(t *testing.T) {
	router := NewEventTypeTopicRouter(nil, &stubTokenStore{}, nil)
	target, err := router.ResolveTarget(context.Background(), Event{DeviceTokens: []string{"direct-1", "direct-2"}, Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetTokens()) != 2 {
		t.Fatalf("ResolveTarget() = %+v, want token-based target with 2 direct tokens", target)
	}
}

func TestEventTypeTopicRouter_TokenStoreError(t *testing.T) {
	store := &stubTokenStore{err: errors.New("token store boom")}
	router := NewEventTypeTopicRouter(nil, store, nil)
	_, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err == nil {
		t.Fatal("ResolveTarget with a failing TokenStore = nil error, want non-nil")
	}
}

func TestTokenOnlyRouter_TokenStoreError(t *testing.T) {
	store := &stubTokenStore{err: errors.New("token store boom")}
	router := NewTokenOnlyRouter(store)
	_, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err == nil {
		t.Fatal("ResolveTarget with a failing TokenStore = nil error, want non-nil")
	}
}

func TestTokenOnlyRouter_AnonymousEvent(t *testing.T) {
	store := &stubTokenStore{tokens: []DeviceToken{{Token: "t1"}}}
	router := NewTokenOnlyRouter(store)
	target, err := router.ResolveTarget(context.Background(), Event{AnonymousID: "a1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetTokens()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want token-based target with 1 token for an anonymous event", target)
	}
}
