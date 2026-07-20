// File: topicrouter_test.go

package grnoti

import (
	"context"
	"testing"
)

type stubTokenStore struct {
	tokens []DeviceToken
	err    error
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
func (s *stubTokenStore) MarkInvalid(context.Context, string) error    { return nil }
func (s *stubTokenStore) SaveToken(context.Context, DeviceToken) error { return nil }
func (s *stubTokenStore) DeleteToken(context.Context, string) error    { return nil }
func (s *stubTokenStore) Close() error                                 { return nil }

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
