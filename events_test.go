// File: events_test.go

package grnoti

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gourdian25/grevents"
)

// stubBus is a minimal grevents.Bus test double recording every Publish
// call — mirrors graudit's own events_test.go stubBus exactly, verifying
// PublishSent/PublishFailed/PublishAssigned's topic/payload shape and
// failure-does-not-propagate contract without depending on a real Bus.
type stubBus struct {
	mu         sync.Mutex
	published  []grevents.Event
	publishErr error
}

func (b *stubBus) Publish(_ context.Context, event grevents.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, event)
	return b.publishErr
}
func (b *stubBus) Subscribe(string, grevents.HandlerFunc, ...grevents.SubscribeOption) (grevents.Unsubscribe, error) {
	return func() {}, nil
}
func (b *stubBus) Use(grevents.Middleware)                       {}
func (b *stubBus) Stats(context.Context) (grevents.Stats, error) { return grevents.Stats{}, nil }
func (b *stubBus) Close() error                                  { return nil }

func (b *stubBus) publishedEvents() []grevents.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]grevents.Event, len(b.published))
	copy(out, b.published)
	return out
}

var _ grevents.Bus = (*stubBus)(nil)

type recordingLogger struct {
	mu                 sync.Mutex
	infos, warns, errs []string
}

func (l *recordingLogger) Debug(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, msg)
}
func (l *recordingLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, msg)
}
func (l *recordingLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *recordingLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, msg)
}
func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}

func TestPublishSent_NilBusIsNoOp(t *testing.T) {
	PublishSent(context.Background(), nil, NopLogger(), NotificationSentPayload{EventID: "e1"})
}

func TestPublishSent_PublishesExpectedTopicAndPayload(t *testing.T) {
	bus := &stubBus{}
	payload := NotificationSentPayload{EventID: "e1", UserID: "u1", EventType: EventTypeSystemAlert, SuccessCount: 2}

	PublishSent(context.Background(), bus, NopLogger(), payload)

	events := bus.publishedEvents()
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 Publish call, got %d", len(events))
	}
	got := events[0]
	if got.Topic != TopicNotificationSent {
		t.Fatalf("Topic = %q, want %q", got.Topic, TopicNotificationSent)
	}
	gotPayload, ok := got.Payload.(NotificationSentPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want NotificationSentPayload", got.Payload)
	}
	if gotPayload.EventID != payload.EventID || gotPayload.SuccessCount != payload.SuccessCount {
		t.Fatalf("Payload = %+v, want %+v", gotPayload, payload)
	}
	if got.Metadata["event_id"] != "e1" || got.Metadata["event_type"] != string(EventTypeSystemAlert) {
		t.Fatalf("unexpected Metadata: %+v", got.Metadata)
	}
	// PublishSent back-fills payload.Timestamp when left zero; the
	// envelope-level grevents.Event.Timestamp is a real Bus's job to set
	// (per grevents.Event's own doc comment), not PublishSent's — stubBus
	// deliberately doesn't do that, so only the payload is checked here.
	if gotPayload.Timestamp.IsZero() {
		t.Fatal("payload.Timestamp not back-filled by PublishSent when left zero")
	}
}

func TestPublishSent_FailureIsLoggedNotPropagated(t *testing.T) {
	bus := &stubBus{publishErr: errors.New("bus unavailable")}
	logger := &recordingLogger{}

	PublishSent(context.Background(), bus, logger, NotificationSentPayload{EventID: "e1"})

	if got := logger.warnCount(); got != 1 {
		t.Fatalf("Warnf call count = %d, want 1", got)
	}
}

func TestPublishFailed_NilBusIsNoOp(t *testing.T) {
	PublishFailed(context.Background(), nil, NopLogger(), NotificationFailedPayload{EventID: "e1"})
}

func TestPublishFailed_PublishesExpectedTopicAndPayload(t *testing.T) {
	bus := &stubBus{}
	payload := NotificationFailedPayload{EventID: "e1", UserID: "u1", EventType: EventTypeSystemAlert, FailureCount: 3, Reason: "fcm unavailable"}

	PublishFailed(context.Background(), bus, NopLogger(), payload)

	events := bus.publishedEvents()
	if len(events) != 1 || events[0].Topic != TopicNotificationFailed {
		t.Fatalf("Publish() events = %+v, want exactly 1 on topic %q", events, TopicNotificationFailed)
	}
	gotPayload, ok := events[0].Payload.(NotificationFailedPayload)
	if !ok || gotPayload.Reason != "fcm unavailable" || gotPayload.FailureCount != 3 {
		t.Fatalf("Payload = %+v (ok=%v), want Reason=%q FailureCount=3", gotPayload, ok, "fcm unavailable")
	}
}

func TestPublishFailed_FailureIsLoggedNotPropagated(t *testing.T) {
	bus := &stubBus{publishErr: errors.New("bus unavailable")}
	logger := &recordingLogger{}
	PublishFailed(context.Background(), bus, logger, NotificationFailedPayload{EventID: "e1"})
	if got := logger.warnCount(); got != 1 {
		t.Fatalf("Warnf call count = %d, want 1", got)
	}
}

func TestPublishAssigned_NilBusIsNoOp(t *testing.T) {
	PublishAssigned(context.Background(), nil, NopLogger(), ExperimentAssignedPayload{UserID: "u1"})
}

func TestPublishAssigned_PublishesExpectedTopicAndPayload(t *testing.T) {
	bus := &stubBus{}
	payload := ExperimentAssignedPayload{UserID: "u1", ExperimentID: "exp-1", VariantID: "control"}

	PublishAssigned(context.Background(), bus, NopLogger(), payload)

	events := bus.publishedEvents()
	if len(events) != 1 || events[0].Topic != TopicExperimentAssigned {
		t.Fatalf("Publish() events = %+v, want exactly 1 on topic %q", events, TopicExperimentAssigned)
	}
	// PublishAssigned back-fills Timestamp when left zero (as here), so
	// compare individual fields rather than the whole struct.
	gotPayload, ok := events[0].Payload.(ExperimentAssignedPayload)
	if !ok || gotPayload.UserID != payload.UserID || gotPayload.ExperimentID != payload.ExperimentID || gotPayload.VariantID != payload.VariantID {
		t.Fatalf("Payload = %+v (ok=%v), want UserID/ExperimentID/VariantID matching %+v", gotPayload, ok, payload)
	}
	if gotPayload.Timestamp.IsZero() {
		t.Fatal("payload.Timestamp not back-filled by PublishAssigned when left zero")
	}
	if events[0].Metadata["user_id"] != "u1" || events[0].Metadata["experiment_id"] != "exp-1" || events[0].Metadata["variant_id"] != "control" {
		t.Fatalf("unexpected Metadata: %+v", events[0].Metadata)
	}
}

func TestPublishAssigned_FailureIsLoggedNotPropagated(t *testing.T) {
	bus := &stubBus{publishErr: errors.New("bus unavailable")}
	logger := &recordingLogger{}
	PublishAssigned(context.Background(), bus, logger, ExperimentAssignedPayload{UserID: "u1"})
	if got := logger.warnCount(); got != 1 {
		t.Fatalf("Warnf call count = %d, want 1", got)
	}
}

// TestExperimentAssigned_RealBusEndToEnd proves the wiring against a real
// grevents.Bus, not just stubBus — unlike Mongo/Postgres/Redis/Kafka,
// grevents is in-process and needs no live service to test for real, so
// there's no reason to only exercise this path through a fake.
func TestExperimentAssigned_RealBusEndToEnd(t *testing.T) {
	bus, err := grevents.NewBus()
	if err != nil {
		t.Fatalf("grevents.NewBus: %v", err)
	}
	defer bus.Close()

	received := make(chan grevents.Event, 1)
	unsubscribe, err := bus.Subscribe(TopicExperimentAssigned, func(_ context.Context, event grevents.Event) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()

	engine := NewDeterministicExperimentEngine(nil, bus, nil)
	experiment := &Experiment{ID: "exp-1", Variants: []ExperimentVariant{{ID: "only", Weight: 1}}}
	assigned, err := engine.AssignVariant(context.Background(), "user-1", experiment)
	if err != nil {
		t.Fatalf("AssignVariant: %v", err)
	}

	select {
	case event := <-received:
		payload, ok := event.Payload.(ExperimentAssignedPayload)
		if !ok || payload.VariantID != assigned.ID {
			t.Fatalf("received event Payload = %+v (ok=%v), want VariantID=%s", payload, ok, assigned.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TopicExperimentAssigned delivery via a real grevents.Bus")
	}
}

func TestTopics_AreTwoSegments(t *testing.T) {
	// Matches the confirmed real grevents convention (role.assigned,
	// order.placed, payment.failed, user.signup) and graudit's own
	// TopicAuditRecorded: exactly two dot-separated segments.
	for _, topic := range []string{TopicNotificationSent, TopicNotificationFailed, TopicExperimentAssigned} {
		segments := 1
		for _, c := range topic {
			if c == '.' {
				segments++
			}
		}
		if segments != 2 {
			t.Fatalf("topic %q has %d dot-separated segments, want 2", topic, segments)
		}
	}
}
