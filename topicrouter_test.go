// File: topicrouter_test.go

package grnoti

import (
	"context"
	"errors"
	"testing"
)

type stubDeviceStore struct {
	devices []Device
	err     error

	// markInvalidErr, if set, is returned by MarkInvalid instead of nil —
	// used to exercise callers' handling of a MarkInvalid failure (e.g.
	// service.go's markInvalidFIDs, which logs and continues).
	markInvalidErr error
}

func (s *stubDeviceStore) GetActiveDevices(context.Context, string) ([]Device, error) {
	return s.devices, s.err
}
func (s *stubDeviceStore) GetActiveDevicesBatch(context.Context, []string) (map[string][]Device, error) {
	return nil, nil
}
func (s *stubDeviceStore) GetActiveDevicesByAnonymousID(context.Context, string) ([]Device, error) {
	return s.devices, s.err
}
func (s *stubDeviceStore) MarkInvalid(context.Context, string) error  { return s.markInvalidErr }
func (s *stubDeviceStore) SaveDevice(context.Context, Device) error   { return nil }
func (s *stubDeviceStore) DeleteDevice(context.Context, string) error { return nil }
func (s *stubDeviceStore) Close() error                               { return nil }

func TestDeviceTarget_GetTopicName_AlwaysEmpty(t *testing.T) {
	target := DeviceTarget{Devices: []Device{{FID: "t1"}}}
	if target.GetTopicName() != "" {
		t.Fatalf("DeviceTarget.GetTopicName() = %q, want empty (a device target has no topic)", target.GetTopicName())
	}
}

func TestTopicTarget_GetDevices_AlwaysNil(t *testing.T) {
	target := TopicTarget{Topic: "some-topic"}
	if got := target.GetDevices(); got != nil {
		t.Fatalf("TopicTarget.GetDevices() = %v, want nil (a topic target has no devices)", got)
	}
}

func TestEventTypeTopicRouter_PayloadOverrideWins(t *testing.T) {
	router := NewEventTypeTopicRouter(map[EventType]string{EventTypeSystemAlert: "alerts-topic"}, &stubDeviceStore{}, nil)
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
	router := NewEventTypeTopicRouter(map[EventType]string{EventTypeSystemAlert: "alerts-topic"}, &stubDeviceStore{}, nil)
	target, err := router.ResolveTarget(context.Background(), Event{Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if !target.IsTopicBased() || target.GetTopicName() != "alerts-topic" {
		t.Fatalf("ResolveTarget() = %+v, want topic alerts-topic", target)
	}
}

func TestEventTypeTopicRouter_FallsBackToDevices(t *testing.T) {
	store := &stubDeviceStore{devices: []Device{{FID: "t1"}}}
	router := NewEventTypeTopicRouter(nil, store, nil)
	target, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetDevices()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want device-based target with 1 device", target)
	}
}

func TestStaticTopicRouter(t *testing.T) {
	router := NewStaticTopicRouter("fixed-topic")
	target, err := router.ResolveTarget(context.Background(), Event{Type: EventTypeSystemAlert})
	if err != nil || target.GetTopicName() != "fixed-topic" {
		t.Fatalf("ResolveTarget() = (%+v, %v), want fixed-topic", target, err)
	}
}

func TestDeviceOnlyRouter(t *testing.T) {
	store := &stubDeviceStore{devices: []Device{{FID: "t1"}, {FID: "t2"}}}
	router := NewDeviceOnlyRouter(store)
	target, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert, Payload: map[string]string{"topic": "ignored"}})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetDevices()) != 2 {
		t.Fatalf("ResolveTarget() = %+v, want device-based target ignoring any topic payload", target)
	}
}

// TestEventTypeTopicRouter_FallsBackToDevices_AnonymousEvent is the
// regression test for a real gap found while wiring Stage 12: the fallback
// branch originally called GetActiveDevices(ctx, event.UserID)
// unconditionally, so an anonymous event (empty UserID) silently resolved
// to zero devices instead of using GetActiveDevicesByAnonymousID.
func TestEventTypeTopicRouter_FallsBackToDevices_AnonymousEvent(t *testing.T) {
	store := &stubDeviceStore{devices: []Device{{FID: "t1"}}}
	router := NewEventTypeTopicRouter(nil, store, nil)
	target, err := router.ResolveTarget(context.Background(), Event{AnonymousID: "a1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetDevices()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want device-based target with 1 device for an anonymous event", target)
	}
}

// TestEventTypeTopicRouter_FallsBackToDevices_DirectFIDs is the same
// regression, for an event carrying direct FIDs with no
// UserID/AnonymousID at all.
func TestEventTypeTopicRouter_FallsBackToDevices_DirectFIDs(t *testing.T) {
	router := NewEventTypeTopicRouter(nil, &stubDeviceStore{}, nil)
	target, err := router.ResolveTarget(context.Background(), Event{FIDs: []string{"direct-1", "direct-2"}, Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetDevices()) != 2 {
		t.Fatalf("ResolveTarget() = %+v, want device-based target with 2 direct FIDs", target)
	}
}

func TestEventTypeTopicRouter_DeviceStoreError(t *testing.T) {
	store := &stubDeviceStore{err: errors.New("device store boom")}
	router := NewEventTypeTopicRouter(nil, store, nil)
	_, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err == nil {
		t.Fatal("ResolveTarget with a failing DeviceStore = nil error, want non-nil")
	}
}

func TestDeviceOnlyRouter_DeviceStoreError(t *testing.T) {
	store := &stubDeviceStore{err: errors.New("device store boom")}
	router := NewDeviceOnlyRouter(store)
	_, err := router.ResolveTarget(context.Background(), Event{UserID: "u1", Type: EventTypeSystemAlert})
	if err == nil {
		t.Fatal("ResolveTarget with a failing DeviceStore = nil error, want non-nil")
	}
}

func TestDeviceOnlyRouter_AnonymousEvent(t *testing.T) {
	store := &stubDeviceStore{devices: []Device{{FID: "t1"}}}
	router := NewDeviceOnlyRouter(store)
	target, err := router.ResolveTarget(context.Background(), Event{AnonymousID: "a1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("ResolveTarget: %v", err)
	}
	if target.IsTopicBased() || len(target.GetDevices()) != 1 {
		t.Fatalf("ResolveTarget() = %+v, want device-based target with 1 device for an anonymous event", target)
	}
}
