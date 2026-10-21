// File: topicrouter.go

package grnoti

import (
	"context"
	"fmt"
)

// DeviceTarget is a NotificationTarget resolving to a fixed set of devices.
type DeviceTarget struct {
	Devices []Device
}

var _ NotificationTarget = DeviceTarget{}

func (t DeviceTarget) IsTopicBased() bool   { return false }
func (t DeviceTarget) GetTopicName() string { return "" }
func (t DeviceTarget) GetDevices() []Device { return t.Devices }

// TopicTarget is a NotificationTarget resolving to an FCM topic.
type TopicTarget struct {
	Topic string
}

var _ NotificationTarget = TopicTarget{}

func (t TopicTarget) IsTopicBased() bool   { return true }
func (t TopicTarget) GetTopicName() string { return t.Topic }
func (t TopicTarget) GetDevices() []Device { return nil }

// resolveDevicesForEvent resolves event's recipient devices: direct
// FIDs embedded in the event take precedence (matching dispatcher.fcm.go's
// own "default to Android for an unset platform" convention), then an
// authenticated user's devices via deviceStore,
// then an anonymous visitor's. Shared by NotificationService's own
// resolution and every TopicRouter's device-fallback branch below — a fix
// found while wiring Stage 12's full pipeline: eventTypeTopicRouter and
// deviceOnlyRouter originally called deviceStore.GetActiveDevices(ctx,
// event.UserID) unconditionally in their fallback branch, which silently
// resolved to zero devices for an anonymous or direct-FID event (an empty
// UserID matches nothing) instead of actually resolving them. See
// docs/plan/grnoti-plan.md's Stage 12 implementation log.
func resolveDevicesForEvent(ctx context.Context, event Event, deviceStore DeviceStore) ([]Device, error) {
	switch {
	case event.HasDirectFIDs():
		devices := make([]Device, len(event.FIDs))
		for i, fid := range event.FIDs {
			devices[i] = Device{FID: fid, Platform: PlatformAndroid, IsActive: true}
		}
		return devices, nil
	case event.IsAuthenticated():
		return deviceStore.GetActiveDevices(ctx, event.UserID)
	case event.IsAnonymous():
		return deviceStore.GetActiveDevicesByAnonymousID(ctx, event.AnonymousID)
	default:
		return nil, nil // unreachable after Event.Validate(), which requires one of the above
	}
}

// eventTypeTopicRouter is the primary TopicRouter: resolves in priority
// order — an explicit event.Payload["topic"] override, then a static
// event-type-to-topic mapping, then falls back to per-user device
// routing via DeviceStore.
type eventTypeTopicRouter struct {
	topicMappings map[EventType]string
	deviceStore   DeviceStore
	logger        Logger
}

var _ TopicRouter = (*eventTypeTopicRouter)(nil)

// NewEventTypeTopicRouter constructs the primary TopicRouter.
//
// Parameters:
//   - topicMappings: map[EventType]string — may be nil
//   - deviceStore: DeviceStore — used as the fallback when neither a payload
//     override nor a type mapping applies
//   - logger: Logger — may be nil
func NewEventTypeTopicRouter(topicMappings map[EventType]string, deviceStore DeviceStore, logger Logger) TopicRouter {
	if topicMappings == nil {
		topicMappings = make(map[EventType]string)
	}
	return &eventTypeTopicRouter{topicMappings: topicMappings, deviceStore: deviceStore, logger: OrNop(logger)}
}

func (r *eventTypeTopicRouter) ResolveTarget(ctx context.Context, event Event) (NotificationTarget, error) {
	if topic, ok := event.Payload["topic"]; ok && topic != "" {
		r.logger.Debug("grnoti: routing event to explicit topic override", "event_id", event.EventID, "topic", topic)
		return TopicTarget{Topic: topic}, nil
	}

	if topic, ok := r.topicMappings[event.Type]; ok && topic != "" {
		r.logger.Debug("grnoti: routing event to mapped topic", "event_id", event.EventID, "topic", topic, "type", event.Type)
		return TopicTarget{Topic: topic}, nil
	}

	r.logger.Debug("grnoti: routing event to per-recipient devices", "event_id", event.EventID)
	devices, err := resolveDevicesForEvent(ctx, event, r.deviceStore)
	if err != nil {
		return nil, fmt.Errorf("grnoti: failed to get devices for event %s: %w", event.EventID, err)
	}
	return DeviceTarget{Devices: devices}, nil
}

type staticTopicRouter struct{ topic string }

var _ TopicRouter = staticTopicRouter{}

// NewStaticTopicRouter returns a TopicRouter that always resolves to the
// same fixed topic, for tests or single-topic applications.
func NewStaticTopicRouter(topic string) TopicRouter { return staticTopicRouter{topic: topic} }

func (r staticTopicRouter) ResolveTarget(context.Context, Event) (NotificationTarget, error) {
	return TopicTarget{Topic: r.topic}, nil
}

type deviceOnlyRouter struct{ deviceStore DeviceStore }

var _ TopicRouter = deviceOnlyRouter{}

// NewDeviceOnlyRouter returns a TopicRouter that always routes via
// DeviceStore, disabling topic-based routing entirely.
func NewDeviceOnlyRouter(deviceStore DeviceStore) TopicRouter {
	return deviceOnlyRouter{deviceStore: deviceStore}
}

func (r deviceOnlyRouter) ResolveTarget(ctx context.Context, event Event) (NotificationTarget, error) {
	devices, err := resolveDevicesForEvent(ctx, event, r.deviceStore)
	if err != nil {
		return nil, fmt.Errorf("grnoti: failed to get devices for event %s: %w", event.EventID, err)
	}
	return DeviceTarget{Devices: devices}, nil
}
