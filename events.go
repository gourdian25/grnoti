// File: events.go

package grnoti

import (
	"context"
	"time"

	"github.com/gourdian25/grevents"
)

// Topic* are the grevents topics grnoti publishes to. Two dot-separated
// segments each, matching the real convention already established by
// grevents' own examples and graudit's TopicAuditRecorded (see
// docs/plan/grnoti-plan.md §1.2/§1.3).
const (
	// TopicNotificationSent fires after a dispatch completes with at
	// least one successful delivery.
	TopicNotificationSent = "notification.sent"
	// TopicNotificationFailed fires after a dispatch ends with zero
	// successful deliveries.
	TopicNotificationFailed = "notification.failed"
	// TopicExperimentAssigned fires the first time a user is assigned a
	// variant for an experiment — not on subsequent lookups of an
	// already-assigned user.
	TopicExperimentAssigned = "experiment.assigned"
)

// NotificationSentPayload is published on TopicNotificationSent.
type NotificationSentPayload struct {
	EventID      string
	UserID       string
	AnonymousID  string
	EventType    EventType
	SuccessCount int
	FailureCount int
	Timestamp    time.Time
}

// NotificationFailedPayload is published on TopicNotificationFailed.
type NotificationFailedPayload struct {
	EventID      string
	UserID       string
	AnonymousID  string
	EventType    EventType
	FailureCount int
	Reason       string
	Timestamp    time.Time
}

// ExperimentAssignedPayload is published on TopicExperimentAssigned.
type ExperimentAssignedPayload struct {
	UserID       string
	ExperimentID string
	VariantID    string
	Timestamp    time.Time
}

// PublishSent publishes a TopicNotificationSent event for payload via bus.
// Following graudit's exact PublishRecorded precedent (docs/plan/
// grnoti-plan.md §1.2/§1.3): bus may be nil (a silent no-op), and any error
// bus.Publish returns is only logged, never propagated to the caller —
// grevents delivery is a best-effort side channel on top of whatever
// durable/authoritative work already happened, never allowed to fail or
// block it.
func PublishSent(ctx context.Context, bus grevents.Bus, logger Logger, payload NotificationSentPayload) {
	if bus == nil {
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}
	event := grevents.Event{
		Topic:   TopicNotificationSent,
		Payload: payload,
		Metadata: map[string]string{
			"event_id":   payload.EventID,
			"event_type": string(payload.EventType),
		},
	}
	if err := bus.Publish(ctx, event); err != nil {
		logger.Warn("grnoti: publish failed", "topic", TopicNotificationSent, "event_id", payload.EventID, "error", err)
	}
}

// PublishFailed publishes a TopicNotificationFailed event. See PublishSent
// for the nil-bus/best-effort contract.
func PublishFailed(ctx context.Context, bus grevents.Bus, logger Logger, payload NotificationFailedPayload) {
	if bus == nil {
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}
	event := grevents.Event{
		Topic:   TopicNotificationFailed,
		Payload: payload,
		Metadata: map[string]string{
			"event_id":   payload.EventID,
			"event_type": string(payload.EventType),
		},
	}
	if err := bus.Publish(ctx, event); err != nil {
		logger.Warn("grnoti: publish failed", "topic", TopicNotificationFailed, "event_id", payload.EventID, "error", err)
	}
}

// PublishAssigned publishes a TopicExperimentAssigned event. See
// PublishSent for the nil-bus/best-effort contract.
//
// Callers (deterministicExperimentEngine, cacheExperimentEngine) call this
// only on a genuinely new assignment, never on a lookup of an
// already-assigned user. The exactly-once-vs-at-least-once guarantee for a
// given (userID, experimentID) pair's first assignment differs by engine:
//   - deterministicExperimentEngine holds a single in-process lock across
//     its whole check-then-write sequence, so exactly one goroutine can
//     ever be the one that assigns+publishes for a given key —
//     exactly-once (see experiment.go's own doc comment).
//   - cacheExperimentEngine cannot close the equivalent race, because
//     grcache.Cache has no compare-and-swap/SetNX primitive to make its
//     check-then-write atomic — concurrent racers on a brand-new pair can
//     each independently publish, giving at-least-once delivery there (see
//     cache.experiment.go's own doc comment).
//
// Either way the underlying map/cache write itself stays correct (both
// racers compute the identical deterministic variant), and at-least-once
// is consistent with grevents' own Bus.Publish, which makes no
// exactly-once guarantee either.
func PublishAssigned(ctx context.Context, bus grevents.Bus, logger Logger, payload ExperimentAssignedPayload) {
	if bus == nil {
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}
	event := grevents.Event{
		Topic:   TopicExperimentAssigned,
		Payload: payload,
		Metadata: map[string]string{
			"user_id":       payload.UserID,
			"experiment_id": payload.ExperimentID,
			"variant_id":    payload.VariantID,
		},
	}
	if err := bus.Publish(ctx, event); err != nil {
		logger.Warn("grnoti: publish failed", "topic", TopicExperimentAssigned, "user_id", payload.UserID, "experiment_id", payload.ExperimentID, "error", err)
	}
}
