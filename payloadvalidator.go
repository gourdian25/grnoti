// File: payloadvalidator.go

package grnoti

import (
	"encoding/json"
	"fmt"
)

// FCMMaxPayloadSize is FCM's documented maximum message payload size in
// bytes.
const FCMMaxPayloadSize = 4096

type fcmPayloadValidator struct{}

var _ PayloadValidator = fcmPayloadValidator{}

// NewFCMPayloadValidator returns a stateless PayloadValidator that
// estimates a Message's serialized FCM payload size.
func NewFCMPayloadValidator() PayloadValidator { return fcmPayloadValidator{} }

func (fcmPayloadValidator) EstimateSize(msg Message) int {
	shape := map[string]any{
		"notification": map[string]any{
			"title": msg.Title,
			"body":  msg.Body,
		},
		"data": msg.Data,
	}
	if msg.ImageURL != "" {
		notification, ok := shape["notification"].(map[string]any)
		if ok {
			notification["image"] = msg.ImageURL
		}
	}
	if raw, err := json.Marshal(shape); err == nil {
		return len(raw)
	}
	// Fallback if marshaling somehow fails: a rough lower-bound estimate
	// from the title/body lengths alone. This deliberately errs toward
	// undercounting (it ignores msg.Data and JSON structural overhead)
	// rather than blocking a send on an estimation failure — the real FCM
	// API call remains the authoritative size check. In practice this is
	// unreachable: shape is built only from string/map[string]string
	// values, which json.Marshal cannot fail on — kept as a real fallback
	// rather than a panic/assumption, in case shape's construction changes.
	return len(msg.Title) + len(msg.Body)
}

func (v fcmPayloadValidator) ValidateSize(msg Message) error {
	size := v.EstimateSize(msg)
	if size > FCMMaxPayloadSize {
		return fmt.Errorf("%w: estimated %d bytes, max %d bytes", ErrFCMPayloadTooLarge, size, FCMMaxPayloadSize)
	}
	return nil
}
