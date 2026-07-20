// File: payloadvalidator_test.go

package grnoti

import (
	"errors"
	"strings"
	"testing"
)

func TestPayloadValidator_ValidateSize_WithinLimit(t *testing.T) {
	v := NewFCMPayloadValidator()
	msg := Message{Title: "Hello", Body: "World"}
	if err := v.ValidateSize(msg); err != nil {
		t.Fatalf("ValidateSize(small message): %v", err)
	}
}

func TestPayloadValidator_ValidateSize_TooLarge(t *testing.T) {
	v := NewFCMPayloadValidator()
	msg := Message{Title: "Hello", Body: strings.Repeat("x", FCMMaxPayloadSize)}
	err := v.ValidateSize(msg)
	if !errors.Is(err, ErrFCMPayloadTooLarge) {
		t.Fatalf("ValidateSize(large message) error = %v, want ErrFCMPayloadTooLarge", err)
	}
}

func TestPayloadValidator_EstimateSize_GrowsWithContent(t *testing.T) {
	v := NewFCMPayloadValidator()
	small := v.EstimateSize(Message{Title: "a", Body: "b"})
	large := v.EstimateSize(Message{Title: "a", Body: strings.Repeat("b", 1000)})
	if large <= small {
		t.Fatalf("EstimateSize did not grow with body size: small=%d large=%d", small, large)
	}
}
