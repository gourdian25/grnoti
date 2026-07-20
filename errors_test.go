// File: errors_test.go

package grnoti

import (
	"errors"
	"strings"
	"testing"
)

func TestFCMErrorCode_IsRetryable(t *testing.T) {
	for _, c := range []FCMErrorCode{FCMErrorCodeUnavailable, FCMErrorCodeInternal, FCMErrorCodeQuotaExceeded} {
		if !c.IsRetryable() {
			t.Errorf("%s.IsRetryable() = false, want true", c)
		}
	}
	for _, c := range []FCMErrorCode{FCMErrorCodeUnregistered, FCMErrorCodeInvalidArgument, FCMErrorCodeUnspecified} {
		if c.IsRetryable() {
			t.Errorf("%s.IsRetryable() = true, want false", c)
		}
	}
}

func TestFCMErrorCode_IsPermanent(t *testing.T) {
	for _, c := range []FCMErrorCode{FCMErrorCodeUnregistered, FCMErrorCodeInvalidArgument, FCMErrorCodeSenderIDMismatch} {
		if !c.IsPermanent() {
			t.Errorf("%s.IsPermanent() = false, want true", c)
		}
	}
	for _, c := range []FCMErrorCode{FCMErrorCodeUnavailable, FCMErrorCodeUnspecified} {
		if c.IsPermanent() {
			t.Errorf("%s.IsPermanent() = true, want false", c)
		}
	}
}

func TestFCMError_Error(t *testing.T) {
	withCause := NewFCMError(FCMErrorCodeUnavailable, "tok-1", "server busy", errors.New("boom"))
	if msg := withCause.Error(); !strings.Contains(msg, "server busy") || !strings.Contains(msg, "boom") {
		t.Fatalf("Error() = %q, want it to contain both the message and wrapped cause", msg)
	}

	withoutCause := NewFCMError(FCMErrorCodeUnregistered, "tok-2", "gone", nil)
	if msg := withoutCause.Error(); !strings.Contains(msg, "gone") || strings.Contains(msg, "<nil>") {
		t.Fatalf("Error() = %q, want the message without a nil-cause suffix", msg)
	}
}

func TestFCMError_Unwrap(t *testing.T) {
	cause := errors.New("root cause")
	fcmErr := NewFCMError(FCMErrorCodeInternal, "tok-1", "failed", cause)
	if !errors.Is(fcmErr, cause) {
		t.Fatal("errors.Is(fcmErr, cause) = false, want true (Unwrap should expose the wrapped cause)")
	}

	noCause := NewFCMError(FCMErrorCodeInternal, "tok-1", "failed", nil)
	if noCause.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil when no cause was given", noCause.Unwrap())
	}
}

func TestFCMError_DelegatesRetryableAndPermanent(t *testing.T) {
	retryable := NewFCMError(FCMErrorCodeUnavailable, "tok-1", "", nil)
	if !retryable.IsRetryable() || retryable.IsPermanent() {
		t.Fatalf("FCMError wrapping %s: IsRetryable/IsPermanent didn't delegate correctly", FCMErrorCodeUnavailable)
	}

	permanent := NewFCMError(FCMErrorCodeUnregistered, "tok-1", "", nil)
	if permanent.IsRetryable() || !permanent.IsPermanent() {
		t.Fatalf("FCMError wrapping %s: IsRetryable/IsPermanent didn't delegate correctly", FCMErrorCodeUnregistered)
	}
}
