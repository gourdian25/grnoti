// File: logger_test.go

package grnoti

import "testing"

func TestNopLogger_DiscardsEverything(t *testing.T) {
	l := NopLogger()
	if l == nil {
		t.Fatal("NopLogger() = nil, want a non-nil Logger")
	}
	// noopLogger's methods have empty bodies; calling them is the whole
	// test — there's nothing to assert beyond "doesn't panic."
	l.Debug("debug", "x")
	l.Info("info", "x")
	l.Warn("warn", "x")
	l.Error("error", "x")
}

func TestOrNop(t *testing.T) {
	if got := OrNop(nil); got == nil {
		t.Fatal("OrNop(nil) = nil, want NopLogger()")
	}

	custom := &recordingLogger{}
	if got := OrNop(custom); got != custom {
		t.Fatalf("OrNop(custom) = %v, want the same custom Logger unchanged", got)
	}
}
