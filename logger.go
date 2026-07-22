// File: logger.go

package grnoti

// Logger is the minimal logging interface grnoti accepts for optional
// diagnostic logging (backend connectivity failures, dispatch retries,
// circuit-breaker state transitions, shutdown). Its four methods match
// *slog.Logger's own signatures exactly, so *slog.Logger satisfies it
// structurally — grnoti itself does not import grlog or log/slog, so
// plugging in a logger is entirely opt-in and adds no dependency for
// consumers who don't want one.
//
// A nil Logger passed to any constructor is replaced with NopLogger() —
// logging is always optional, never required for grnoti to function.
//
// Example, using grlog via its log/slog adapter (the recommended bridge —
// grlog itself needs no code changes for this):
//
//	import (
//		"log/slog"
//
//		"github.com/gourdian25/grlog"
//	)
//
//	logger := slog.New(grlog.NewSlogHandler(grlog.NewDefaultLogger()))
//	deps.Logger = logger
//	svc, err := grnoti.NewNotificationService(deps)
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// NopLogger returns a Logger that discards every message. It is the default
// used whenever no Logger is configured.
//
// Returns:
//   - Logger: a non-nil, no-op implementation safe to call from any goroutine
func NopLogger() Logger { return noopLogger{} }

// OrNop returns l if it is non-nil, otherwise NopLogger(). Every
// constructor in grnoti calls this once at construction time so every
// subsequent log call site can assume a non-nil Logger.
//
// Parameters:
//   - l: Logger — may be nil
//
// Returns:
//   - Logger: l unchanged if non-nil, otherwise NopLogger()
func OrNop(l Logger) Logger {
	if l == nil {
		return NopLogger()
	}
	return l
}
