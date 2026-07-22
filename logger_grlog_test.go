// File: logger_grlog_test.go

package grnoti_test

// This file proves *slog.Logger satisfies grnoti.Logger structurally,
// without grnoti itself importing grlog or log/slog — grlog is a
// test-only dependency of this module, so it never leaks into consumers
// who don't want a logging dependency at all. Mirrors grcache's,
// grevents's, graudit's, and grpolicy's own logger_test.go pattern.

import (
	"log/slog"
	"testing"

	"github.com/gourdian25/grlog"

	"github.com/gourdian25/grnoti"
)

var _ grnoti.Logger = (*slog.Logger)(nil)

func TestGrlogSatisfiesLoggerInterface(t *testing.T) {
	logger := grlog.NewDefaultLogger()
	defer func() { _ = logger.Close() }()

	slogger := slog.New(grlog.NewSlogHandler(logger))
	var l grnoti.Logger = slogger

	l.Debug("grnoti test", "level", "debug")
	l.Info("grnoti test", "level", "info")
	l.Warn("grnoti test", "level", "warn")
	l.Error("grnoti test", "level", "error")
}
