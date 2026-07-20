// File: templateengine_test.go

package grnoti

import (
	"strings"
	"testing"
)

func TestTemplateEngine_DefaultsHaveNoHardcodedScheme(t *testing.T) {
	// Regression test for docs/plan/grnoti-plan.md §2 item 8: the reference
	// implementation hardcoded a "skipp://" deep-link scheme into 8 of 9
	// default templates. None of grnoti's own defaults may reference any
	// scheme at all.
	te := NewTemplateEngine().(*defaultTemplateEngine)
	te.mu.RLock()
	defer te.mu.RUnlock()
	for eventType, compiled := range te.templates {
		if strings.Contains(compiled.deepLink, "://") {
			t.Errorf("default template for %s has a hardcoded deep-link scheme: %q", eventType, compiled.deepLink)
		}
	}
}

func TestTemplateEngine_BuildMessage_KnownType(t *testing.T) {
	te := NewTemplateEngine()
	msg, err := te.BuildMessage(Event{
		EventID: "evt-1",
		UserID:  "user-1",
		Type:    EventTypePasswordReset,
	})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title == "" || msg.Body == "" {
		t.Fatalf("BuildMessage produced an empty message: %+v", msg)
	}
}

func TestTemplateEngine_BuildMessage_FallsBackToCustom(t *testing.T) {
	te := NewTemplateEngine()
	msg, err := te.BuildMessage(Event{
		EventID: "evt-1",
		UserID:  "user-1",
		Type:    EventType("never_registered"),
		Payload: map[string]string{"title": "Hello", "body": "World"},
	})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title != "Hello" || msg.Body != "World" {
		t.Fatalf("BuildMessage via EventTypeCustom fallback = %+v, want Title=Hello Body=World", msg)
	}
}

func TestTemplateEngine_BuildMessage_NoTemplateAtAll(t *testing.T) {
	te := &defaultTemplateEngine{templates: map[EventType]*compiledTemplate{}}
	_, err := te.BuildMessage(Event{EventID: "e", Type: "unknown"})
	if err != ErrTemplateNotFound {
		t.Fatalf("BuildMessage() error = %v, want ErrTemplateNotFound", err)
	}
}

func TestTemplateEngine_RegisterTemplate_PayloadOverrides(t *testing.T) {
	te := NewTemplateEngine()
	if err := te.RegisterTemplate("promo", MessageTemplate{
		TitleTemplate: "{{.title}}",
		BodyTemplate:  "{{.body}}",
		DeepLink:      "myapp://promo/{{.promo_id}}",
	}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}

	msg, err := te.BuildMessage(Event{
		EventID: "evt-1",
		UserID:  "user-1",
		Type:    "promo",
		Payload: map[string]string{"title": "Sale", "body": "50% off", "promo_id": "42", "ttl": "1h", "collapse_key": "promo-42"},
	})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.DeepLink != "myapp://promo/42" {
		t.Errorf("DeepLink = %q, want myapp://promo/42", msg.DeepLink)
	}
	if msg.CollapseKey != "promo-42" {
		t.Errorf("CollapseKey = %q, want promo-42", msg.CollapseKey)
	}
	if msg.TTL.String() != "1h0m0s" {
		t.Errorf("TTL = %v, want 1h0m0s", msg.TTL)
	}
}

func TestTemplateEngine_RegisterTemplate_InvalidSyntax(t *testing.T) {
	te := NewTemplateEngine()
	err := te.RegisterTemplate("bad", MessageTemplate{TitleTemplate: "{{.unterminated", BodyTemplate: "ok"})
	if err == nil {
		t.Fatal("RegisterTemplate with invalid template syntax = nil error, want non-nil")
	}
}

func TestTemplateEngine_RegisterTemplate_InvalidBodySyntax(t *testing.T) {
	te := NewTemplateEngine()
	err := te.RegisterTemplate("bad", MessageTemplate{TitleTemplate: "ok", BodyTemplate: "{{.unterminated"})
	if err == nil {
		t.Fatal("RegisterTemplate with invalid body template syntax = nil error, want non-nil")
	}
}

// TestTemplateEngine_RegisterTemplate_InvalidDeepLinkSyntax is the
// regression test for the bug fixed alongside this validation: previously
// a malformed DeepLink template wasn't caught until the first BuildMessage
// call, and even then its render error was silently swallowed, shipping
// the raw "{{...}}" text. It must now be rejected at registration time,
// matching Title/Body.
func TestTemplateEngine_RegisterTemplate_InvalidDeepLinkSyntax(t *testing.T) {
	te := NewTemplateEngine()
	err := te.RegisterTemplate("bad", MessageTemplate{
		TitleTemplate: "ok", BodyTemplate: "ok", DeepLink: "myapp://{{.unterminated",
	})
	if err == nil {
		t.Fatal("RegisterTemplate with invalid deep link template syntax = nil error, want non-nil")
	}
}

func TestTemplateEngine_RegisterTemplate_InvalidActionURLSyntax(t *testing.T) {
	te := NewTemplateEngine()
	err := te.RegisterTemplate("bad", MessageTemplate{
		TitleTemplate: "ok", BodyTemplate: "ok",
		Actions: []NotificationAction{{ID: "a1", Title: "Open", URL: "myapp://{{.unterminated"}},
	})
	if err == nil {
		t.Fatal("RegisterTemplate with invalid action url template syntax = nil error, want non-nil")
	}
}

func TestTemplateEngine_BuildMessage_TitleExecuteError(t *testing.T) {
	te := NewTemplateEngine()
	if err := te.RegisterTemplate("bad", MessageTemplate{TitleTemplate: "{{call .title}}", BodyTemplate: "ok"}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}
	_, err := te.BuildMessage(Event{EventID: "e", Type: "bad", Payload: map[string]string{"title": "not a function"}})
	if err == nil {
		t.Fatal("BuildMessage with a title template that fails to execute = nil error, want non-nil")
	}
}

func TestTemplateEngine_BuildMessage_BodyExecuteError(t *testing.T) {
	te := NewTemplateEngine()
	if err := te.RegisterTemplate("bad", MessageTemplate{TitleTemplate: "ok", BodyTemplate: "{{call .body}}"}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}
	_, err := te.BuildMessage(Event{EventID: "e", Type: "bad", Payload: map[string]string{"body": "not a function"}})
	if err == nil {
		t.Fatal("BuildMessage with a body template that fails to execute = nil error, want non-nil")
	}
}

// TestTemplateEngine_BuildMessage_DeepLinkExecuteErrorPropagates proves the
// actual bug fix: renderMessage used to swallow a DeepLink render error and
// silently ship the raw unrendered template text; it must now propagate
// the error instead. Parse-time failures are now caught earlier at
// RegisterTemplate (see InvalidDeepLinkSyntax above), so this uses
// {{call .x}} — syntactically valid, but fails at Execute time, the only
// way left to reach this branch.
func TestTemplateEngine_BuildMessage_DeepLinkExecuteErrorPropagates(t *testing.T) {
	te := NewTemplateEngine()
	if err := te.RegisterTemplate("bad", MessageTemplate{
		TitleTemplate: "ok", BodyTemplate: "ok", DeepLink: "myapp://{{call .x}}",
	}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}
	_, err := te.BuildMessage(Event{EventID: "e", Type: "bad", Payload: map[string]string{"x": "not a function"}})
	if err == nil {
		t.Fatal("BuildMessage with a deep link that fails to execute = nil error, want non-nil (not silently swallowed)")
	}
}

func TestTemplateEngine_BuildMessage_ActionURLExecuteErrorPropagates(t *testing.T) {
	te := NewTemplateEngine()
	if err := te.RegisterTemplate("bad", MessageTemplate{
		TitleTemplate: "ok", BodyTemplate: "ok",
		Actions: []NotificationAction{{ID: "a1", Title: "Open", URL: "myapp://{{call .x}}"}},
	}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}
	_, err := te.BuildMessage(Event{EventID: "e", Type: "bad", Payload: map[string]string{"x": "not a function"}})
	if err == nil {
		t.Fatal("BuildMessage with an action url that fails to execute = nil error, want non-nil (not silently swallowed)")
	}
}

func TestRenderInline_ParseError(t *testing.T) {
	if _, err := renderInline("{{.unterminated", nil); err == nil {
		t.Fatal("renderInline(malformed) = nil error, want non-nil")
	}
}

func TestRenderInline_ExecuteError(t *testing.T) {
	if _, err := renderInline("{{call .x}}", map[string]string{"x": "not a function"}); err == nil {
		t.Fatal("renderInline({{call .x}}) = nil error, want non-nil")
	}
}

func TestRenderInline_Success(t *testing.T) {
	got, err := renderInline("hello {{.name}}", map[string]string{"name": "world"})
	if err != nil {
		t.Fatalf("renderInline: %v", err)
	}
	if got != "hello world" {
		t.Fatalf("renderInline() = %q, want %q", got, "hello world")
	}
}
