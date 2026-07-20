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
