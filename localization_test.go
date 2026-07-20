// File: localization_test.go

package grnoti

import (
	"context"
	"testing"
)

func TestInMemoryLocalizationStore_RegisterAndGet(t *testing.T) {
	store := NewInMemoryLocalizationStore()
	tmpl := MessageTemplate{TitleTemplate: "Hola", BodyTemplate: "Mundo"}

	if err := store.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", tmpl); err != nil {
		t.Fatalf("RegisterLocalizedTemplate: %v", err)
	}

	got, err := store.GetLocalizedTemplate(EventTypeSystemAlert, "es")
	if err != nil {
		t.Fatalf("GetLocalizedTemplate: %v", err)
	}
	if got.TitleTemplate != "Hola" {
		t.Fatalf("GetLocalizedTemplate() = %+v, want TitleTemplate=Hola", got)
	}
}

func TestInMemoryLocalizationStore_FallsBackToDefaultLocale(t *testing.T) {
	store := NewInMemoryLocalizationStore()
	_ = store.RegisterLocalizedTemplate(EventTypeSystemAlert, "en", MessageTemplate{TitleTemplate: "Hello"})

	got, err := store.GetLocalizedTemplate(EventTypeSystemAlert, "fr")
	if err != nil {
		t.Fatalf("GetLocalizedTemplate (fallback): %v", err)
	}
	if got.TitleTemplate != "Hello" {
		t.Fatalf("GetLocalizedTemplate (fallback) = %+v, want the 'en' default", got)
	}
}

func TestInMemoryLocalizationStore_NotFound(t *testing.T) {
	store := NewInMemoryLocalizationStore()
	if _, err := store.GetLocalizedTemplate("never-registered", "en"); err != ErrTemplateNotFound {
		t.Fatalf("GetLocalizedTemplate() error = %v, want ErrTemplateNotFound", err)
	}
}

func TestInMemoryLocalizationStore_GetSupportedLocales(t *testing.T) {
	store := NewInMemoryLocalizationStore()
	if got := store.GetSupportedLocales("nothing"); len(got) != 0 {
		t.Fatalf("GetSupportedLocales(unknown) = %v, want empty", got)
	}
	_ = store.RegisterLocalizedTemplate(EventTypeSystemAlert, "en", MessageTemplate{})
	_ = store.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{})
	if got := store.GetSupportedLocales(EventTypeSystemAlert); len(got) != 2 {
		t.Fatalf("GetSupportedLocales() = %v, want 2 entries", got)
	}
}

func TestStaticLocaleResolver(t *testing.T) {
	r := NewStaticLocaleResolver("de")
	locale, err := r.ResolveLocale(context.Background(), "any-user")
	if err != nil || locale != "de" {
		t.Fatalf("ResolveLocale() = (%q, %v), want (de, nil)", locale, err)
	}
	if r.GetDefaultLocale() != "de" {
		t.Fatalf("GetDefaultLocale() = %q, want de", r.GetDefaultLocale())
	}
}

func TestLocalizedTemplateEngine_UsesLocalizedTemplateWhenAvailable(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{
		TitleTemplate: "Alerta", BodyTemplate: "{{.message}}",
	})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("es"))
	msg, err := engine.BuildMessage(Event{
		EventID: "evt-1", UserID: "user-1", Type: EventTypeSystemAlert,
		Payload: map[string]string{"message": "algo salió mal"},
	})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title != "Alerta" {
		t.Fatalf("BuildMessage() = %+v, want the localized Spanish title", msg)
	}
}

func TestLocalizedTemplateEngine_FallsBackToBaseEngine(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore() // nothing registered
	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("es"))

	msg, err := engine.BuildMessage(Event{EventID: "evt-1", UserID: "user-1", Type: EventTypePasswordReset})
	if err != nil {
		t.Fatalf("BuildMessage (fallback to base engine): %v", err)
	}
	if msg.Title == "" {
		t.Fatal("BuildMessage (fallback to base engine) produced an empty title")
	}
}
