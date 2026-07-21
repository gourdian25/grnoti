// File: localization_test.go

package grnoti

import (
	"context"
	"errors"
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

func TestStaticLocaleResolver_ResolveLocaleForAnonymous(t *testing.T) {
	r := NewStaticLocaleResolver("de")
	locale, err := r.ResolveLocaleForAnonymous(context.Background(), "anon-1")
	if err != nil || locale != "de" {
		t.Fatalf("ResolveLocaleForAnonymous() = (%q, %v), want (de, nil)", locale, err)
	}
}

func TestLocalizedTemplateEngine_RegisterTemplate(t *testing.T) {
	base := NewTemplateEngine()
	engine := NewLocalizedTemplateEngine(base, NewInMemoryLocalizationStore(), NewStaticLocaleResolver("en"))

	if err := engine.RegisterTemplate(EventTypeCustom, MessageTemplate{TitleTemplate: "{{.title}}", BodyTemplate: "{{.body}}"}); err != nil {
		t.Fatalf("RegisterTemplate: %v", err)
	}
	if err := engine.RegisterTemplate(EventTypeCustom, MessageTemplate{TitleTemplate: "{{.bad"}); err == nil {
		t.Fatal("RegisterTemplate(malformed template) = nil error, want a parse error (delegated to base engine)")
	}
}

func TestPreferencesLocaleResolver(t *testing.T) {
	store := NewMemoryPreferencesStore()
	if err := store.SavePreferences(context.Background(), &NotificationPreferences{UserID: "u1", GlobalEnabled: true, Locale: "ja"}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}

	r := NewPreferencesLocaleResolver(store, "en")

	t.Run("ResolveLocale_UsesStoredLocale", func(t *testing.T) {
		locale, err := r.ResolveLocale(context.Background(), "u1")
		if err != nil || locale != "ja" {
			t.Fatalf("ResolveLocale() = (%q, %v), want (ja, nil)", locale, err)
		}
	})

	t.Run("ResolveLocale_FallsBackOnMissingPreferences", func(t *testing.T) {
		locale, err := r.ResolveLocale(context.Background(), "no-such-user")
		if err != nil || locale != "en" {
			t.Fatalf("ResolveLocale() = (%q, %v), want (en, nil) on a store miss", locale, err)
		}
	})

	t.Run("ResolveLocale_FallsBackOnEmptyStoredLocale", func(t *testing.T) {
		_ = store.SavePreferences(context.Background(), &NotificationPreferences{UserID: "u2", GlobalEnabled: true})
		locale, err := r.ResolveLocale(context.Background(), "u2")
		if err != nil || locale != "en" {
			t.Fatalf("ResolveLocale() = (%q, %v), want (en, nil) when the stored Locale is empty", locale, err)
		}
	})

	t.Run("ResolveLocaleForAnonymous_AlwaysFallback", func(t *testing.T) {
		locale, err := r.ResolveLocaleForAnonymous(context.Background(), "anon-1")
		if err != nil || locale != "en" {
			t.Fatalf("ResolveLocaleForAnonymous() = (%q, %v), want (en, nil)", locale, err)
		}
	})

	t.Run("GetDefaultLocale", func(t *testing.T) {
		if got := r.GetDefaultLocale(); got != "en" {
			t.Fatalf("GetDefaultLocale() = %q, want en", got)
		}
	})
}

// errorLocaleResolver always fails ResolveLocale/ResolveLocaleForAnonymous
// — used to exercise localizedTemplateEngine.BuildMessage's
// fall-back-to-default-locale-on-error branch.
type errorLocaleResolver struct{ defaultLocale string }

func (r errorLocaleResolver) ResolveLocale(context.Context, string) (string, error) {
	return "", errors.New("resolver boom")
}
func (r errorLocaleResolver) ResolveLocaleForAnonymous(context.Context, string) (string, error) {
	return "", errors.New("resolver boom")
}
func (r errorLocaleResolver) GetDefaultLocale() string { return r.defaultLocale }

func TestLocalizedTemplateEngine_BuildMessage_LocaleResolverErrorFallsBackToDefault(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "en", MessageTemplate{TitleTemplate: "Default locale title", BodyTemplate: "ok"})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, errorLocaleResolver{defaultLocale: "en"})
	msg, err := engine.BuildMessage(Event{EventID: "e", UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title != "Default locale title" {
		t.Fatalf("BuildMessage() = %+v, want the 'en' (default locale) template used after the resolver error", msg)
	}
}

func TestLocalizedTemplateEngine_BuildMessage_NeitherAuthenticatedNorAnonymous(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "en", MessageTemplate{TitleTemplate: "Direct token title", BodyTemplate: "ok"})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("en"))
	msg, err := engine.BuildMessage(Event{EventID: "e", DeviceTokens: []string{"t1"}, Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title != "Direct token title" {
		t.Fatalf("BuildMessage() = %+v, want the default-locale template resolved via GetDefaultLocale", msg)
	}
}

func TestLocalizedTemplateEngine_BuildMessage_CompileErrorFallsBackToBaseEngine(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{TitleTemplate: "{{.unterminated", BodyTemplate: "ok"})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("es"))
	msg, err := engine.BuildMessage(Event{EventID: "e", UserID: "u1", Type: EventTypeSystemAlert})
	if err != nil {
		t.Fatalf("BuildMessage: %v", err)
	}
	if msg.Title == "" {
		t.Fatal("BuildMessage (compile error fallback) produced an empty title")
	}
}

// TestLocalizedTemplateEngine_BuildMessage_ReusesCompiledTemplate is the
// falsifying regression test for compileTemplate being re-run on every
// BuildMessage call instead of once per distinct MessageTemplate: it
// inspects the engine's own cache directly (same package) and asserts the
// *compiledTemplate pointer is identical across two BuildMessage calls for
// an unchanged localized template — proving the second call reused the
// first call's compiled form rather than re-parsing.
func TestLocalizedTemplateEngine_BuildMessage_ReusesCompiledTemplate(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{
		TitleTemplate: "Alerta", BodyTemplate: "{{.message}}",
	})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("es")).(*localizedTemplateEngine)
	ev := Event{EventID: "e", UserID: "u1", Type: EventTypeSystemAlert, Payload: map[string]string{"message": "hola"}}

	if _, err := engine.BuildMessage(ev); err != nil {
		t.Fatalf("BuildMessage (first call): %v", err)
	}
	engine.mu.RLock()
	first := engine.cache["system_alert:es"].compiled
	engine.mu.RUnlock()
	if first == nil {
		t.Fatal("cache has no entry after first BuildMessage call")
	}

	if _, err := engine.BuildMessage(ev); err != nil {
		t.Fatalf("BuildMessage (second call): %v", err)
	}
	engine.mu.RLock()
	second := engine.cache["system_alert:es"].compiled
	engine.mu.RUnlock()
	if second != first {
		t.Fatal("BuildMessage recompiled an unchanged localized template instead of reusing the cached compiledTemplate")
	}
}

// TestLocalizedTemplateEngine_BuildMessage_PicksUpUpdatedTemplate proves
// the cache added to fix the re-compilation issue above doesn't trade one
// bug for another: a template re-registered via
// LocalizationStore.RegisterLocalizedTemplate after it was already cached
// must still take effect on the next BuildMessage call, not serve stale
// compiled content.
func TestLocalizedTemplateEngine_BuildMessage_PicksUpUpdatedTemplate(t *testing.T) {
	localeStore := NewInMemoryLocalizationStore()
	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{TitleTemplate: "Old Title", BodyTemplate: "ok"})

	engine := NewLocalizedTemplateEngine(NewTemplateEngine(), localeStore, NewStaticLocaleResolver("es"))
	ev := Event{EventID: "e", UserID: "u1", Type: EventTypeSystemAlert}

	msg, err := engine.BuildMessage(ev)
	if err != nil {
		t.Fatalf("BuildMessage (before update): %v", err)
	}
	if msg.Title != "Old Title" {
		t.Fatalf("BuildMessage (before update) = %+v, want Title=Old Title", msg)
	}

	_ = localeStore.RegisterLocalizedTemplate(EventTypeSystemAlert, "es", MessageTemplate{TitleTemplate: "New Title", BodyTemplate: "ok"})

	msg, err = engine.BuildMessage(ev)
	if err != nil {
		t.Fatalf("BuildMessage (after update): %v", err)
	}
	if msg.Title != "New Title" {
		t.Fatalf("BuildMessage (after update) = %+v, want Title=New Title (cache must not serve stale content)", msg)
	}
}

func TestNewPreferencesLocaleResolver_DefaultsEmptyFallbackToEn(t *testing.T) {
	r := NewPreferencesLocaleResolver(NewMemoryPreferencesStore(), "")
	if got := r.GetDefaultLocale(); got != "en" {
		t.Fatalf("GetDefaultLocale() = %q, want en (empty fallbackLocale defaults to en)", got)
	}
}
