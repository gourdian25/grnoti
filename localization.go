// File: localization.go

package grnoti

import (
	"context"
	"reflect"
	"sync"
)

type inMemoryLocalizationStore struct {
	mu        sync.RWMutex
	templates map[EventType]*LocalizedTemplate
}

var _ LocalizationStore = (*inMemoryLocalizationStore)(nil)

// NewInMemoryLocalizationStore constructs an in-memory LocalizationStore.
func NewInMemoryLocalizationStore() LocalizationStore {
	return &inMemoryLocalizationStore{templates: make(map[EventType]*LocalizedTemplate)}
}

func (s *inMemoryLocalizationStore) RegisterLocalizedTemplate(eventType EventType, locale string, tmpl MessageTemplate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.templates[eventType]
	if !ok {
		entry = &LocalizedTemplate{DefaultLocale: "en", Templates: make(map[string]MessageTemplate)}
		s.templates[eventType] = entry
	}
	entry.Templates[locale] = tmpl
	return nil
}

func (s *inMemoryLocalizationStore) GetLocalizedTemplate(eventType EventType, locale string) (MessageTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.templates[eventType]
	if !ok {
		return MessageTemplate{}, ErrTemplateNotFound
	}
	if tmpl, ok := entry.Templates[locale]; ok {
		return tmpl, nil
	}
	if tmpl, ok := entry.Templates[entry.DefaultLocale]; ok {
		return tmpl, nil
	}
	return MessageTemplate{}, ErrTemplateNotFound
}

func (s *inMemoryLocalizationStore) GetSupportedLocales(eventType EventType) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.templates[eventType]
	if !ok {
		return []string{}
	}
	locales := make([]string, 0, len(entry.Templates))
	for locale := range entry.Templates {
		locales = append(locales, locale)
	}
	return locales
}

// preferencesLocaleResolver resolves a user's locale from their stored
// NotificationPreferences, falling back to a fixed locale on any lookup
// failure (missing preferences, store error) rather than propagating the
// error — locale resolution is best-effort by design, since failing to
// render a notification in the exact right locale is preferable to failing
// to send it at all.
type preferencesLocaleResolver struct {
	store          PreferencesStore
	fallbackLocale string
}

var _ LocaleResolver = (*preferencesLocaleResolver)(nil)

// NewPreferencesLocaleResolver constructs a LocaleResolver backed by store.
//
// Parameters:
//   - store: PreferencesStore
//   - fallbackLocale: string — used when a user has no stored locale
//     preference, or store lookup fails; defaults to "en" if empty
func NewPreferencesLocaleResolver(store PreferencesStore, fallbackLocale string) LocaleResolver {
	if fallbackLocale == "" {
		fallbackLocale = "en"
	}
	return &preferencesLocaleResolver{store: store, fallbackLocale: fallbackLocale}
}

func (r *preferencesLocaleResolver) ResolveLocale(ctx context.Context, userID string) (string, error) {
	prefs, err := r.store.GetPreferences(ctx, userID)
	if err != nil || prefs.Locale == "" {
		return r.fallbackLocale, nil
	}
	return prefs.Locale, nil
}

func (r *preferencesLocaleResolver) ResolveLocaleForAnonymous(context.Context, string) (string, error) {
	return r.fallbackLocale, nil
}

func (r *preferencesLocaleResolver) GetDefaultLocale() string { return r.fallbackLocale }

type staticLocaleResolver struct{ locale string }

var _ LocaleResolver = staticLocaleResolver{}

// NewStaticLocaleResolver returns a LocaleResolver that always resolves to
// locale, for tests or single-language applications.
func NewStaticLocaleResolver(locale string) LocaleResolver {
	return staticLocaleResolver{locale: locale}
}

func (r staticLocaleResolver) ResolveLocale(context.Context, string) (string, error) {
	return r.locale, nil
}
func (r staticLocaleResolver) ResolveLocaleForAnonymous(context.Context, string) (string, error) {
	return r.locale, nil
}
func (r staticLocaleResolver) GetDefaultLocale() string { return r.locale }

// localizedTemplateEngine wraps a TemplateEngine with per-user locale
// resolution: BuildMessage resolves the event's target locale, looks up a
// localized MessageTemplate, and renders it directly via the shared
// renderMessage path — falling back to baseEngine.BuildMessage if no
// localized template is registered for the event's type. Unlike the
// reference implementation, this never constructs a throwaway TemplateEngine
// per call; see compileTemplate/renderMessage's doc comments.
//
// A compiled-template cache (keyed by eventType+locale) avoids re-parsing
// the same MessageTemplate on every BuildMessage call: LocalizationStore
// has no registration hook this engine can intercept the way
// defaultTemplateEngine.RegisterTemplate compiles once up front, so the
// cache is populated lazily on first use instead. Each cache hit is
// validated with reflect.DeepEqual against the just-fetched
// MessageTemplate (a cheap struct comparison next to text/template
// parsing) before being reused, so a template updated via
// LocalizationStore.RegisterLocalizedTemplate after it was first cached is
// still picked up on the next BuildMessage call rather than serving a
// stale compiled form indefinitely.
type localizedTemplateEngine struct {
	baseEngine     TemplateEngine
	localeStore    LocalizationStore
	localeResolver LocaleResolver

	mu    sync.RWMutex
	cache map[string]localizedCacheEntry // key: string(eventType) + ":" + locale
}

type localizedCacheEntry struct {
	source   MessageTemplate
	compiled *compiledTemplate
}

var _ TemplateEngine = (*localizedTemplateEngine)(nil)

// NewLocalizedTemplateEngine wraps baseEngine with locale-aware rendering,
// itself implementing TemplateEngine so it's a drop-in replacement.
func NewLocalizedTemplateEngine(baseEngine TemplateEngine, localeStore LocalizationStore, localeResolver LocaleResolver) TemplateEngine {
	return &localizedTemplateEngine{
		baseEngine:     baseEngine,
		localeStore:    localeStore,
		localeResolver: localeResolver,
		cache:          make(map[string]localizedCacheEntry),
	}
}

func (e *localizedTemplateEngine) RegisterTemplate(eventType EventType, tmpl MessageTemplate) error {
	return e.baseEngine.RegisterTemplate(eventType, tmpl)
}

func (e *localizedTemplateEngine) BuildMessage(event Event) (Message, error) {
	var locale string
	var err error
	switch {
	case event.IsAuthenticated():
		locale, err = e.localeResolver.ResolveLocale(context.Background(), event.UserID)
	case event.IsAnonymous():
		locale, err = e.localeResolver.ResolveLocaleForAnonymous(context.Background(), event.AnonymousID)
	default:
		locale = e.localeResolver.GetDefaultLocale()
	}
	if err != nil {
		locale = e.localeResolver.GetDefaultLocale()
	}

	localizedTmpl, err := e.localeStore.GetLocalizedTemplate(event.Type, locale)
	if err != nil {
		return e.baseEngine.BuildMessage(event)
	}

	compiled, err := e.getCompiled(event.Type, locale, localizedTmpl)
	if err != nil {
		return e.baseEngine.BuildMessage(event)
	}
	return renderMessage(compiled, event)
}

// getCompiled returns a compiledTemplate for source, reusing a cached one
// if source is unchanged since it was cached, and compiling (then caching)
// otherwise. A source that fails to compile is not cached — it is
// re-attempted (and re-fail) on every call, matching this engine's
// existing fall-back-to-base-engine behavior for a malformed template
// rather than caching a failure.
func (e *localizedTemplateEngine) getCompiled(eventType EventType, locale string, source MessageTemplate) (*compiledTemplate, error) {
	key := string(eventType) + ":" + locale

	e.mu.RLock()
	entry, ok := e.cache[key]
	e.mu.RUnlock()
	if ok && reflect.DeepEqual(entry.source, source) {
		return entry.compiled, nil
	}

	compiled, err := compileTemplate(eventType, source)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	e.cache[key] = localizedCacheEntry{source: source, compiled: compiled}
	e.mu.Unlock()

	return compiled, nil
}
