// File: templateengine.go

package grnoti

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"
)

type compiledTemplate struct {
	titleTmpl   *template.Template
	bodyTmpl    *template.Template
	defaultData map[string]string
	defaultTTL  time.Duration
	collapseKey string
	channelID   string
	sound       string
	actions     []NotificationAction
	deepLink    string
	category    NotificationCategory
}

type defaultTemplateEngine struct {
	mu        sync.RWMutex
	templates map[EventType]*compiledTemplate
}

var _ TemplateEngine = (*defaultTemplateEngine)(nil)

// NewTemplateEngine constructs a TemplateEngine pre-seeded with a small set
// of generic default templates (see registerDefaults) — deliberately
// scheme-free and deep-link-free, unlike the reference implementation,
// which hardcoded a "skipp://" deep-link scheme into 8 of 9 default
// templates (see docs/plan/grnoti-plan.md §2 item 8). Consumers register
// their own application-specific templates (including their own deep-link
// scheme) via RegisterTemplate.
func NewTemplateEngine() TemplateEngine {
	te := &defaultTemplateEngine{templates: make(map[EventType]*compiledTemplate)}
	te.registerDefaults()
	return te
}

func (te *defaultTemplateEngine) registerDefaults() {
	defaults := map[EventType]MessageTemplate{
		EventTypeCustom: {
			TitleTemplate: "{{.title}}",
			BodyTemplate:  "{{.body}}",
			Category:      CategoryTransactional,
		},
		EventTypeSystemAlert: {
			TitleTemplate: "System Alert",
			BodyTemplate:  "{{.message}}",
			ChannelID:     "alerts",
			Sound:         "default",
			Category:      CategoryAlert,
		},
		EventTypeAccountVerification: {
			TitleTemplate: "Verify your account",
			BodyTemplate:  "Please verify your account to continue.",
			ChannelID:     "account",
			Sound:         "default",
			Category:      CategoryTransactional,
		},
		EventTypePasswordReset: {
			TitleTemplate: "Password reset requested",
			BodyTemplate:  "A password reset was requested for your account. If this wasn't you, please secure your account.",
			ChannelID:     "security",
			Sound:         "default",
			Category:      CategoryTransactional,
		},
		EventTypeGenericTransactional: {
			TitleTemplate: "{{.title}}",
			BodyTemplate:  "{{.body}}",
			ChannelID:     "transactional",
			Sound:         "default",
			Category:      CategoryTransactional,
		},
		EventTypeGenericMarketing: {
			TitleTemplate: "{{.title}}",
			BodyTemplate:  "{{.body}}",
			ChannelID:     "promotions",
			Category:      CategoryMarketing,
		},
	}
	for eventType, tmpl := range defaults {
		_ = te.RegisterTemplate(eventType, tmpl)
	}
}

func (te *defaultTemplateEngine) RegisterTemplate(eventType EventType, tmpl MessageTemplate) error {
	compiled, err := compileTemplate(eventType, tmpl)
	if err != nil {
		return err
	}
	te.mu.Lock()
	defer te.mu.Unlock()
	te.templates[eventType] = compiled
	return nil
}

func (te *defaultTemplateEngine) BuildMessage(event Event) (Message, error) {
	te.mu.RLock()
	compiled, ok := te.templates[event.Type]
	if !ok {
		compiled, ok = te.templates[EventTypeCustom]
	}
	te.mu.RUnlock()
	if !ok {
		return Message{}, ErrTemplateNotFound
	}
	return renderMessage(compiled, event)
}

// compileTemplate parses tmpl's title/body text/template sources into a
// compiledTemplate. Shared by defaultTemplateEngine.RegisterTemplate and
// localization.go's LocalizedTemplateEngine, so a localized MessageTemplate
// is compiled once per registration, not re-parsed on every BuildMessage
// call — the reference implementation's LocalizedTemplateEngine
// constructed and fully re-registered a brand-new TemplateEngine (all 9
// default templates included) on every single BuildMessage call just to
// render one localized template; see docs/plan/grnoti-plan.md's research
// notes on localization.go.
func compileTemplate(eventType EventType, tmpl MessageTemplate) (*compiledTemplate, error) {
	titleTmpl, err := template.New("title").Parse(tmpl.TitleTemplate)
	if err != nil {
		return nil, fmt.Errorf("grnoti: parsing title template for %s: %w", eventType, err)
	}
	bodyTmpl, err := template.New("body").Parse(tmpl.BodyTemplate)
	if err != nil {
		return nil, fmt.Errorf("grnoti: parsing body template for %s: %w", eventType, err)
	}
	// DeepLink and each Action.URL are re-parsed lazily per render (see
	// renderMessage) rather than stored here as *template.Template, since
	// that's the existing shape of compiledTemplate — but they must still be
	// validated up front, at RegisterTemplate time, exactly like title/body
	// above: without this, a malformed deep-link/action template would only
	// surface (and previously, silently: see renderMessage) the first time
	// an event of this type was actually sent.
	if strings.Contains(tmpl.DeepLink, "{{") {
		if _, err := template.New("deeplink").Parse(tmpl.DeepLink); err != nil {
			return nil, fmt.Errorf("grnoti: parsing deep link template for %s: %w", eventType, err)
		}
	}
	for i, action := range tmpl.Actions {
		if strings.Contains(action.URL, "{{") {
			if _, err := template.New("action").Parse(action.URL); err != nil {
				return nil, fmt.Errorf("grnoti: parsing action[%d] url template for %s: %w", i, eventType, err)
			}
		}
	}
	return &compiledTemplate{
		titleTmpl:   titleTmpl,
		bodyTmpl:    bodyTmpl,
		defaultData: tmpl.DefaultData,
		defaultTTL:  tmpl.DefaultTTL,
		collapseKey: tmpl.CollapseKey,
		channelID:   tmpl.ChannelID,
		sound:       tmpl.Sound,
		actions:     tmpl.Actions,
		deepLink:    tmpl.DeepLink,
		category:    tmpl.Category,
	}, nil
}

// renderMessage renders compiled against event's payload — the shared
// rendering path for both defaultTemplateEngine and the localized decorator.
func renderMessage(compiled *compiledTemplate, event Event) (Message, error) {
	data := make(map[string]string, len(compiled.defaultData)+len(event.Payload)+3)
	for k, v := range compiled.defaultData {
		data[k] = v
	}
	for k, v := range event.Payload {
		data[k] = v
	}
	data["event_id"] = event.EventID
	data["user_id"] = event.UserID
	data["event_type"] = event.Type.String()

	var titleBuf, bodyBuf bytes.Buffer
	if err := compiled.titleTmpl.Execute(&titleBuf, data); err != nil {
		return Message{}, fmt.Errorf("grnoti: rendering title for %s: %w", event.Type, err)
	}
	if err := compiled.bodyTmpl.Execute(&bodyBuf, data); err != nil {
		return Message{}, fmt.Errorf("grnoti: rendering body for %s: %w", event.Type, err)
	}

	msg := Message{
		Title:       strings.TrimSpace(titleBuf.String()),
		Body:        strings.TrimSpace(bodyBuf.String()),
		Data:        data,
		Priority:    event.Priority,
		TTL:         compiled.defaultTTL,
		CollapseKey: compiled.collapseKey,
		ChannelID:   compiled.channelID,
		Sound:       compiled.sound,
		Category:    compiled.category,
	}

	if ttlStr, ok := event.Payload["ttl"]; ok {
		if d, err := time.ParseDuration(ttlStr); err == nil {
			msg.TTL = d
		}
	}
	if ck, ok := event.Payload["collapse_key"]; ok {
		msg.CollapseKey = ck
	}
	if img, ok := event.Payload["image_url"]; ok {
		msg.ImageURL = img
	}

	msg.Actions = make([]NotificationAction, len(compiled.actions))
	for i, action := range compiled.actions {
		msg.Actions[i] = action
		if strings.Contains(action.URL, "{{") {
			rendered, err := renderInline(action.URL, data)
			if err != nil {
				return Message{}, fmt.Errorf("grnoti: rendering action[%d] url for %s: %w", i, event.Type, err)
			}
			msg.Actions[i].URL = rendered
		}
	}

	msg.DeepLink = compiled.deepLink
	if strings.Contains(compiled.deepLink, "{{") {
		rendered, err := renderInline(compiled.deepLink, data)
		if err != nil {
			return Message{}, fmt.Errorf("grnoti: rendering deep link for %s: %w", event.Type, err)
		}
		msg.DeepLink = rendered
	}
	if dl, ok := event.Payload["deep_link"]; ok {
		msg.DeepLink = dl
	}

	return msg, nil
}

func renderInline(tmplText string, data map[string]string) (string, error) {
	tmpl, err := template.New("inline").Parse(tmplText)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
