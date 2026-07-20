// File: topicrouter.go

package grnoti

import (
	"context"
	"fmt"
)

// TokenTarget is a NotificationTarget resolving to a fixed set of device
// tokens.
type TokenTarget struct {
	Tokens []DeviceToken
}

var _ NotificationTarget = TokenTarget{}

func (t TokenTarget) IsTopicBased() bool       { return false }
func (t TokenTarget) GetTopicName() string     { return "" }
func (t TokenTarget) GetTokens() []DeviceToken { return t.Tokens }

// TopicTarget is a NotificationTarget resolving to an FCM topic.
type TopicTarget struct {
	Topic string
}

var _ NotificationTarget = TopicTarget{}

func (t TopicTarget) IsTopicBased() bool       { return true }
func (t TopicTarget) GetTopicName() string     { return t.Topic }
func (t TopicTarget) GetTokens() []DeviceToken { return nil }

// eventTypeTopicRouter is the primary TopicRouter: resolves in priority
// order — an explicit event.Payload["topic"] override, then a static
// event-type-to-topic mapping, then falls back to per-user device-token
// routing via TokenStore.
type eventTypeTopicRouter struct {
	topicMappings map[EventType]string
	tokenStore    TokenStore
	logger        Logger
}

var _ TopicRouter = (*eventTypeTopicRouter)(nil)

// NewEventTypeTopicRouter constructs the primary TopicRouter.
//
// Parameters:
//   - topicMappings: map[EventType]string — may be nil
//   - tokenStore: TokenStore — used as the fallback when neither a payload
//     override nor a type mapping applies
//   - logger: Logger — may be nil
func NewEventTypeTopicRouter(topicMappings map[EventType]string, tokenStore TokenStore, logger Logger) TopicRouter {
	if topicMappings == nil {
		topicMappings = make(map[EventType]string)
	}
	return &eventTypeTopicRouter{topicMappings: topicMappings, tokenStore: tokenStore, logger: OrNop(logger)}
}

func (r *eventTypeTopicRouter) ResolveTarget(ctx context.Context, event Event) (NotificationTarget, error) {
	if topic, ok := event.Payload["topic"]; ok && topic != "" {
		r.logger.Infof("grnoti: routing event %s to explicit topic override %s", event.EventID, topic)
		return TopicTarget{Topic: topic}, nil
	}

	if topic, ok := r.topicMappings[event.Type]; ok && topic != "" {
		r.logger.Infof("grnoti: routing event %s to mapped topic %s for type %s", event.EventID, topic, event.Type)
		return TopicTarget{Topic: topic}, nil
	}

	r.logger.Infof("grnoti: routing event %s to per-user tokens (no topic override or mapping)", event.EventID)
	tokens, err := r.tokenStore.GetActiveTokens(ctx, event.UserID)
	if err != nil {
		return nil, fmt.Errorf("grnoti: failed to get tokens for user %s: %w", event.UserID, err)
	}
	return TokenTarget{Tokens: tokens}, nil
}

type staticTopicRouter struct{ topic string }

var _ TopicRouter = staticTopicRouter{}

// NewStaticTopicRouter returns a TopicRouter that always resolves to the
// same fixed topic, for tests or single-topic applications.
func NewStaticTopicRouter(topic string) TopicRouter { return staticTopicRouter{topic: topic} }

func (r staticTopicRouter) ResolveTarget(context.Context, Event) (NotificationTarget, error) {
	return TopicTarget{Topic: r.topic}, nil
}

type tokenOnlyRouter struct{ tokenStore TokenStore }

var _ TopicRouter = tokenOnlyRouter{}

// NewTokenOnlyRouter returns a TopicRouter that always routes via
// TokenStore, disabling topic-based routing entirely.
func NewTokenOnlyRouter(tokenStore TokenStore) TopicRouter {
	return tokenOnlyRouter{tokenStore: tokenStore}
}

func (r tokenOnlyRouter) ResolveTarget(ctx context.Context, event Event) (NotificationTarget, error) {
	tokens, err := r.tokenStore.GetActiveTokens(ctx, event.UserID)
	if err != nil {
		return nil, fmt.Errorf("grnoti: failed to get tokens for user %s: %w", event.UserID, err)
	}
	return TokenTarget{Tokens: tokens}, nil
}
