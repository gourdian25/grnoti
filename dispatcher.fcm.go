// File: dispatcher.fcm.go

package grnoti

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"firebase.google.com/go/v4/messaging"
)

const (
	// FCMMaxBatchSize is FCM's documented maximum tokens per multicast
	// request.
	FCMMaxBatchSize = 500

	// DefaultTTL is applied to Android messages whose Message.TTL is unset.
	DefaultTTL = 24 * time.Hour
)

// FCMClient is the subset of Firebase Admin SDK's *messaging.Client that
// fcmDispatcher needs. Unlike every other backend in this repo, which is
// tested against a real local instance (Mongo/Postgres/Redis/Kafka all run
// in docker for their own test suites), FCM has no local emulator for
// actually delivering pushes — this interface exists so fcmDispatcher's own
// logic (batching, retry, error classification, rate-limiter/
// circuit-breaker wiring) can be tested against a fake, the one deliberate
// exception to this repo's real-services testing policy. Matches the
// reference implementation's own justification (fcm.dispatcher.go:26-27).
type FCMClient interface {
	SendEachForMulticast(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error)
	Send(ctx context.Context, message *messaging.Message) (string, error)
}

// FCMDispatcherConfig holds fcmDispatcher's retry tuning. See
// DefaultFCMDispatcherConfig for the recommended starting point.
type FCMDispatcherConfig struct {
	// EnableRetry turns on sendBatchWithRetry. If false, a batch is sent
	// exactly once.
	EnableRetry bool
	// MaxRetryAttempts is the total number of attempts per batch,
	// including the first. Required to be > 0 when EnableRetry is true —
	// unlike a retry *delay*, a retry *count* of 0 has no sane meaning
	// ("retry enabled but never send") to silently default around.
	MaxRetryAttempts int
	// RetryBaseDelay/RetryMaxDelay feed FullJitterBackoff between
	// attempts.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// DefaultFCMDispatcherConfig returns a sane starting configuration: retry
// enabled, 3 attempts, 500ms base / 5s max jittered backoff.
func DefaultFCMDispatcherConfig() FCMDispatcherConfig {
	return FCMDispatcherConfig{
		EnableRetry:      true,
		MaxRetryAttempts: 3,
		RetryBaseDelay:   500 * time.Millisecond,
		RetryMaxDelay:    5 * time.Second,
	}
}

// FCMDispatcherDeps configures an fcmDispatcher constructed by
// NewFCMDispatcher, following WorkerPoolDeps' shape (required collaborator
// + tuning Config + optional collaborators + Logger).
type FCMDispatcherDeps struct {
	// Client is required.
	Client FCMClient
	Config FCMDispatcherConfig

	// RateLimiter, if set, gates every outbound batch/single-send through
	// Wait before it reaches Client — the reference implementation built a
	// RateLimiter but never connected it to dispatch at all
	// (docs/plan/grnoti-plan.md §3.2).
	RateLimiter RateLimiter
	// CircuitBreaker, if set, wraps every Client call — same §3.2 gap for
	// CircuitBreaker.
	CircuitBreaker CircuitBreaker
	// Metrics, if set, receives IncInvalidTokens for tokens FCM reports as
	// permanently invalid. Send's per-eventType/per-platform metrics
	// (IncNotificationsSent/Failed, ObserveDispatchLatency) are NOT called
	// from here: PushDispatcher.Send only ever sees tokens+Message, never
	// the originating Event/EventType those calls require — that wiring
	// belongs one layer up, in NotificationService (Stage 12), which does
	// have the Event.
	Metrics Metrics
	Logger  Logger
}

// fcmDispatcher implements PushDispatcher using Firebase Cloud Messaging.
type fcmDispatcher struct {
	client           FCMClient
	logger           Logger
	retryStrategy    RetryStrategy
	payloadValidator PayloadValidator
	batchSplitter    BatchSplitter
	rateLimiter      RateLimiter
	circuitBreaker   CircuitBreaker
	metrics          Metrics
	config           FCMDispatcherConfig
}

var _ PushDispatcher = (*fcmDispatcher)(nil)

// NewFCMDispatcher constructs an FCM-backed PushDispatcher.
//
// Parameters:
//   - deps: FCMDispatcherDeps — deps.Client is required
//
// Returns:
//   - PushDispatcher
//   - error: ErrFCMClientNil if deps.Client is nil; non-nil if
//     deps.Config.EnableRetry is true and MaxRetryAttempts <= 0
func NewFCMDispatcher(deps FCMDispatcherDeps) (PushDispatcher, error) {
	if deps.Client == nil {
		return nil, ErrFCMClientNil
	}
	if deps.Config.EnableRetry && deps.Config.MaxRetryAttempts <= 0 {
		return nil, fmt.Errorf("grnoti/fcm: FCMDispatcherConfig.MaxRetryAttempts must be > 0 when EnableRetry is true")
	}

	var retryStrategy RetryStrategy
	if deps.Config.EnableRetry {
		retryStrategy = NewFullJitterRetry(deps.Config.MaxRetryAttempts, deps.Config.RetryBaseDelay, deps.Config.RetryMaxDelay)
	} else {
		retryStrategy = NewNoopRetryStrategy()
	}

	return &fcmDispatcher{
		client:           deps.Client,
		logger:           OrNop(deps.Logger),
		retryStrategy:    retryStrategy,
		payloadValidator: NewFCMPayloadValidator(),
		batchSplitter:    NewBatchSplitter(),
		rateLimiter:      deps.RateLimiter,
		circuitBreaker:   deps.CircuitBreaker,
		metrics:          deps.Metrics,
		config:           deps.Config,
	}, nil
}

// Send dispatches msg to every token, deduplicated, grouped by platform,
// and batched at FCMMaxBatchSize — each platform group's batches are sent
// concurrently with each other (matching the reference's per-platform
// goroutine fan-out), while batches within one platform are sent
// sequentially so a shared RateLimiter/CircuitBreaker sees them one at a
// time in submission order.
func (d *fcmDispatcher) Send(ctx context.Context, tokens []DeviceToken, msg Message) (DispatchResult, error) {
	if len(tokens) == 0 {
		return DispatchResult{}, nil
	}
	if err := d.payloadValidator.ValidateSize(msg); err != nil {
		d.logger.Error("grnoti/fcm: payload validation failed", "error", err)
		return DispatchResult{FailureCount: len(tokens), Errors: []error{err}}, err
	}

	byPlatform := make(map[Platform][]DeviceToken)
	for _, t := range d.batchSplitter.Deduplicate(tokens) {
		platform := t.Platform
		if platform == "" {
			platform = PlatformAndroid // preserve the reference's "default to Android" fallback for an unset platform
		}
		byPlatform[platform] = append(byPlatform[platform], t)
	}

	d.logger.Info("grnoti/fcm: dispatching", "tokens", len(tokens), "platform_groups", len(byPlatform))

	result := DispatchResult{
		SuccessByPlatform: make(map[Platform]int, len(byPlatform)),
		FailureByPlatform: make(map[Platform]int, len(byPlatform)),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for platform, platformTokens := range byPlatform {
		wg.Add(1)
		go func(platform Platform, platformTokens []DeviceToken) {
			defer wg.Done()
			r := d.sendPlatformBatches(ctx, platformTokens, msg, platform)
			mu.Lock()
			result.SuccessCount += r.SuccessCount
			result.FailureCount += r.FailureCount
			result.InvalidTokens = append(result.InvalidTokens, r.InvalidTokens...)
			result.RetryableErrors += r.RetryableErrors
			result.Errors = append(result.Errors, r.Errors...)
			result.SuccessByPlatform[platform] += r.SuccessCount
			result.FailureByPlatform[platform] += r.FailureCount
			mu.Unlock()
		}(platform, platformTokens)
	}
	wg.Wait()

	if d.metrics != nil && len(result.InvalidTokens) > 0 {
		d.metrics.IncInvalidTokens(len(result.InvalidTokens))
	}
	d.logger.Info("grnoti/fcm: dispatch complete",
		"success", result.SuccessCount, "failure", result.FailureCount, "invalid", len(result.InvalidTokens), "retryable", result.RetryableErrors)
	return result, nil
}

// sendPlatformBatches splits tokens (already grouped to one platform by the
// caller) into FCMMaxBatchSize-sized batches and sends each in turn,
// merging results. A batch is skipped (counted as a failure, not attempted)
// once ctx is already canceled, rather than letting every remaining batch
// individually discover that via sendBatch/sendBatchWithRetry — cheaper,
// and avoids a burst of identical "context canceled" log lines.
func (d *fcmDispatcher) sendPlatformBatches(ctx context.Context, tokens []DeviceToken, msg Message, platform Platform) DispatchResult {
	var result DispatchResult
	for _, batch := range d.batchSplitter.Split(tokens, FCMMaxBatchSize) {
		if err := ctx.Err(); err != nil {
			result.FailureCount += len(batch)
			result.Errors = append(result.Errors, err)
			continue
		}

		var batchResult DispatchResult
		if d.config.EnableRetry {
			batchResult = d.sendBatchWithRetry(ctx, batch, msg, platform)
		} else {
			batchResult, _ = d.sendBatch(ctx, batch, msg, platform)
		}
		result.SuccessCount += batchResult.SuccessCount
		result.FailureCount += batchResult.FailureCount
		result.InvalidTokens = append(result.InvalidTokens, batchResult.InvalidTokens...)
		result.RetryableErrors += batchResult.RetryableErrors
		result.Errors = append(result.Errors, batchResult.Errors...)
	}
	return result
}

// sendBatchWithRetry retries a batch up to d.config.MaxRetryAttempts
// attempts total. Two distinct failure shapes can make an attempt worth
// retrying, and they're judged differently: a total request-level failure
// (sendBatch returns a non-nil error — e.g. the FCM call itself errored,
// or the circuit breaker rejected it) defers to d.retryStrategy.ShouldRetry,
// which classifies that specific error; a partial per-token failure
// (sendBatch returns nil error but result.RetryableErrors > 0) has already
// had each failing token individually classified by classifyFCMError, so
// RetryableErrors > 0 is itself the retry signal — routing it back through
// ShouldRetry(attempt, nil) would never retry at all, since a nil err
// unconditionally means "don't retry" there.
func (d *fcmDispatcher) sendBatchWithRetry(ctx context.Context, tokens []DeviceToken, msg Message, platform Platform) DispatchResult {
	var result DispatchResult
	var lastErr error

	for attempt := 0; attempt < d.config.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			delay := d.retryStrategy.GetDelay(attempt - 1)
			d.logger.Debug("grnoti/fcm: retrying batch", "platform", platform, "attempt", attempt, "delay", delay)
			select {
			case <-ctx.Done():
				result.Errors = append(result.Errors, ctx.Err())
				return result
			case <-time.After(delay):
			}
		}

		result, lastErr = d.sendBatch(ctx, tokens, msg, platform)

		shouldRetry := result.RetryableErrors > 0
		if lastErr != nil {
			shouldRetry = d.retryStrategy.ShouldRetry(attempt, lastErr)
		}
		if !shouldRetry {
			break
		}
	}
	return result
}

// sendBatch sends one batch (<= FCMMaxBatchSize tokens), gated through
// RateLimiter.Wait and wrapped in CircuitBreaker.Execute when configured —
// the actual wiring the reference implementation built both components for
// but never connected (docs/plan/grnoti-plan.md §3.2).
func (d *fcmDispatcher) sendBatch(ctx context.Context, tokens []DeviceToken, msg Message, platform Platform) (DispatchResult, error) {
	var result DispatchResult

	if d.rateLimiter != nil {
		if err := d.rateLimiter.Wait(ctx); err != nil {
			result.FailureCount = len(tokens)
			result.Errors = append(result.Errors, fmt.Errorf("grnoti/fcm: rate limiter wait: %w", err))
			return result, err
		}
	}

	tokenStrs := make([]string, len(tokens))
	for i, t := range tokens {
		tokenStrs[i] = t.Token
	}
	multicastMsg := d.buildMulticastMessage(tokenStrs, msg, platform)

	var resp *messaging.BatchResponse
	sendFn := func() error {
		var sendErr error
		resp, sendErr = d.client.SendEachForMulticast(ctx, multicastMsg)
		return sendErr
	}

	var err error
	if d.circuitBreaker != nil {
		err = d.circuitBreaker.Execute(ctx, sendFn)
	} else {
		err = sendFn()
	}
	if err != nil {
		result.FailureCount = len(tokens)
		result.Errors = append(result.Errors, err)
		return result, err
	}

	result.SuccessCount = int(resp.SuccessCount)
	result.FailureCount = int(resp.FailureCount)
	for idx, sendResp := range resp.Responses {
		if sendResp.Success || sendResp.Error == nil {
			continue
		}
		fcmErr := classifyFCMError(sendResp.Error, tokenStrs[idx])
		result.Errors = append(result.Errors, fcmErr)
		if fcmErr.IsPermanent() {
			result.InvalidTokens = append(result.InvalidTokens, tokenStrs[idx])
		} else if fcmErr.IsRetryable() {
			result.RetryableErrors++
		}
	}
	return result, nil
}

// SendToToken sends msg to a single token, gated/wrapped the same way as
// sendBatch.
func (d *fcmDispatcher) SendToToken(ctx context.Context, token DeviceToken, msg Message) error {
	if err := d.payloadValidator.ValidateSize(msg); err != nil {
		return err
	}
	if d.rateLimiter != nil {
		if err := d.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("grnoti/fcm: rate limiter wait: %w", err)
		}
	}

	fcmMsg := d.buildFCMMessage(token.Token, msg, token.Platform)
	sendFn := func() error {
		_, err := d.client.Send(ctx, fcmMsg)
		return err
	}

	var err error
	if d.circuitBreaker != nil {
		err = d.circuitBreaker.Execute(ctx, sendFn)
	} else {
		err = sendFn()
	}
	if err != nil {
		fcmErr := classifyFCMError(err, token.Token)
		d.logger.Error("grnoti/fcm: send to token failed", "token", maskToken(token.Token), "code", fcmErr.Code, "error", err)
		return fcmErr
	}
	d.logger.Debug("grnoti/fcm: sent to token", "token", maskToken(token.Token), "platform", token.Platform)
	return nil
}

// SendToTopic sends msg to every device subscribed to topic.
func (d *fcmDispatcher) SendToTopic(ctx context.Context, topic string, msg Message) error {
	if err := d.payloadValidator.ValidateSize(msg); err != nil {
		return err
	}
	if d.rateLimiter != nil {
		if err := d.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("grnoti/fcm: rate limiter wait: %w", err)
		}
	}

	fcmMsg := d.buildTopicMessage(topic, msg)
	sendFn := func() error {
		_, err := d.client.Send(ctx, fcmMsg)
		return err
	}

	var err error
	if d.circuitBreaker != nil {
		err = d.circuitBreaker.Execute(ctx, sendFn)
	} else {
		err = sendFn()
	}
	if err != nil {
		d.logger.Error("grnoti/fcm: send to topic failed", "topic", topic, "error", err)
		return err
	}
	d.logger.Info("grnoti/fcm: sent to topic", "topic", topic)
	return nil
}

func (d *fcmDispatcher) buildTopicMessage(topic string, msg Message) *messaging.Message {
	notification := &messaging.Notification{Title: msg.Title, Body: msg.Body}
	if msg.ImageURL != "" {
		notification.ImageURL = msg.ImageURL
	}
	return &messaging.Message{
		Topic:        topic,
		Notification: notification,
		Data:         msg.Data,
		Android:      d.buildAndroidConfig(msg),
		APNS:         d.buildAPNSConfig(msg),
		Webpush:      d.buildWebpushConfig(msg),
	}
}

func (d *fcmDispatcher) buildMulticastMessage(tokens []string, msg Message, platform Platform) *messaging.MulticastMessage {
	notification := &messaging.Notification{Title: msg.Title, Body: msg.Body}
	if msg.ImageURL != "" {
		notification.ImageURL = msg.ImageURL
	}
	multicast := &messaging.MulticastMessage{Tokens: tokens, Notification: notification, Data: msg.Data}
	switch platform {
	case PlatformAndroid:
		multicast.Android = d.buildAndroidConfig(msg)
	case PlatformIOS:
		multicast.APNS = d.buildAPNSConfig(msg)
	case PlatformWeb:
		multicast.Webpush = d.buildWebpushConfig(msg)
	}
	return multicast
}

func (d *fcmDispatcher) buildFCMMessage(token string, msg Message, platform Platform) *messaging.Message {
	notification := &messaging.Notification{Title: msg.Title, Body: msg.Body}
	if msg.ImageURL != "" {
		notification.ImageURL = msg.ImageURL
	}
	fcmMsg := &messaging.Message{Token: token, Notification: notification, Data: msg.Data}
	switch platform {
	case PlatformAndroid:
		fcmMsg.Android = d.buildAndroidConfig(msg)
	case PlatformIOS:
		fcmMsg.APNS = d.buildAPNSConfig(msg)
	case PlatformWeb:
		fcmMsg.Webpush = d.buildWebpushConfig(msg)
	}
	return fcmMsg
}

// buildAndroidConfig maps Message onto FCM's Android-specific payload.
// DeepLink/Actions/Category have no first-class field on
// messaging.AndroidConfig, so they're carried in the free-form Data map
// (as JSON for Actions) for the client app to read on receipt — config.Data
// is only allocated when at least one of those three is actually present,
// so a plain notification doesn't grow an empty map.
func (d *fcmDispatcher) buildAndroidConfig(msg Message) *messaging.AndroidConfig {
	priority := "normal"
	if msg.Priority == PriorityHigh {
		priority = "high"
	}
	ttl := msg.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}

	config := &messaging.AndroidConfig{Priority: priority, TTL: &ttl}
	if msg.CollapseKey != "" {
		config.CollapseKey = msg.CollapseKey
	}

	androidNotification := &messaging.AndroidNotification{}
	if msg.ChannelID != "" {
		androidNotification.ChannelID = msg.ChannelID
	}
	if msg.Sound != "" {
		androidNotification.Sound = msg.Sound
	}
	config.Notification = androidNotification

	if msg.DeepLink != "" || len(msg.Actions) > 0 || msg.Category != "" {
		config.Data = make(map[string]string)
	}
	if msg.DeepLink != "" {
		config.Data["click_action"] = msg.DeepLink
		config.Data["deep_link"] = msg.DeepLink
	}
	if len(msg.Actions) > 0 {
		if actionsJSON, err := json.Marshal(msg.Actions); err == nil {
			config.Data["actions"] = string(actionsJSON)
		}
	}
	if msg.Category != "" {
		config.Data["category"] = string(msg.Category)
	}
	return config
}

// buildAPNSConfig maps Message onto FCM's APNS-specific payload. Unlike
// buildAndroidConfig, DeepLink/Actions ride in APNSPayload.CustomData
// (arbitrary top-level JSON keys alongside "aps"), APNS's own equivalent of
// Android's Data map; Category, having a first-class Aps.Category field,
// doesn't need it. MutableContent is always set so a client-side
// Notification Service Extension can rewrite the notification (e.g. fetch
// and attach ImageURL) before display — grnoti doesn't do that itself, but
// leaves the door open for a consuming app's own extension.
func (d *fcmDispatcher) buildAPNSConfig(msg Message) *messaging.APNSConfig {
	headers := make(map[string]string)
	if msg.Priority == PriorityHigh {
		headers["apns-priority"] = "10"
	} else {
		headers["apns-priority"] = "5"
	}
	if msg.TTL > 0 {
		headers["apns-expiration"] = strconv.FormatInt(time.Now().Add(msg.TTL).Unix(), 10)
	}
	if msg.CollapseKey != "" {
		headers["apns-collapse-id"] = msg.CollapseKey
	}

	aps := &messaging.Aps{
		Alert:          &messaging.ApsAlert{Title: msg.Title, Body: msg.Body},
		MutableContent: true,
	}
	if msg.Sound != "" {
		aps.Sound = msg.Sound
	} else {
		aps.Sound = "default"
	}
	if msg.Badge != nil {
		aps.Badge = msg.Badge
	}
	if msg.Category != "" {
		aps.Category = string(msg.Category)
	}

	customData := make(map[string]any)
	if msg.DeepLink != "" {
		customData["deep_link"] = msg.DeepLink
		customData["click_action"] = msg.DeepLink
	}
	if len(msg.Actions) > 0 {
		customData["actions"] = msg.Actions
	}

	payload := &messaging.APNSPayload{Aps: aps}
	if len(customData) > 0 {
		payload.CustomData = customData
	}
	return &messaging.APNSConfig{Headers: headers, Payload: payload}
}

// buildWebpushConfig maps Message onto FCM's Web Push payload. Unlike
// Android/APNS (which only branch on == PriorityHigh, see the Priority
// const block in types.go), Webpush's "Urgency" header has a genuine three
// -way mapping and is the one platform where PriorityLow is actually
// distinguished from PriorityNormal. Actions map onto
// WebpushNotificationAction directly (Web Push's browser-rendered action
// buttons), unlike Android/APNS which have no native action-button concept
// and so carry Actions as opaque JSON/CustomData instead.
func (d *fcmDispatcher) buildWebpushConfig(msg Message) *messaging.WebpushConfig {
	headers := make(map[string]string)
	if msg.TTL > 0 {
		headers["TTL"] = strconv.Itoa(int(msg.TTL.Seconds()))
	}
	switch msg.Priority {
	case PriorityHigh:
		headers["Urgency"] = "high"
	case PriorityLow:
		headers["Urgency"] = "low"
	default:
		headers["Urgency"] = "normal"
	}

	notification := &messaging.WebpushNotification{Title: msg.Title, Body: msg.Body}
	if msg.ImageURL != "" {
		notification.Image = msg.ImageURL
	}
	if len(msg.Actions) > 0 {
		notification.Actions = make([]*messaging.WebpushNotificationAction, len(msg.Actions))
		for i, action := range msg.Actions {
			notification.Actions[i] = &messaging.WebpushNotificationAction{Action: action.ID, Title: action.Title, Icon: action.Icon}
		}
	}

	config := &messaging.WebpushConfig{Headers: headers, Notification: notification}
	if msg.DeepLink != "" || msg.Category != "" {
		config.Data = make(map[string]string)
		if msg.DeepLink != "" {
			config.Data["deep_link"] = msg.DeepLink
			config.Data["click_action"] = msg.DeepLink
		}
		if msg.Category != "" {
			config.Data["category"] = string(msg.Category)
		}
	}
	return config
}

// classifyFCMError classifies a raw FCM SDK error into a typed FCMError by
// substring-matching its message — the FCM Admin SDK does not expose a
// structured error-code type, only text, so this is the same approach the
// reference implementation used (fcm.dispatcher.go:632-666), kept as-is
// since it's the actually-wired classification (see docs/plan/
// grnoti-plan.md §3.3: the reference also had 12 dead ErrFCM* sentinels
// that never touched this classification at all — those are simply not
// present in grnoti's errors.go, so there's nothing to remove).
func classifyFCMError(err error, token string) *FCMError {
	if err == nil {
		return nil
	}
	errMsg := err.Error()
	switch {
	case containsAnyFold(errMsg, "unregistered", "not-registered", "NotRegistered"):
		return NewFCMError(FCMErrorCodeUnregistered, token, "token is no longer registered", err)
	case containsAnyFold(errMsg, "invalid-argument", "InvalidRegistration", "invalid registration"):
		return NewFCMError(FCMErrorCodeInvalidArgument, token, "invalid token or payload", err)
	case containsAnyFold(errMsg, "sender-id-mismatch", "MismatchSenderId"):
		return NewFCMError(FCMErrorCodeSenderIDMismatch, token, "sender ID mismatch", err)
	case containsAnyFold(errMsg, "quota-exceeded", "QuotaExceeded", "rate limit"):
		return NewFCMError(FCMErrorCodeQuotaExceeded, token, "quota exceeded", err)
	case containsAnyFold(errMsg, "unavailable", "temporarily unavailable"):
		return NewFCMError(FCMErrorCodeUnavailable, token, "FCM service temporarily unavailable", err)
	case containsAnyFold(errMsg, "internal", "InternalError"):
		return NewFCMError(FCMErrorCodeInternal, token, "FCM internal error", err)
	case containsAnyFold(errMsg, "third-party-auth-error"):
		return NewFCMError(FCMErrorCodeThirdPartyAuthErr, token, "third party auth error", err)
	default:
		return NewFCMError(FCMErrorCodeUnspecified, token, "unspecified FCM error", err)
	}
}

func containsAnyFold(s string, substrings ...string) bool {
	lower := strings.ToLower(s)
	for _, sub := range substrings {
		if strings.Contains(lower, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

// maskToken masks a device token for logging, showing only its first 8 and
// last 4 characters.
func maskToken(token string) string {
	if len(token) <= 12 {
		return "***"
	}
	return token[:8] + "..." + token[len(token)-4:]
}
