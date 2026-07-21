// File: example/main.go

// Command example is a minimal, fully self-contained walkthrough of grnoti:
// no Docker containers, no Firebase credentials, no network access at all.
// It wires a NotificationService from in-memory backends plus a
// stdout-logging PushDispatcher, then processes a handful of events to
// show the pipeline end to end (template rendering, preferences filtering,
// idempotency, DLQ-on-failure, metrics).
//
// Run it with:
//
//	go run ./example
//
// Swapping in a real backend is a one-line change at its construction site
// — see the "Using real backends instead" comment near the bottom of this
// file for the exact constructor call each swap needs
// (NewMongoTokenStore, NewPostgresPreferencesStore, NewFCMDispatcher, ...).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gourdian25/grcache/memory"
	"github.com/gourdian25/grnoti"
)

// EventTypeOrderShipped is an application-defined event type — grnoti
// places no constraints on EventType beyond "non-empty" (see
// EventType.IsValid), so any application-specific string works.
const EventTypeOrderShipped grnoti.EventType = "order_shipped"

// loggingDispatcher is a PushDispatcher that prints what it would send
// instead of calling FCM — this is the one piece every real deployment
// must supply itself (via grnoti.NewFCMDispatcher, wired to real Firebase
// credentials); everything else in this example is a genuine grnoti
// backend.
type loggingDispatcher struct{}

func (loggingDispatcher) Send(_ context.Context, tokens []grnoti.DeviceToken, msg grnoti.Message) (grnoti.DispatchResult, error) {
	result := grnoti.DispatchResult{
		SuccessCount:      len(tokens),
		SuccessByPlatform: make(map[grnoti.Platform]int, len(tokens)),
	}
	for _, tok := range tokens {
		fmt.Printf("  [push] -> token=%s platform=%s title=%q body=%q\n", tok.Token, tok.Platform, msg.Title, msg.Body)
		result.SuccessByPlatform[tok.Platform]++
	}
	return result, nil
}

func (loggingDispatcher) SendToToken(_ context.Context, token grnoti.DeviceToken, msg grnoti.Message) error {
	fmt.Printf("  [push] -> token=%s platform=%s title=%q body=%q\n", token.Token, token.Platform, msg.Title, msg.Body)
	return nil
}

func (loggingDispatcher) SendToTopic(_ context.Context, topic string, msg grnoti.Message) error {
	fmt.Printf("  [push] -> topic=%s title=%q body=%q\n", topic, msg.Title, msg.Body)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds every fallible step and all cleanup, so a mid-setup error can
// simply be returned — main stays defer-free, and every defer registered
// in here still runs on the way out regardless of which step failed.
func run() error {
	ctx := context.Background()

	// --- 1. Templates: NewTemplateEngine() ships with generic
	// scheme-free defaults (see docs.go); RegisterTemplate adds
	// application-specific ones on top. ---

	templates := grnoti.NewTemplateEngine()
	if err := templates.RegisterTemplate(EventTypeOrderShipped, grnoti.MessageTemplate{
		TitleTemplate: "Your order has shipped!",
		BodyTemplate:  "Order #{{.order_id}} is on its way — track it at {{.tracking_url}}.",
		ChannelID:     "orders",
		Sound:         "default",
		Category:      grnoti.CategoryTransactional,
		DeepLink:      "myapp://orders/{{.order_id}}",
	}); err != nil {
		return fmt.Errorf("RegisterTemplate: %w", err)
	}

	// --- 2. Backends: every one of these is a real grnoti implementation,
	// just the in-memory/local variant rather than the Mongo/Postgres/Redis
	// one a production deployment would use. ---

	tokenStore := grnoti.NewMemoryTokenStore()
	preferencesStore := grnoti.NewMemoryPreferencesStore()
	dlqHandler := grnoti.NewMemoryDLQHandler(3, time.Minute, 10*time.Minute)

	idempotencyCache, err := memory.NewMemoryCache()
	if err != nil {
		return fmt.Errorf("memory.NewMemoryCache: %w", err)
	}
	defer func() { _ = idempotencyCache.Close() }()
	idempotencyStore := grnoti.NewCacheIdempotencyStore(idempotencyCache)

	// --- 3. The service itself. ---

	config := grnoti.DefaultServiceConfig()
	config.EnablePreferencesFilter = true
	config.EnableDLQ = true

	svc, err := grnoti.NewNotificationService(grnoti.ServiceDeps{
		TokenStore:        tokenStore,
		Dispatcher:        loggingDispatcher{},
		Templates:         templates,
		Idempotency:       idempotencyStore,
		PreferencesFilter: grnoti.NewPreferencesFilter(preferencesStore, nil),
		DLQHandler:        dlqHandler,
		Config:            config,
	})
	if err != nil {
		return fmt.Errorf("NewNotificationService: %w", err)
	}
	defer func() { _ = svc.Close() }()

	// --- 4. Seed a user with a registered device and their notification
	// preferences, matching what an application's own signup/login and
	// settings flows would normally do via TokenStore.SaveToken and
	// PreferencesStore.SavePreferences directly. ---

	const userID = "user-42"
	if err := tokenStore.SaveToken(ctx, grnoti.DeviceToken{
		Token: "example-device-token", UserID: userID, Platform: grnoti.PlatformAndroid,
	}); err != nil {
		return fmt.Errorf("SaveToken: %w", err)
	}
	if err := preferencesStore.SavePreferences(ctx, &grnoti.NotificationPreferences{
		UserID: userID, GlobalEnabled: true, Locale: "en",
	}); err != nil {
		return fmt.Errorf("SavePreferences: %w", err)
	}

	// --- 5. Process a few events. ---

	fmt.Println("--- event 1: order shipped, notification succeeds ---")
	result, err := svc.ProcessEvent(ctx, grnoti.Event{
		EventID:  "evt-1",
		UserID:   userID,
		Type:     EventTypeOrderShipped,
		Priority: grnoti.PriorityHigh,
		Payload:  map[string]string{"order_id": "1001", "tracking_url": "https://example.com/t/1001"},
	})
	if err != nil {
		return fmt.Errorf("ProcessEvent: %w", err)
	}
	fmt.Printf("  result: tokens=%d success=%d failure=%d skipped=%v\n\n",
		result.TokenCount, result.DispatchResult.SuccessCount, result.DispatchResult.FailureCount, result.Skipped)

	fmt.Println("--- event 2: same EventID resubmitted, idempotency short-circuits it ---")
	result, err = svc.ProcessEvent(ctx, grnoti.Event{
		EventID:  "evt-1", // deliberately reused
		UserID:   userID,
		Type:     EventTypeOrderShipped,
		Priority: grnoti.PriorityHigh,
		Payload:  map[string]string{"order_id": "1001", "tracking_url": "https://example.com/t/1001"},
	})
	if err != nil {
		return fmt.Errorf("ProcessEvent: %w", err)
	}
	fmt.Printf("  result: skipped=%v reason=%q\n\n", result.Skipped, result.SkipReason)

	fmt.Println("--- event 3: anonymous visitor, no device token registered -> skipped, not an error ---")
	result, err = svc.ProcessEvent(ctx, grnoti.Event{
		EventID:     "evt-2",
		AnonymousID: "anon-session-999",
		Type:        EventTypeOrderShipped,
		Priority:    grnoti.PriorityNormal,
		Payload:     map[string]string{"order_id": "1002", "tracking_url": "https://example.com/t/1002"},
	})
	if err != nil {
		return fmt.Errorf("ProcessEvent: %w", err)
	}
	fmt.Printf("  result: skipped=%v reason=%q\n", result.Skipped, result.SkipReason)
	return nil
}

// Using real backends instead of the in-memory ones above is a one-line
// change at each construction site — every constructor lives in this same
// package:
//
//	TokenStore:        grnoti.NewMongoTokenStore(grnoti.MongoTokenStoreConfig{URI: ..., Database: ...})
//	                    // or grnoti.NewPostgresTokenStore(grnoti.PostgresConfig{DSN: ...})
//	PreferencesStore:   grnoti.NewPostgresPreferencesStore(grnoti.PostgresConfig{DSN: ...})
//	IdempotencyStore:   grnoti.NewCacheIdempotencyStore(<any grcache.Cache, e.g. grcache/redis>)
//	DLQHandler:         grnoti.NewPostgresDLQHandler(grnoti.PostgresDLQHandlerConfig{...})
//	                    // or grnoti.NewMongoDLQHandler(grnoti.MongoDLQHandlerConfig{...})
//	Dispatcher:         grnoti.NewFCMDispatcher(grnoti.FCMDispatcherDeps{Client: <*messaging.Client>})
//	RateLimiter:        grnoti.NewRedisRateLimiter(grnoti.RedisRateLimiterConfig{...}) — wire into
//	                    FCMDispatcherDeps.RateLimiter, not ServiceDeps
//
// Wiring multiple Postgres stores (TokenStore, PreferencesStore,
// ExperimentStore, DLQHandler) together in one backend? Build one
// *pgxpool.Pool yourself and inject it via PostgresConfig.Pool into each
// store instead of giving each one its own DSN — see docs/postgres.md
// for the full pattern, the Close() ownership rules, and how schema
// application (PostgresConfig.SkipSchemaEnsure) behaves when sharing a
// pool.
//
// See CLAUDE.md for the docker run commands each backend's own tests use,
// and docs/architecture.md for why the package is structured this way.
