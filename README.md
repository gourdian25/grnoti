# grnoti

[![Go Reference](https://pkg.go.dev/badge/github.com/gourdian25/grnoti.svg)](https://pkg.go.dev/github.com/gourdian25/grnoti)
[![Go Version](https://img.shields.io/badge/go-1.26.4+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Push-notification service library for the gourdian ecosystem
(`github.com/gourdian25/grnoti`): FCM dispatch, idempotent event
processing, device-token management, durable dead-letter retry, circuit
breaking, distributed rate limiting, deterministic A/B experiment
assignment, localization, and topic-based routing, behind a set of
storage-agnostic interfaces.

Status: feature-complete per the 14-stage build plan
([docs/plan/grnoti-plan.md](docs/plan/grnoti-plan.md)), pre-tagged-release.
`golangci-lint run` reports 0 issues; test coverage is 95.1% on the root
package, enforced by a 95% gate (`make coverage-check`), verified against
real local MongoDB/PostgreSQL/Redis/Kafka instances (see
[CLAUDE.md](CLAUDE.md) for the docker setup).

## Table of Contents

- [Part of the gourdian25 ecosystem](#part-of-the-gourdian25-ecosystem)
- [Install](#install)
- [Dependencies](#dependencies)
- [Quickstart](#quickstart)
- [An intermediate example: DLQ, circuit breaker, rate limiting](#an-intermediate-example-dlq-circuit-breaker-rate-limiting)
- [Configuration](#configuration)
- [Public API overview](#public-api-overview)
- [Postgres: sharing one pool across stores](#postgres-sharing-one-pool-across-stores)
- [Why storage-agnostic interfaces](#why-storage-agnostic-interfaces)
- [Why this shape](#why-this-shape)
- [Testing](#testing)
- [Error handling](#error-handling)
- [Limitations / out of scope](#limitations--out-of-scope)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Part of the gourdian25 ecosystem

grnoti is one of several small, independent Go libraries meant to be used
together:

- [grcache](https://github.com/gourdian25/grcache) — backend-agnostic
  caching abstraction; grnoti's `NewCacheIdempotencyStore`,
  `NewCachedPreferencesStore`, and `NewCacheBackedExperimentEngine` each
  wrap any `grcache.Cache` directly, no adapter needed.
- [grevents](https://github.com/gourdian25/grevents) — an in-process event
  bus; grnoti optionally publishes `notification.sent`/
  `notification.failed`/`experiment.assigned` lifecycle events through it —
  best-effort, so a nil bus or a publish failure never affects the durable
  operation it follows.
- [gourdiantoken](https://github.com/gourdian25/gourdiantoken) — JWT
  access/refresh token issuance, verification, revocation, and rotation.
- [grlog](https://github.com/gourdian25/grlog) — zero-dependency structured
  logging.
- [graudit](https://github.com/gourdian25/graudit) — an append-only,
  tamper-evident audit log with pluggable storage backends.
- [grpolicy](https://github.com/gourdian25/grpolicy) — attribute-based
  policy evaluation (RBAC/ABAC), independent of any notion of "user" or
  "role".

## Install

```sh
go get github.com/gourdian25/grnoti
```

## Dependencies

grnoti is a single flat package with no subpackages of its own (see
[Why this shape](#why-this-shape)) — every backend lives in the same
module, distinguished by a `<concern>.<backend>.go` file-naming
convention. The direct consequence: **`go get
github.com/gourdian25/grnoti` pulls in every backend driver, regardless of
which ones a given deployment actually uses** — the MongoDB driver,
pgx/v5 (plus sqlc-generated query code), go-redis, IBM's Sarama (Kafka),
and the Firebase Admin SDK. This is a deliberate divergence from sibling
repos like `grcache`/`graudit`, which use one subpackage per backend
specifically to keep unused drivers out of a consumer's dependency graph.
grnoti accepts that heavier import in exchange for a simpler package to
navigate (one `import "github.com/gourdian25/grnoti"`, not one per
backend). If a heavy transitive dependency graph is a concern for your
deployment, that tradeoff is worth knowing about up front — it isn't
something `go mod tidy` or build tags can undo here.

## Quickstart

```go
templates := grnoti.NewTemplateEngine()
templates.RegisterTemplate("order_shipped", grnoti.MessageTemplate{
    TitleTemplate: "Your order has shipped!",
    BodyTemplate:  "Order #{{.order_id}} is on its way.",
})

tokenStore, err := grnoti.NewMongoTokenStore(grnoti.MongoTokenStoreConfig{URI: mongoURI, Database: "myapp"})
if err != nil {
    log.Fatal(err)
}
dispatcher, err := grnoti.NewFCMDispatcher(grnoti.FCMDispatcherDeps{Client: fcmClient})
if err != nil {
    log.Fatal(err)
}

svc, err := grnoti.NewNotificationService(grnoti.ServiceDeps{
    TokenStore:  tokenStore,
    Dispatcher:  dispatcher,
    Templates:   templates,
    Idempotency: grnoti.NewCacheIdempotencyStore(redisCache), // any grcache.Cache
    Config:      grnoti.DefaultServiceConfig(),
})
if err != nil {
    log.Fatal(err)
}
defer svc.Close()

_, err = svc.ProcessEvent(ctx, grnoti.Event{
    EventID:  "evt-1",
    UserID:   "user-42",
    Type:     "order_shipped",
    Priority: grnoti.PriorityHigh,
    Payload:  map[string]string{"order_id": "1001"},
})
```

See [example/main.go](example/main.go) for a complete, runnable,
narrated walkthrough — `go run ./example`, no external services required
(it uses the in-memory backends plus a dispatcher that logs to stdout
instead of calling FCM). It also documents the exact one-line swap for
every real backend constructor.

## An intermediate example: DLQ, circuit breaker, rate limiting

The Quickstart above only wires the four *required* `ServiceDeps` fields
(`TokenStore`, `Dispatcher`, `Templates`, `Idempotency`). A more realistic
production wiring also protects the FCM dispatch path itself and gives
failed sends somewhere durable to land. Building on
[example/main.go](example/main.go)'s own structure:

```go
// A circuit breaker: after 5 consecutive FCM failures, stop calling FCM
// for 30s and fail fast instead; a closed breaker's failure counter
// resets after 1 minute with no failures.
breaker, err := grnoti.NewCircuitBreaker(5, 30*time.Second, time.Minute)
if err != nil {
    log.Fatal(err)
}

// A local (per-process) rate limiter: at most 50 FCM calls/sec, bursts
// of up to 10. Swap for grnoti.NewRedisRateLimiter(...) to share one
// limit across multiple service instances instead.
limiter, err := grnoti.NewLocalRateLimiter(50, 10)
if err != nil {
    log.Fatal(err)
}

// RateLimiter/CircuitBreaker, once set here, actually gate every
// outbound FCM batch/single-send in dispatcher.fcm.go — not just built
// and left unconnected (docs/architecture.md §3.2).
dispatcher, err := grnoti.NewFCMDispatcher(grnoti.FCMDispatcherDeps{
    Client:         fcmClient,
    Config:         grnoti.DefaultFCMDispatcherConfig(),
    RateLimiter:    limiter,
    CircuitBreaker: breaker,
})
if err != nil {
    log.Fatal(err)
}

// A durable dead-letter queue: any dispatch failure not already
// accounted for by a marked-invalid token is published here instead of
// silently dropped. NewMemoryDLQHandler(maxRetries, retryDelay,
// maxRetryDelay) shown here; swap for NewPostgresDLQHandler/
// NewMongoDLQHandler in production for a restart-durable queue.
dlqHandler := grnoti.NewMemoryDLQHandler(3, time.Minute, 10*time.Minute)

config := grnoti.DefaultServiceConfig() // EnableDLQ is already true by default
svc, err := grnoti.NewNotificationService(grnoti.ServiceDeps{
    TokenStore:  tokenStore,
    Dispatcher:  dispatcher,
    Templates:   templates,
    Idempotency: idempotencyStore,
    DLQHandler:  dlqHandler,
    Config:      config,
})
if err != nil {
    log.Fatal(err)
}
defer svc.Close()
```

`NotificationService` never calls `DLQHandler.ClaimRetryableEvents`
itself — it only ever calls `PublishToDLQ`. Draining the queue is a
separate, external retry-worker process's job, polling periodically:

```go
events, err := dlqHandler.ClaimRetryableEvents(ctx, 50) // atomically claims up to 50
// events may be non-nil even when err != nil for some backends (e.g.
// Mongo) — process what was returned regardless; see
// DLQHandler.ClaimRetryableEvents' doc comment (interfaces.go) and
// docs/architecture.md §3.6.
for _, ev := range events {
    // re-attempt delivery, then report the outcome:
    _ = dlqHandler.MarkRetried(ctx, ev.EventID, success, attemptErr)
}
```

## Configuration

### `NewNotificationService(ServiceDeps) (NotificationService, error)`

| `ServiceDeps` field | Required? | Notes |
|---|:---:|---|
| `TokenStore` | **required** | device-token lookup |
| `Dispatcher` | **required** | FCM send path |
| `Templates` | **required** | `Event` → `Message` rendering |
| `Idempotency` | **required** | duplicate-delivery suppression |
| `PreferencesFilter` | optional | gates authenticated dispatch on `ShouldSendNotification`; only consulted when `Config.EnablePreferencesFilter` is also set — nil-safe either way |
| `TopicRouter` | optional | resolves each event's `NotificationTarget` instead of the default direct-token resolution; only consulted when `Config.EnableTopicRouting` is also set |
| `DLQHandler` | optional | receives unresolved dispatch failures; only consulted when `Config.EnableDLQ` is also set |
| `EventBus` (`grevents.Bus`) | optional | receives lifecycle events; only consulted when `Config.EnableEventBus` is also set — publishing is always best-effort |
| `Metrics` | optional | per-event/per-platform counters and latency observations |
| `WorkerPoolConfig` | optional | only used when `Config.EnableBackpressure` is set, to build the service's own internal `*WorkerPool` |
| `Logger` | optional | nil-safe, defaults to a no-op logger |
| `Config` (`ServiceConfig`) | optional | see table below; zero value is "everything opt-in off" except where `DefaultServiceConfig()` says otherwise |

Missing any of the four required fields makes `NewNotificationService`
return an error immediately — every other field is nil-safe and simply
disables the behavior it would have enabled.

### `ServiceConfig` (via `DefaultServiceConfig()`)

| Field | Default | Effect |
|---|---|---|
| `IdempotencyTTL` | `24 * time.Hour` | How long a processed event's ID is remembered by `IdempotencyStore` before it could be reprocessed if redelivered again |
| `MaxTokensPerBatch` | `500` (FCM's own per-multicast-call limit) | Batch size `dispatchToTokens` splits a resolved token list into when `EnforceBatching` is set |
| `EnableMetrics` | `true` | Gates whether `processEvent` calls the configured `Metrics` collector at all |
| `SkipInvalidEvents` | `false` | A failed `Event.Validate()` is reported as a skip (`ProcessingResult.Skipped`) instead of returned as an error |
| `EnableTokenDeduplication` | `false` | Deduplicates a resolved token list (by `Token` value) before dispatch |
| `EnforceBatching` | `false` | Pre-splits a resolved token list into `MaxTokensPerBatch`-sized dispatcher calls, merging the per-batch results — a coarser, caller-controlled layer that composes with (doesn't replace) the dispatcher's own fixed internal FCM batching |
| `EnablePreferencesFilter` | `false` | Consults `ServiceDeps.PreferencesFilter` for authenticated events; no-op if `PreferencesFilter` is nil regardless of this flag |
| `EnableTopicRouting` | `false` | Consults `ServiceDeps.TopicRouter` to resolve a topic/token target instead of always resolving tokens directly; no-op if `TopicRouter` is nil regardless of this flag |
| `EnableRichPush` | `false` | **Documentation-only.** Not read anywhere in `processEvent` — rich-push fields live on `Message` and are populated by whichever `TemplateEngine` is wired in, independent of this flag |
| `EnableLocalization` | `false` | **Documentation-only.** Localization is a `TemplateEngine` decorator (`NewLocalizedTemplateEngine`) the caller wraps `ServiceDeps.Templates` in — this flag doesn't gate anything itself |
| `EnableABTesting` | `false` | **Documentation-only.** A/B variant assignment (`ExperimentEngine.AssignVariant`) happens before an `Event` is even constructed, to decide what goes into its `Payload` — this flag doesn't gate anything itself |
| `EnableBackpressure` | `false` | Routes `Submit` through the service's own internal, bounded `*WorkerPool` (non-blocking, `ErrWorkerPoolFull` on a full queue) instead of processing on the caller's goroutine |
| `EnableDLQ` | `true` | Publishes unresolved dispatch failures to the configured `DLQHandler`; no-op if `DLQHandler` is nil regardless of this flag |
| `EnableEventBus` | `false` | Publishes `notification.sent`/`notification.failed` lifecycle events via the configured `EventBus`; no-op if `EventBus` is nil regardless of this flag |

`EnableRichPush`, `EnableLocalization`, and `EnableABTesting` are real
fields on the struct and are safe to set, but as of the current code
they are **composition-time bookkeeping only** — nothing in
`notificationService.processEvent` (`service.go`) branches on them. What
they'd describe is decided entirely by which concrete `TemplateEngine`/
`ExperimentEngine` implementation you compose into `ServiceDeps` before
constructing the service, not by anything the pipeline itself can gate
on at request time. Don't rely on flipping one of these three to change
runtime behavior.

## Public API overview

The tables below are constructor-oriented; see
[docs/architecture.md](docs/architecture.md) §2 for the full
interface-by-backend matrix (with checkmarks) and the reasoning behind
each implementation choice.

**TokenStore**

| Backend | Constructor |
|---|---|
| In-memory | `NewMemoryTokenStore()` |
| MongoDB | `NewMongoTokenStore(MongoTokenStoreConfig)` |
| PostgreSQL | `NewPostgresTokenStore(PostgresConfig)` |

**PreferencesStore / PreferencesFilter**

| Variant | Constructor |
|---|---|
| In-memory store | `NewMemoryPreferencesStore()` |
| PostgreSQL store | `NewPostgresPreferencesStore(PostgresConfig)` |
| Cache-backed read-through (wraps any `PreferencesStore`) | `NewCachedPreferencesStore(store, grcache.Cache, ttl, Logger)` |
| Filter (consumes a `PreferencesStore`, used via `ServiceDeps.PreferencesFilter`) | `NewPreferencesFilter(store, Logger)` |

**DLQHandler**

| Backend | Constructor |
|---|---|
| In-memory | `NewMemoryDLQHandler(maxRetries, retryDelay, maxRetryDelay)` |
| MongoDB | `NewMongoDLQHandler(MongoDLQHandlerConfig)` |
| PostgreSQL | `NewPostgresDLQHandler(PostgresDLQHandlerConfig)` |

**ExperimentStore / ExperimentEngine**

| Variant | Constructor |
|---|---|
| Store: in-memory | `NewMemoryExperimentStore()` |
| Store: PostgreSQL | `NewPostgresExperimentStore(PostgresConfig)` |
| Engine: in-process assignment cache | `NewDeterministicExperimentEngine(AnalyticsPublisher, grevents.Bus, Logger)` |
| Engine: shared, cache-backed assignment | `NewCacheBackedExperimentEngine(grcache.Cache, AnalyticsPublisher, grevents.Bus, Logger)` |

Assignment itself (`deterministicPick`) is a pure, deterministic
function of `(userID, experiment.ID, experiment.Variants)` in both
engines — the cache is purely an optimization for where repeated
assignments are remembered, never a correctness dependency.

**IdempotencyStore**

| Backend | Constructor |
|---|---|
| Any `grcache.Cache` (Redis, in-memory, ...) | `NewCacheIdempotencyStore(grcache.Cache)` |

**PushDispatcher (FCM)**

| Backend | Constructor |
|---|---|
| Firebase Cloud Messaging | `NewFCMDispatcher(FCMDispatcherDeps)` — tune retry via `FCMDispatcherConfig`/`DefaultFCMDispatcherConfig()` (`EnableRetry`, `MaxRetryAttempts`, `RetryBaseDelay`/`RetryMaxDelay`) |

**RateLimiter**

| Variant | Constructor |
|---|---|
| Local (per-process) | `NewLocalRateLimiter(requestsPerSecond, burstSize int)` |
| Redis (distributed) | `NewRedisRateLimiter(RedisRateLimiterConfig)` |

**CircuitBreaker**

| Variant | Constructor |
|---|---|
| The one implementation (`standardCircuitBreaker`), returned as the `CircuitBreaker` interface | `NewCircuitBreaker(maxFailures, timeout, resetTimeout)` or `NewCircuitBreakerWithConfig(CircuitBreakerConfig)` |

**Kafka**

| Role | Constructor |
|---|---|
| `EventConsumer` | `NewKafkaEventConsumer(KafkaConsumerConfig)` |
| `AnalyticsPublisher` | `NewKafkaAnalyticsPublisher(KafkaAnalyticsPublisherConfig)` |

**Templates, localization, routing, misc.**

| Capability | Constructor |
|---|---|
| `TemplateEngine` (scheme-free defaults) | `NewTemplateEngine()` |
| `TemplateEngine` (locale-aware decorator) | `NewLocalizedTemplateEngine(base, LocalizationStore, LocaleResolver)` |
| `LocalizationStore` | `NewInMemoryLocalizationStore()` |
| `LocaleResolver` (reads a user's stored locale) | `NewPreferencesLocaleResolver(store, fallbackLocale)` |
| `LocaleResolver` (fixed locale) | `NewStaticLocaleResolver(locale)` |
| `TopicRouter` (by event type) | `NewEventTypeTopicRouter(topicMappings, TokenStore, Logger)` |
| `TopicRouter` (fixed topic) | `NewStaticTopicRouter(topic)` |
| `TopicRouter` (tokens only, no topic routing) | `NewTokenOnlyRouter(TokenStore)` |
| `BatchSplitter` | `NewBatchSplitter()` |
| `RetryStrategy` (full-jitter backoff) | `NewFullJitterRetry(maxAttempts, baseDelay, maxDelay)` |
| `RetryStrategy` (never retries) | `NewNoopRetryStrategy()` |
| `PayloadValidator` (FCM payload-size check) | `NewFCMPayloadValidator()` |
| `*WorkerPool` (used internally when `Config.EnableBackpressure` is set) | `NewWorkerPool(WorkerPoolDeps)` |

## Postgres: sharing one pool across stores

`NewPostgresTokenStore`, `NewPostgresPreferencesStore`,
`NewPostgresExperimentStore`, and `NewPostgresDLQHandler` each take a
`PostgresConfig`. Giving each one its own `DSN` means each dials its own
pool — fine for one store, wasteful for four (`MaxConns` × 4 connections,
not × 1). `PostgresConfig.Pool` lets you build one `*pgxpool.Pool`
yourself and inject it into every store instead:

```go
pool, err := pgxpool.NewWithConfig(ctx, poolCfg) // your own bootstrap code
if err != nil {
    log.Fatal(err)
}
defer pool.Close() // grnoti never closes a Pool it didn't dial itself

tokenStore, err := grnoti.NewPostgresTokenStore(grnoti.PostgresConfig{Pool: pool})
preferencesStore, err := grnoti.NewPostgresPreferencesStore(grnoti.PostgresConfig{
    Pool: pool, SkipSchemaEnsure: true, // schema already applied by tokenStore above
})
```

`DSN` and `Pool` are mutually exclusive — set exactly one.
`PostgresConfig.SkipSchemaEnsure` skips grnoti's built-in schema
application for stores managed by your own migration pipeline instead.
See [docs/postgres.md](docs/postgres.md) for the full pattern, `Close()`
ownership rules, and the concurrency-safety guarantee (schema application
is now serialized via a Postgres advisory lock).

## Why storage-agnostic interfaces

Every capability — token storage, preferences, dead-letter retry, rate
limiting, experiment assignment — is a small interface with real,
independently-usable implementations: in-memory (for tests or small
deployments), MongoDB, PostgreSQL, or a `grcache.Cache`-backed adapter
(works with any of `grcache`'s own backends, including Redis). Pick
whichever combination matches your infrastructure; nothing in
`NotificationService` assumes a specific one. See
[docs/architecture.md](docs/architecture.md) for the full interface/backend
matrix and the reasoning behind each design decision.

## Why this shape

grnoti was the first repo in the gourdian25 ecosystem to use this shape —
a single flat package with pgx/v5 + sqlc-generated Postgres queries and no
GORM, ever — rather than adopting it from a sibling repo. grcache, graudit,
and gourdiantoken later converged on the same flat-package, GORM-free
pattern during their own standardization passes, so grnoti's shape became
the template the rest of the ecosystem adopted, not the other way around.
See [docs.go](docs.go)'s "Package shape" section for the full reasoning.

## Testing

```sh
make test              # go test -count=1 -timeout=5m -cover ./...
make race               # go test -race -timeout 5m ./... — mandatory before any commit
                         # touching experiment.go, workerpool.go, dlq.*.go, or any store
make coverage-summary    # per-function coverage breakdown
make coverage-check      # gates the root package at 95%
make precommit           # fmt + vet + lint + race + coverage-check — run before every commit
```

**Coverage scoping caveat**: use `go test -cover .` (a single dot), not
`go test -cover ./...`. The root package's own coverage is what
`coverage-check` gates on and reports (95.1% at last check), but running
against `./...` also compiles and instruments `internal/postgresdb` (sqlc-
generated query wrappers with no test file of their own, so it always
reports a flat 0%) and the `example` command package (also untested by
design — it's a runnable demo, not library code) — both dilute the
printed number without reflecting anything about this package's actual
test coverage. `make coverage-check` already scopes to `.` for this
reason; `internal/postgresdb`'s 0% line in `go tool cover -func` output
can be ignored — see [docs/architecture.md](docs/architecture.md) §5.

**Real local backends, not mocks**: every backend (MongoDB, PostgreSQL,
Redis, Kafka) is tested against a real local instance via Docker, not a
mock — this is a genuine differentiator, and it has caught real bugs
mocked tests would have missed (see docs/architecture.md §4 for three
concrete examples, including a confirmed data race and a silently-dropped
total-request-failure retry path). **FCM is the one deliberate
exception**: it has no local emulator to test against, so
`dispatcher.fcm_test.go` uses a hand-rolled fake `FCMClient` instead.
Tests skip gracefully (`t.Skip`, not fail) when a backend isn't running
locally, so `go test .` still works with no containers up — but the tests
that matter for a given change need the real thing. See
[CLAUDE.md](CLAUDE.md) for the exact `docker run` commands (or `make
docker-up`/`make docker-down` to start/stop them all at once) — these
containers are shared across the whole gourdian25 workspace (grnoti,
graudit, grcache, gourdiantoken all test against the same running
instances, each using its own database/keyspace/DB-index).

**Benchmarks**: `make bench` (`go test -bench=. -benchmem -benchtime=10s
./...`) is a defined Makefile target, but there are currently no
`func Benchmark*` functions anywhere in this repo — running it executes
the regular test suite and prints no benchmark lines. Treat it as a
target reserved for future use, not a populated benchmark suite today.

## Error handling

Sentinel errors are used with `errors.Is`, never a per-error `IsX(err)
bool` helper, matching every other gourdian repo. A backend-native error
(`mongo.ErrNoDocuments`, `pgx.ErrNoRows`, `redis.Nil`, ...) is always
translated to one of these before it can leak through a grnoti interface.

The 16 sentinels declared in [errors.go](errors.go):

| Error | Meaning |
|---|---|
| `ErrClosed` | A method was called after `Close` |
| `ErrBackendUnavailable` | A storage backend could not be reached |
| `ErrInvalidEventID` | `Event.EventID` was empty |
| `ErrNoTargetSpecified` | An `Event` has none of `UserID`, `AnonymousID`, or `DeviceTokens` set |
| `ErrInvalidEventType` | `Event.Type` failed `EventType.IsValid()` |
| `ErrInvalidPriority` | `Event.Priority` failed `Priority.IsValid()` |
| `ErrTemplateNotFound` | No `MessageTemplate` registered for an event type, and no `EventTypeCustom` fallback either |
| `ErrPreferencesNotFound` | No `NotificationPreferences` exist yet for a user (callers generally treat this as "use defaults") |
| `ErrPreferencesUserIDRequired` | `SavePreferences` was called with an empty `UserID` |
| `ErrExperimentNotFound` | No `Experiment` registered under the requested ID |
| `ErrExperimentAlreadyExists` | `CreateExperiment` was called with an ID that already exists |
| `ErrExperimentHasNoVariants` | An `Experiment` exists but has zero `ExperimentVariant`s |
| `ErrDLQEventNotFound` | No `DLQEvent` found for the requested event ID |
| `ErrDLQEventNotClaimed` | `MarkRetried` was called for an event not currently in the claimed (retrying) state |
| `ErrFCMClientNil` | A `PushDispatcher` was constructed with a nil FCM client |
| `ErrFCMPayloadTooLarge` | A `Message`'s estimated FCM payload size exceeds `FCMMaxPayloadSize` |

Three more sentinels live alongside the feature they belong to rather
than in `errors.go`, but follow the identical `errors.Is` convention:
`ErrCircuitOpen`/`ErrTooManyRequests` (`circuitbreaker.go`) and
`ErrWorkerPoolFull` (`workerpool.go`).

FCM failures additionally get a structured `*FCMError` (`Code`, `Token`,
`Message`, wrapped `Err`), classified into a small `FCMErrorCode` enum
(`unspecified`, `invalid_argument`, `unregistered`, `sender_id_mismatch`,
`quota_exceeded`, `unavailable`, `internal`, `third_party_auth_error`)
with `IsRetryable()`/`IsPermanent()` methods that drive real retry and
invalid-token decisions in `dispatcher.fcm.go` — not just informational
classification.

## Limitations / out of scope

- **FCM has no local test emulator.** Unlike Mongo/Postgres/Redis/Kafka,
  which are all tested against real local instances, FCM is tested with a
  hand-rolled fake `FCMClient` (`dispatcher.fcm_test.go`) — there is no
  equivalent "run it locally" option for Firebase Cloud Messaging.
- **`CircuitBreaker` and `WorkerPool` state is per-process, deliberately
  not centralized.** Each service instance's breaker/queue is independent
  in-memory state — centralizing it across replicas was considered and
  rejected, since it risks a synchronized thundering-herd retry against
  FCM the moment it recovers, which is arguably worse than each instance
  recovering independently.
- **`grevents` publishing is always best-effort.** A nil `EventBus` or a
  publish failure never affects the durable operation
  (dispatch/DLQ/idempotency) it follows — lifecycle events are an
  observability nicety, not part of the delivery guarantee.
- **`DeviceToken` is a bearer credential grnoti does not verify ownership
  of.** grnoti does not check that a given token actually belongs to the
  `UserID`/`AnonymousID` it's registered under — that binding is entirely
  the caller's responsibility at `TokenStore.SaveToken` time.
- **No encryption at rest or in transit is provided by grnoti itself.**
  TLS to Mongo/Postgres/Redis/Kafka, and encrypting any sensitive
  `Event.Payload`/`Message.Data` values before they reach grnoti, are the
  caller's responsibility.
- **Templates are not sanitized against injection.** `text/template`
  (not `html/template`) renders `Event.Payload` values directly — a
  caller feeding untrusted input into a payload value is responsible for
  sanitizing it first.
- **Idempotency and DLQ keys are trusted, not authenticated.** Any caller
  holding an `IdempotencyStore`/`DLQHandler` handle can mark arbitrary
  event IDs processed or replay dead-lettered events; there is no
  per-caller access control over these operations.
- **FCM credentials are never handled by grnoti.** The `PushDispatcher`'s
  FCM client is constructed and authenticated by the caller via the
  official Firebase Admin SDK — key management stays outside this
  library's scope.
- **Postgres schema management is additive only.** Every `New*Postgres*`
  constructor applies `CREATE TABLE/INDEX IF NOT EXISTS` on connect; there
  is no down-migration, no versioning, and no support for evolving the
  schema beyond that. An `ALTER TABLE`, column type change, or backfill is
  entirely your own migration tool's job — set
  `PostgresConfig.SkipSchemaEnsure: true` once you own the schema that
  way. See [docs/postgres.md](docs/postgres.md).

See [SECURITY.md](SECURITY.md) for the complete scope-notes list this
section draws from.

## Development

```sh
make docker-up   # start the shared Postgres/Redis/Mongo/Kafka test containers
make precommit   # fmt + vet + lint + race + coverage-check
make docker-down # stop them when you're done
```

These containers are shared with graudit, grcache, and gourdiantoken (each
gets its own database/keyspace) — see [CLAUDE.md](CLAUDE.md) for backend
setup, test scoping, and conventions.

## Contributing

Issues and PRs are welcome at
[github.com/gourdian25/grnoti](https://github.com/gourdian25/grnoti).
Please run `make precommit` (fmt + vet + lint + race + coverage-check)
before submitting.

## License

MIT — see [LICENSE](LICENSE).
