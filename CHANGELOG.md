# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Core contracts: `Event`, `Message`, `DeviceToken`, `DispatchResult`,
  `ProcessingResult`, and the full set of storage-agnostic interfaces
  (`TokenStore`, `PreferencesStore`, `DLQHandler`, `ExperimentStore`,
  `ExperimentEngine`, `IdempotencyStore`, `RateLimiter`, `PushDispatcher`,
  `TemplateEngine`, `TopicRouter`, `CircuitBreaker`, `EventConsumer`,
  `AnalyticsPublisher`).
- Pure in-process logic: `CircuitBreaker`, `RetryStrategy` (Full-Jitter
  backoff), `BatchSplitter`, `PayloadValidator`, `PreferencesFilter`
  (global/per-type opt-out + quiet hours).
- `memory.go`: in-memory `TokenStore`/`PreferencesStore`/`DLQHandler`/
  `ExperimentStore`, for tests and small deployments.
- `grcache`-backed adapters: `IdempotencyStore`, a read-through
  `PreferencesStore` cache, and an `ExperimentEngine` assignment cache —
  work with any `grcache.Cache` backend.
- MongoDB backends: `TokenStore`, `DLQHandler` (atomic per-document claim
  via `FindOneAndUpdate`).
- PostgreSQL backends (pgx/v5 + sqlc): `TokenStore`, `PreferencesStore`,
  `ExperimentStore`, `DLQHandler` (atomic claim via `SELECT ... FOR UPDATE
  SKIP LOCKED`).
- Cross-backend contract tests: every `TokenStore`/`PreferencesStore`/
  `ExperimentStore`/`DLQHandler` implementation runs the same behavioral
  contract suite.
- Redis-backed distributed `RateLimiter` (Lua token-bucket script for
  atomic refill+consume across replicas).
- Kafka `EventConsumer` (consumer-group, at-least-once redelivery) and
  `AnalyticsPublisher` (experiment impression/conversion events).
- `NewFCMDispatcher`: FCM push dispatch with platform-grouped fan-out,
  per-batch retry, and `RateLimiter`/`CircuitBreaker` actually wired into
  the send path.
- `grevents` integration (`events.go`): best-effort, nil-safe
  `notification.sent`/`notification.failed`/`experiment.assigned`
  lifecycle events.
- `NewNotificationService`: the orchestrator tying every interface above
  together — idempotency, preferences filtering, template rendering,
  topic/token target resolution, dispatch, DLQ publication, metrics, and
  lifecycle events, with `Submit` as a direct `EventConsumer` handler.
- `example/main.go`: a complete, runnable, dependency-free walkthrough
  (`go run ./example`).
- `docs/architecture.md`: interface/backend matrix and the reasoning
  behind every major design decision, including where and why this
  package diverges from its reference implementation.
- `CLAUDE.md`: backend setup, test scoping, and repo conventions.
- `make precommit`/`make prerelease` targets.

### Fixed

Found via this repo's real-local-services testing policy (Docker
containers, not mocks) rather than by code review alone — see
`docs/plan/grnoti-plan.md`'s §11 implementation log for the full account
of each:

- `dispatcher.fcm.go`'s retry loop silently abandoned a total
  request-level failure after one attempt, only ever retrying a partial
  per-token failure.
- `eventTypeTopicRouter`/`tokenOnlyRouter` silently dropped anonymous and
  direct-token events when topic routing was enabled (their fallback
  unconditionally looked up tokens by `UserID`).
- `ratelimiter.redis.go`'s `UpdateLimit` was the one method missing the
  closed-store guard every sibling method has.
- `templateengine.go` silently swallowed a `DeepLink`/action-URL render
  error and shipped the literal, unrendered template text instead —
  `DeepLink`/action URLs are now validated at template-registration time,
  matching title/body.
- A repository-tooling regression (not a grnoti bug): the `bark` header
  tool twice overwrote sqlc's generated-file marker on
  `internal/postgresdb`, which every Go linter relies on to skip
  generated code — fixed at the root via `.bark.toml`'s exclude list.
- `workerpool.go`'s `Stop()` called `cancel()` before `close(queue)`,
  letting each worker's `select` pseudo-randomly exit via the
  now-ready `ctx.Done()` case instead of draining the buffered queue —
  silently losing already-accepted, not-yet-processed events on every
  graceful shutdown. Found via a targeted concurrency audit
  (`go test -race -run TestWorkerPool_StopDrains -count=200`), not the
  real-local-services policy above; `Stop()` now closes the queue and
  waits for full drain before canceling.
- `localization.go`'s `localizedTemplateEngine.BuildMessage` re-parsed the
  localized `MessageTemplate`'s title/body `text/template` sources on
  every call instead of once, unlike `defaultTemplateEngine`, which
  compiles at `RegisterTemplate` time — `LocalizationStore` has no
  registration hook this engine can intercept, so `BuildMessage` now
  caches each compiled template (keyed by event type + locale) and
  validates cache hits against the freshly-fetched `MessageTemplate` via
  `reflect.DeepEqual` before reuse, so a template updated via
  `RegisterLocalizedTemplate` after being cached is still picked up on the
  next call rather than serving stale content.
- `preferences.postgres.go`/`memory.go`'s `PreferencesStore.SavePreferences`
  returned `ErrNoTargetSpecified` — a sentinel documented as meaning "an
  `Event` has no resolvable recipient" — for an unrelated empty-`UserID`
  validation; now returns a dedicated `ErrPreferencesUserIDRequired`.
- Four call sites checked `err == ErrPreferencesNotFound` via direct
  equality instead of `errors.Is`, contrary to this repo's own sentinel-
  error convention and `GetPreferences`'s doc comment — harmless only
  because no implementation currently wraps the error; fixed in
  `memory.go`, `preferences.postgres.go`, `preferencesfilter.go`, and
  `cache.preferences.go` so a future implementation is free to add
  `%w`-wrapped context without silently breaking preferences-defaulting
  logic.

### Repository scaffolding

- `go.mod`, lint/release config, `Makefile`, `LICENSE`, `SECURITY.md`.
