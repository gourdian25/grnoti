# Changelog

All notable changes to this project are documented in this file.

## [0.2.0] - 2026-08-14

**Breaking**, for the Postgres backend only. Mirrors the fix gourdiantoken
shipped in its own `v2.4.0` (see gourdiantoken's `CHANGELOG.md`): schema
auto-apply on every connect required `CREATE` on the target schema, which
a deliberately least-privilege application role (a common production
setup — a separate role owns migrations, the app connects with a
DML-only role) doesn't have, failing construction with `permission
denied for schema ...` — and this can't be worked around by pre-creating
the tables some other way, since `CREATE TABLE IF NOT EXISTS` still
checks `CREATE` privilege before checking whether the table exists, so
the attempt fails every time regardless. Applied here proactively, for
consistency across the gourdian25 ecosystem's Postgres-backed libraries,
ahead of grnoti's own Postgres stores getting a real consumer.

### Changed

- **No `New*Postgres*` constructor applies schema anymore.** Signatures
  of `NewPostgresTokenStore`, `NewPostgresPreferencesStore`,
  `NewPostgresExperimentStore`, and `NewPostgresDLQHandler` are
  unchanged; behavior is: construction now only pings the pool (the
  caller's own externally-supplied pool, in `Pool` mode). Call
  `SchemaSQL()` and apply the result through your own project's
  migration tool before constructing — see docs/postgres.md (rewritten
  for this change). Existing deployments that relied on the implicit
  auto-apply need to add that migration step; the schema itself is
  unchanged (still `CREATE TABLE/INDEX IF NOT EXISTS`, still the same
  four `grnoti_`-prefixed tables: `grnoti_tokens`, `grnoti_preferences`,
  `grnoti_experiments`, `grnoti_dlq`).
- **`PostgresConfig.SkipSchemaEnsure` removed.** Added in `v0.1.0` as a
  per-store opt-out from schema auto-apply; this release removes
  auto-apply itself instead, so the flag no longer has anything to opt
  out of. grnoti is pre-1.0, so this ships as a straight field removal
  rather than a deprecate-then-remove cycle.

### Added

- **`SchemaSQL() string`**, returning grnoti's Postgres schema as text —
  covering all four Postgres-backed stores' tables in one call, since
  they share a single `internal/postgresdb/schema.sql` — for applying
  through your own migration tool (see docs/postgres.md).

### Testing

- Every `newTestPostgresTokenStore`/`newTestPostgresPreferencesStore`/
  `newTestPostgresExperimentStore`/`newTestPostgresDLQHandler` test
  helper, plus every `contract_*_test.go` "Postgres" subtest, now applies
  schema explicitly via a new shared `ensureTestPostgresSchema` helper
  (`tokenstore.postgres_test.go`) before constructing, since construction
  itself no longer does. `postgres_test.go`'s
  `TestConnectPostgres_SharedPool` similarly now applies schema itself
  before asserting `connectPostgres`'s shared-pool passthrough behavior,
  rather than relying on `connectPostgres` to have applied it.

### Fixed

- `version.go` was still hardcoded to `v0.1.0` despite `v0.1.1` already
  being the actual most recent tag; corrected as part of this bump.

### Documentation

- `docs/postgres.md`'s schema-application section rewritten around
  `SchemaSQL()` and "why no auto-apply," dropping `SkipSchemaEnsure`
  entirely.
- `docs/architecture.md` §3.12, `README.md`, `CLAUDE.md`, and
  `example/main.go`'s "Using real backends instead" comment updated to
  match — every mention of schema auto-apply or `SkipSchemaEnsure`
  outside this file's own historical `[0.1.0]` entry now describes the
  current (never-auto-apply) behavior.

## [0.1.0] - 2026-07-23

### Added

- `CircuitBreakerConfig.Logger` field: the breaker now logs a Warn on
  opening and an Info on half-open/close transitions — previously it had
  no logging at all despite `logger.go`'s own doc comment already listing
  "circuit-breaker state transitions" as a logged concern.
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
- `PostgresConfig.Pool`: inject an already-built `*pgxpool.Pool` instead
  of `DSN`, so multiple Postgres stores can share one pool instead of
  each dialing its own — see `docs/postgres.md` for the pattern. Every
  store's `Close()` now only closes a pool it dialed itself, never one
  supplied via `Pool`. `PostgresConfig.SkipSchemaEnsure` opts a store out
  of schema application for teams managing it via their own migration
  pipeline; `PostgresConfig.ConnectTimeout` makes the previously-hardcoded
  10-second connect timeout configurable in `DSN` mode.
- `docs/postgres.md`: the shared-pool wiring pattern, `Close()` ownership
  rules, and schema-management guidance for using grnoti's Postgres
  stores in a real backend.
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

### Changed

- `Logger`'s three printf-style methods (`Infof`/`Warnf`/`Errorf(format
  string, args ...interface{})`) replaced with four `log/slog`-shaped
  methods (`Debug`/`Info`/`Warn`/`Error(msg string, args ...any)`), matching
  `*slog.Logger`'s own signatures exactly so any slog-based logger —
  including `*grlog.Logger` via `slog.New(grlog.NewSlogHandler(...))` —
  satisfies it with no adapter. Consistent with the same change landing
  across grcache/grevents/graudit/grpolicy/gourdiantoken in this pass. Real
  structured field values (previously flattened into printf format
  strings) now reach any structured-output logger intact. Since this repo
  has never tagged a release, this is not a breaking change to any shipped
  version. Several previously-Info call sites (routine per-event/per-token/
  per-attempt detail: FCM per-token sends, topic-routing decisions,
  "token not found (not an error)" cache misses) were reclassified to
  Debug; lifecycle and failure events are unchanged. `github.com/gourdian25/
  grlog` is now a test-only dependency (see `logger_grlog_test.go`),
  matching grcache/grevents/graudit/grpolicy's own compile-time proof that
  `*slog.Logger` (via `slog.New(grlog.NewSlogHandler(...))`) satisfies
  `grnoti.Logger` with no adapter.

Ecosystem-wide Stage 4 pass: grnoti was the last repo to adopt the
workspace's standardized Docker test infrastructure (Postgres/Redis/Mongo/
Kafka shared containers, one database/keyspace/DB-index per repo).

- The Mongo backend's tests (and this repo's documented connection
  settings) now use the workspace-standard **authenticated** single-node
  replica set (`root`/`mongo_password` on port `27018`, `directConnection=
  true`), replacing the previous no-auth standalone connection string —
  matching graudit's, grcache's, and gourdiantoken's own test setup.
  `MongoTokenStoreConfig`/`MongoDLQHandlerConfig` already accepted an
  arbitrary URI, so no production code changed — only `testMongoURI` in
  `tokenstore.mongo_test.go` (shared by every Mongo-backed test file via
  the same package).
- The Redis rate-limiter's and Redis-backed cache's tests now authenticate
  with the workspace-standard password (`redis_password`), replacing the
  previous no-auth connection. `RedisRateLimiterConfig`/`grcache.RedisConfig`
  already had a `Password` field, so this is a test-constant change only
  (`testRedisPassword` in `ratelimiter.redis_test.go`).
- Realigned `cache_test.go`/`cache.redis_test.go`/`service_test.go`/
  `example/main.go` with `grcache` v0.2.0's own flattened package shape
  (`grcache.NewMemoryCache`/`grcache.NewRedisCache` instead of the
  now-removed `grcache/memory`/`grcache/redis` subpackages).
- `make coverage-check`'s threshold raised from 90% to 95%, matching
  every other repo in the ecosystem's own coverage bar.

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
- `connectPostgres`'s schema application (`CREATE TABLE/INDEX IF NOT
  EXISTS` against `internal/postgresdb/schema.sql`) raced under
  concurrent connects — multiple stores constructed from goroutines, or
  multiple service replicas booting simultaneously against a fresh
  database, could hit a duplicate-catalog-entry error, since Postgres's
  `IF NOT EXISTS` DDL isn't fully race-free across concurrent sessions.
  `applyPostgresSchema` now serializes it behind a Postgres advisory lock.
  Separately, `internal/postgresdb/schema.sql`'s header comment pointed
  at a `migrate.go` that doesn't exist in this repo; corrected to describe
  the actual mechanism.
- `experiment.go`'s `deterministicExperimentEngine.AssignVariant` used a
  check-then-act pattern (`RLock` check, unlock, compute, `Lock`, write)
  that let two goroutines both observe a brand-new (userID, experimentID)
  pair as unassigned and each publish `experiment.assigned` — collapsed to
  a single lock-held check-then-write, making the publish exactly-once for
  this engine. `cacheExperimentEngine`'s equivalent race remains
  at-least-once, documented as an inherent `grcache.Cache` limitation (no
  compare-and-swap primitive), not a grnoti bug.
- `dispatcher.fcm.go`'s `classifyFCMError` only ever substring-matched
  error text. It now tries the FCM Admin SDK's structured `messaging.IsX`
  checks first, falling back to substring-matching unchanged — more
  precise classification for real production FCM errors without changing
  behavior for this repo's fake-client test suite.

### Testing

- Coverage raised from 94.9% to 95.1% on the root package: added
  `TestPayloadValidator_EstimateSize_IncludesImageURL` (the
  `ImageURL`-set branch of `fcmPayloadValidator.EstimateSize` had no
  dedicated test) and `TestApplyPostgresSchema_AcquireFailsOnClosedPool`
  (the `pool.Acquire` failure branch of `applyPostgresSchema`, via a
  closed pool). The two remaining error branches in `applyPostgresSchema`
  (the advisory-lock and schema-apply `Exec` calls) are documented in a
  code comment as accepted, untested gaps — only reachable via a
  connection breaking mid-function, not deterministically triggerable
  against a live Postgres without a flaky timing race.

### Repository scaffolding

- `go.mod`, lint/release config, `Makefile`, `LICENSE`, `SECURITY.md`.

### Documentation

- README: added a "Part of the gourdian25 ecosystem" section (the one
  multi-backend repo that didn't have one yet), covering grnoti's real
  `grcache`/`grevents` integrations plus the rest of the sibling repos.
- README: added a short "Contributing" section pointing at `make
  precommit`, alongside the existing "Development" section.
- README: added a short "Why this shape" note explaining that grnoti's
  flat-package, pgx/sqlc, no-GORM layout came first in this ecosystem and
  was later adopted by grcache/graudit/gourdiantoken, condensed from
  `docs.go`'s "Package shape" section.
- README: tightened the loose "~95%" coverage claim to the precise,
  verified root-package number (95.1%), and noted the enforced 95% gate.
