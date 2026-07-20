# grnoti — Scope & Implementation Plan

**Status:** under active implementation (Stages 0-6 complete as of this
update). See §11 for the log of decisions made or corrected during
implementation that this plan didn't originally anticipate.

**PostgreSQL tooling correction (post-Stage-6):** this plan originally
assumed GORM (matching graudit/grcache's own Postgres backends). Implemented
with GORM first, then explicitly redirected mid-Stage-6 to **pgx/v5 +
sqlc** instead — raw SQL, compile-time-checked generated query code, no
ORM/reflection magic. sqlc's generated code lives in `internal/postgresdb`
(schema + queries + generated `Queries` type), which is an `internal/`
package, not a public subpackage — stricter than grcache/graudit's own
public `redis`/`postgres` subpackages (unimportable outside this module),
so it doesn't reopen the "no subpackages" decision in §4, it's an
implementation-isolation detail for generated code. Every
`Postgres*Store`/`PostgresDLQHandler` constructor in root now builds a
`pgxpool.Pool` + `postgresdb.Queries` via a shared `connectPostgres` helper
(`postgres.go`) instead of `gorm.Open`. `ClaimRetryableEvents`'s `FOR UPDATE
SKIP LOCKED` design (§1.3/§5) is unchanged — the sqlc version consolidated
it into a single `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP
LOCKED) RETURNING *` statement, which is actually simpler than the
GORM-transaction version (no explicit transaction wrapper needed, a single
statement is already atomic).

**Repo path (to be created):** `~/Dev/gourdian25/grnoti`, module
`github.com/gourdian25/grnoti`.

**Behavioral reference (spec, not code to port verbatim):**
`~/Dev/skipptech/skipp.app.shared.golang.library/grnoti` — 22 files, 7,166
lines, a working but untested single-package push-notification service
currently embedded in a shared library. Read end-to-end for this plan (see
§0).

**Package shape: single flat package, no subpackages.** This is a deliberate
choice, confirmed explicitly rather than defaulted to — see §4.

---

## 0. Research method

The reference source was read in full via two parallel research passes:
storage/dispatch (`interfaces.go`, `types.go`, `errors.go`, `config.full.go`,
`service.go`, `fcm.dispatcher.go`, `store.redis.go`, `store.mongo.go`,
`preferences.go`, `preferences.mongo.go`, `dlq.handler.go`) and
experiment/template/support (`experiment.go`, `template.engine.go`,
`localization.go`, `topic.router.go`, `consumer.kafka.go`, `worker.pool.go`,
`circuitbreaker.go`, `ratelimiter.go`, `retry.strategy.go`,
`batch.splitter.go`, `payload.validator.go`, `metrics.prometheus.go`,
`event.types.complete.go`). Every defect and design decision below is
traceable to a specific file:line in that source.

Six sibling repos were checked directly on disk (git log, tags, file
listing) rather than trusted from their `CLAUDE.md` summaries, after finding
that grevents' own `CLAUDE.md` was stale — it claimed "no code yet" while the
repo is actually tagged `v0.1.1` with a fully implemented, tested `Bus`.
**Lesson applied throughout this plan: verify sibling state against the
actual filesystem, not the docs describing it.** All six —
`grlog` (v0.1.1), `graudit` (v0.2.0), `grpolicy` (v0.1.1), `grcache`
(v0.2.0), `grevents` (v0.1.1), `gourdiantoken` (v2.1.1) — are real, tagged,
released packages.

---

## 1. Mandatory research questions — answered

### 1.1 Should `IdempotencyStore` / preferences cache / rate limiter build on `grcache.Cache`?

**Yes for two of three, no for one.**

`grcache.Cache` is `Get/Set/Delete/Exists/InvalidateTag/Stats/Close`, generic
`[]byte` + TTL + tags, with backends for memory/redis/memcached/postgres/mongo
already built, tested, and conformance-covered — importable as
`github.com/gourdian25/grcache` (interface) plus whichever backend
subpackage the *caller* chooses to construct (`grcache/redis`,
`grcache/mongostore`, etc.). grnoti's own files only ever import the root
`grcache` interface, never a specific backend — the caller decides which
`grcache.Cache` to hand in, so this dependency stays light regardless of
grnoti's own no-subpackage decision (§4).

- **`IdempotencyStore`** (`IsProcessed`/`MarkProcessed`) maps onto `Get`/
  `Exists` + `Set(..., ttl)` exactly. One generic adapter,
  `NewCacheIdempotencyStore(cache grcache.Cache) IdempotencyStore`, replaces
  the source's two separate hand-rolled clients (`RedisIdempotencyStore`,
  257 lines; `MongoIdempotencyStore`, ~150 lines) with ~40 lines that work
  against any backend.
- **Preferences read-cache**: same reasoning, plus `InvalidateTag` fits
  "invalidate this user's cached preferences on write" exactly — tag every
  cached entry `"user:" + userID`. `NewCachedPreferencesStore(store
  PreferencesStore, cache grcache.Cache, ttl time.Duration)
  PreferencesStore` decorates any durable store with read-through caching.
- **Experiment assignment cache**: same pattern — assignment is a pure
  function of `hash(userID, experimentID)`, so caching it is memoization,
  not a source of truth. Same generic adapter (§4.6 in the interface
  section).
- **Distributed rate limiter — no.** `grcache.Cache` has no atomic
  increment/CAS primitive; a correct distributed token bucket needs
  `INCR`+`PEXPIRE` or a Lua script, which `Get`/`Set` cannot provide without
  a read/write race. Extending grcache's interface is out of scope (stable
  sibling library, fixed documented contract). **This one gets its own raw
  `*redis.Client`.**

### 1.2 grevents — corrected after verifying the real implementation

**Original mistake, now fixed:** an earlier draft of this plan, based on
grevents' stale `CLAUDE.md`, concluded grevents didn't exist yet and grnoti
should defer any dependency on it. That was wrong — grevents is real,
tested, and already consumed in production by graudit's own `events.go`.
Corrected conclusions:

- **Lifecycle-event publishing — yes, now, following graudit's exact
  precedent.** `Bus` injected via config, nil-safe (`bus == nil` is a silent
  no-op), best-effort (`bus.Publish` errors are logged, never propagated,
  never block the durable operation). grnoti reserves and publishes
  `"notification.sent"`, `"notification.failed"`, `"experiment.assigned"` —
  matching grevents' real `Event{Topic, Payload, Timestamp, Metadata}`
  shape.
- **The DLQ conclusion does *not* change**, and this held up against the
  real code, not just the stale plan doc: grevents' own
  `NewMemoryDeadLetterSink` doc comment is explicit — "a best-effort
  recent-history buffer, not a durable audit log... entries are lost on
  process restart." grnoti's `DLQHandler` needs durability across restarts
  (an ops engineer inspecting a specific failed push days later), so it
  stays independently backed by Postgres/Mongo, not grevents' sink. The two
  solve genuinely different problems: in-process pub/sub redelivery
  (grevents) vs. durable cross-restart FCM-retry tracking (grnoti).
- **New: mirror grevents' Full-Jitter backoff formula.** `retry.go`'s
  `computeBackoff` (`sleep = random(0, min(cap, base·2^attempt))`, the AWS
  "Full Jitter" formula) is the first backoff-with-jitter implementation in
  this ecosystem — its own comment notes there was no precedent to port
  from. It's unexported, so grnoti can't import the function directly, but
  it should now be *the* ecosystem convention for any new backoff logic.
  grnoti's two retry paths (FCM dispatch retry, DLQ backoff) mirror this
  formula instead of the source's un-jittered `base·2^attempt`, which is
  worse for avoiding synchronized retry storms against FCM after an outage.

### 1.3 graudit precedent — structure and the Postgres locking technique

Structural precedent adopted: `docs/architecture.md` for divergences,
`PublishRecorded`-style best-effort event publishing (§1.2).

**The locking technique needs to differ, not be copied verbatim** — worth
being precise about, since it's easy to over-generalize "graudit uses
`pg_advisory_xact_lock`, so DLQ claiming should too." graudit's lock is a
**single global serialization point**, correct because there's exactly one
hash chain and only one writer may ever append at a time, by design.
grnoti's DLQ retry-claiming is the opposite shape: N worker replicas should
each claim a *different* pending row **concurrently**, with no reason to
serialize them. The right technique is `SELECT ... FOR UPDATE SKIP LOCKED`
(the source's own defect list already names this), letting N workers grab
disjoint batches without contention. "Transactional claim, not
read-then-write" is the shared principle worth taking from graudit — the
specific mechanism is not the same. Full DLQ redesign in §5.

### 1.4 grpolicy — is `PreferencesFilter` a fit?

**Viable, not adopted for v1.** grpolicy's `Engine.Compile`/`Evaluate` over
`map[string]any` would technically express quiet-hours + opt-out logic, but
grpolicy is explicitly staged as the future `grauth` repo's primary
dependency, and pulling in an expression parser for logic that's currently a
handful of boolean/time checks is disproportionate. Keep `PreferencesFilter`
as native Go logic, but keep its interface small
(`ShouldSendNotification(ctx, event) (bool, string, error)`) so a
grpolicy-backed implementation could replace it later without an interface
change.

### 1.5 gourdiantoken precedent

Sentinel-error style and `sync.Once`-guarded idempotent `Close()` are
adopted. Its **flat single-package layout is now the adopted layout for
grnoti too** (§4) — this reverses what an earlier draft of this plan said
("gourdiantoken's flat layout is not the right precedent"). Its
`New<Maker>With<Backend>(...)` factory-naming convention is followed for
constructors.

---

## 2. Defects confirmed in the reference — and how the rewrite fixes each

All confirmed via direct code read, file:line references into the *source*.

| # | Defect | Source location | Fix in grnoti |
|---|---|---|---|
| 1 | Zero test coverage across 7,166 lines | entire repo | Table-driven cross-backend tests (gourdiantoken's own pattern — see §7), `-race` mandatory, 70-80% per-file-group coverage gate |
| 2 | `deterministicExperimentEngine.experiments`/`.assignments` maps mutated with no synchronization | `experiment.go:54-66,69-88,91-123,140-142` (zero `sync.*`/`atomic.*` in file) | Split storage from algorithm: experiment *definitions* move to a Postgres-backed `ExperimentStore`; variant assignment becomes a pure function of `hash(userID, experimentID, variants)` with no mutable map to race on; assignment caching (optional) goes through the `grcache`-backed adapter. The race is designed away, not locked away. |
| 3 | `InMemoryPreferencesStore.preferences` map mutated with no synchronization | `preferences.mongo.go:143-145,156,176,181-199` | The in-memory variant gets a real `sync.RWMutex`, matching every other in-memory component in the rewrite — not a silent test-only shortcut. |
| 4 | `MongoDLQHandler.MarkRetried` read-then-write race (two concurrent retries can compute the same `newRetryCount`, one write silently loses) | `dlq.handler.go:286-354`, filter at line 324 has no version/status guard | Atomic-claim DLQ (§5). Postgres: `FOR UPDATE SKIP LOCKED` claim transitions `pending→retrying`; `MarkRetried`'s `UPDATE` scoped `WHERE event_id=$1 AND status='retrying'`. Mongo: `findOneAndUpdate` for the claim, `$inc` (not Go-side read+`$set`) for the counter — both natively atomic per-document. |
| 5 | Hard `*grlog.Logger` (concrete type) threaded through every constructor | all 11 storage/dispatch files, e.g. `service.go:18,33,57`; `preferences.mongo.go:21,25` (not nil-checked — panics on first use if `nil`) | Structural `Logger` interface (`Infof`/`Warnf`/`Errorf`) + `NopLogger()`/`OrNop()`, matching every sibling repo verbatim. `*grlog.Logger` used only in test files. |
| 6 | Sentinel error reuse hiding real error classes | `types.go:104` (`ErrInvalidUserID` reused for "no target specified," source's own comment admits it); `experiment.go:72,93` (`ErrTemplateNotFound` reused for "experiment not found" *and* "experiment has no variants") | New sentinels: `ErrNoTargetSpecified`, `ErrExperimentNotFound`, `ErrExperimentHasNoVariants`. See also §3.3 (12 dead sentinels, two disconnected FCM-error taxonomies). |
| 7 | No distributed rate limiting — `golang.org/x/time/rate` is per-process; N replicas each enforce the full FCM quota independently | `ratelimiter.go:11,56,90` (no redis, no network I/O in the file) | Raw-Redis Lua/`INCR`-based distributed token bucket (§1.1). Local per-process limiter stays as the default/dev option. |
| 8 | Skipp-specific coupling: hardcoded `skipp://` scheme, ~130 e-commerce `EventType` constants in the core package | `template.engine.go` (8 of 9 default templates); `event.types.complete.go` (136 constants, one 175-line block) | Generic vocabulary + `EventTypeCustom` + a real `EventTypeRegistry` (§5), replacing 8 copy-pasted trait switch statements with one data table. Skipp's catalog moves to a consumer-side package, not `example/`. |
| 9 | No-op A/B analytics: `TrackImpression`/`TrackConversion` unconditionally `return nil` | `experiment.go:125-137`, comments say "placeholder" | New Kafka **producer** (source only ever consumes Kafka — new scope) publishes real impression/conversion events. Swappable for a grevents publish later per §1.2. |
| 10 | Prometheus label triple-counting: `IncNotificationsSent`/`Failed` write into the *same* two-label `CounterVec` three ways (unlabeled, by-type, by-platform), silently triple-counting on `sum()` | `metrics.prometheus.go:76-136` | Collapse into one call taking both labels together: `IncNotificationsSent(eventType EventType, platform Platform, count int)`. |

---

## 3. Findings beyond the original defect list

### 3.1 `WorkerPool` is built but never wired to anything

`NewWorkerPool` and `ServiceConfig.EnableBackpressure`/
`FullServiceConfig.WorkerPoolConfig` exist, but `notificationService` has
**no `workerPool` field at all**, and `EnableBackpressure` is never read in
`ProcessEvent`. `consumer.kafka.go`'s `ConsumeClaim` invokes its handler
**synchronously**, one Kafka message at a time — no queue between ingestion
and processing exists in the source at all. **Fix:** wire `WorkerPool` as
the real ingestion→processing bridge (Kafka handler → `pool.Submit(event)`
→ pool workers call `service.ProcessEvent`).

### 3.2 `RateLimiter` and `CircuitBreaker` have zero touchpoints with dispatch

Grep across `fcm.dispatcher.go`/`service.go` returns zero hits for either
type, despite both being fully implemented elsewhere in the package.
**Fix:** `Execute`-wrap the FCM client call through the circuit breaker;
`Wait`/`Allow`-gate each outbound batch through the rate limiter.

### 3.3 Two disconnected FCM-error taxonomies, and 12 dead sentinels

12 of 21 sentinels in `errors.go` have zero references anywhere outside
their own declaration (`ErrEventAlreadyProcessed`, `ErrNoActiveTokens`,
`ErrMessageBuildFailed`, all 6 `ErrFCM*` variants, `ErrTokenStoreFailure`,
`ErrIdempotencyStoreFailure`, `ErrContextCanceled`, `ErrContextTimeout`).
`fcm.dispatcher.go`'s real error classification (`classifyError`, lines
633-666) builds a completely separate `FCMErrorCode`/`FCMError` system via
string-matching, never touching the `ErrFCM*` sentinels that exist for
exactly this purpose. **Fix:** delete the 12 dead sentinels; keep and extend
`FCMErrorCode`/`FCMError` (the one actually wired to `IsRetryable`/
`IsPermanent`).

### 3.4 `RateLimiter.Reserve()` leaks a third-party type through the interface

`Reserve() *rate.Reservation` puts a concrete `golang.org/x/time/rate` type
in the public interface. Forced fix, not gratuitous cleanup: the new
Redis-backed limiter has no equivalent concept to return, so the shared
interface can't be implemented by both backends as written. **Fix:** drop
`Reserve()` from `RateLimiter`.

### 3.5 `MongoDLQHandler.updateExistingDLQEvent` is a second, uncoordinated writer

`PublishToDLQ`'s duplicate-key fallback does its own unguarded `UpdateOne`
against the same document `MarkRetried` writes to, always pushing
`AttemptNumber: 0` regardless of true attempt count — a second source of the
lost-update class of bug in finding #4. The atomic-claim redesign (§5) must
route both paths through the same claim/update discipline.

### 3.6 `notificationService` never calls `DLQHandler` at all

Not previously called out as its own item: `notificationService` (source
`service.go:12-23`) has no `DLQHandler` field, and `ProcessEvent` never
calls `PublishToDLQ` anywhere — a dispatch that exhausts FCM retries is
logged and its errors surfaced in `DispatchResult.Errors`, but nothing ever
reaches the DLQ automatically. The entire DLQ subsystem is built but
unreachable from the main pipeline. **Fix:** wire `DLQHandler` as a real
dependency of the orchestrator; on a dispatch result with unresolved
failures after retries are exhausted, call `PublishToDLQ`.

---

## 4. Package layout — flat, no subpackages

**Confirmed decision.** grnoti is a single package (`package grnoti`),
following gourdiantoken's actual layout, not grcache/graudit's
subpackage-per-backend layout. Files are organized by a
`<concern>.<backend>.go` naming convention for backend-specific
implementations, and `<concern>.go` for interfaces/default logic — closer to
the source's own existing naming (`store.mongo.go`, `dlq.handler.go`) than
to gourdiantoken's `gourdiantoken.<area>.go` prefix, since the latter exists
mainly to alphabetize a flat directory and grnoti's file count doesn't need
that.

```
grnoti/
├── interfaces.go          # every interface: TokenStore, IdempotencyStore, PreferencesStore,
│                           # PreferencesFilter, DLQHandler, PushDispatcher, EventConsumer,
│                           # Metrics, ExperimentEngine, ExperimentStore, RateLimiter,
│                           # CircuitBreaker, TemplateEngine, LocalizationStore, LocaleResolver,
│                           # TopicRouter, BatchSplitter, RetryStrategy, PayloadValidator
├── types.go                # Event, DeviceToken, Message, DispatchResult, ProcessingResult,
│                           # IdempotencyRecord, NotificationPreferences, NotificationAction,
│                           # Priority, Platform
├── eventtypes.go            # EventType, EventTypeCustom, EventTypeRegistry (§5) — generic
│                           # vocabulary only, not the 130-constant Skipp catalog
├── errors.go                 # sentinels, post-cleanup (§3.3)
├── logger.go                  # Logger interface + NopLogger/OrNop
├── docs.go                     # godoc only
├── service.go                   # NotificationService orchestrator, now wired to
│                               # WorkerPool/RateLimiter/CircuitBreaker/DLQHandler (§3.1,3.2,3.6)
├── circuitbreaker.go             # stdlib-only
├── workerpool.go                  # stdlib-only, wired as ingestion→processing bridge (§3.1)
├── ratelimiter.go                  # local in-memory token bucket (default/dev), fixed interface (§3.4)
├── ratelimiter.redis.go              # distributed token bucket, raw *redis.Client (§1.1)
├── retrystrategy.go                    # Full-Jitter backoff, mirroring grevents' formula (§1.2)
├── batchsplitter.go                     # stdlib-only
├── payloadvalidator.go                   # stdlib-only
├── templateengine.go                      # default rendering impl, generic templates only (§2 item 8)
├── localization.go                         # LocalizationStore interface + in-memory default + LocaleResolver
├── topicrouter.go                           # stdlib-only, depends only on TokenStore interface
├── experiment.go                             # ExperimentEngine — pure assignment function, no mutex needed (§2 item 2)
├── metrics.go                                 # Metrics interface only, no Prometheus import
├── cache.idempotency.go                        # grcache.Cache-backed generic IdempotencyStore adapter (§1.1)
├── cache.preferences.go                         # grcache.Cache-backed PreferencesStore read-cache decorator (§1.1)
├── cache.experiment.go                           # grcache.Cache-backed assignment-cache decorator
├── events.go                                      # grevents integration: topic constants +
│                                                  # PublishSent/PublishFailed/PublishAssigned (§1.2)
├── tokenstore.mongo.go                             # TokenStore, primary
├── tokenstore.postgres.go                           # TokenStore, alt (GORM)
├── preferences.postgres.go                           # PreferencesStore, source of truth
├── experimentstore.postgres.go                        # ExperimentStore (definitions)
├── dlq.postgres.go                                     # DLQHandler, primary (FOR UPDATE SKIP LOCKED)
├── dlq.mongo.go                                         # DLQHandler, alt (findOneAndUpdate + $inc)
├── consumer.kafka.go                                     # EventConsumer, wired to WorkerPool (§3.1)
├── producer.kafka.go                                      # new: experiment analytics producer (§2 item 9)
├── dispatcher.fcm.go                                       # PushDispatcher, wired to RateLimiter +
│                                                           # CircuitBreaker + WorkerPool (§3.2)
├── memory.go                                                # in-memory test/dev variants of every
│                                                           # storage interface, real sync.RWMutex (§2 item 3)
└── example/                                                  # runnable demo, package main
```

**The tradeoff this creates, stated plainly:** importing `grnoti` pulls in
the Mongo driver, GORM+the Postgres driver, `go-redis`, `sarama` (Kafka),
and the Firebase messaging SDK into every consumer's build, regardless of
which backends they actually use — this is exactly the problem
grcache/graudit's subpackage-per-backend layout exists to avoid. This is a
real cost, not a hidden one. It is the same tradeoff gourdiantoken already
accepted (its flat package compiles in Redis+Mongo+GORM regardless of which
one backend a consumer picks), so it's a precedented pattern in this
ecosystem, not an unprecedented one — grnoti is following the gourdiantoken
lineage rather than the grcache/graudit lineage, by explicit choice.

**What doesn't change:** `grcache`- and `grevents`-backed files
(`cache.*.go`, `events.go`) only ever import those libraries' lightweight
root interface packages, not a specific backend subpackage of theirs — that
choice is deferred to whoever constructs the `grcache.Cache`/`grevents.Bus`
passed into grnoti's constructors, so those two dependencies don't add to
the "every backend driver, always" cost described above.

**Testing implication (replaces the conformance-subpackage plan):** without
a subpackage-per-backend split, there's no import-cycle problem a separate
`conformance` package would need to solve. Testing instead follows
gourdiantoken's actual convention directly: a shared test-helper file
(`grnoti_test_helpers_test.go`, mirroring gourdiantoken's
`token.test.helper_test.go`) exposes one factory function per storage
interface per backend, and a single table-driven test suite runs identical
behavioral assertions across all of them via `t.Run(backendName, ...)`
subtests — e.g. `TestTokenStore_Contract` runs the same scenarios against
`Memory`, `Mongo`, and `Postgres` subtests from one factory table. See §7.

---

## 5. Interface & type surface (deltas from the source only)

- **`IdempotencyStore`** — unchanged signatures; implementation is the one
  `grcache`-backed adapter (§1.1), not per-backend hand-rolled clients.
- **`DLQHandler`** — `GetRetryableEvents` becomes `ClaimRetryableEvents`,
  documented as atomically transitioning claimed rows to a "retrying" state
  as part of the same call. `MarkRetried` keeps its signature but its
  contract changes: implementations must scope the update to the claimed
  state (Postgres: `WHERE status='retrying'`; Mongo: `$inc`, not
  read-then-`$set`).
- **`RateLimiter`** — drops `Reserve() *rate.Reservation` (§3.4).
- **`Metrics`** — the four `*ByType`/`*ByPlatform` variants collapse into
  `IncNotificationsSent(eventType EventType, platform Platform, count int)`
  / `IncNotificationsFailed(...)` / `ObserveDispatchLatency(eventType,
  platform, duration)` — one call site, both labels always supplied
  together.
- **`ExperimentEngine`** — splits into `ExperimentStore` (CRUD for
  definitions, Postgres-backed) and a leaner `ExperimentEngine`
  (`AssignVariant`, `GetVariant`, `TrackImpression`, `TrackConversion`) that
  takes definitions as input instead of owning them in a mutable map.
- **`EventType`** — stays `string`-backed; new `EventTypeRegistry` interface
  replaces the source's 8 separate exhaustive `switch` statements over 136
  hardcoded constants with one data table plus a `Register` method for
  consumer-defined types.
- **`NotificationService`** — now takes `DLQHandler` and (optionally)
  `grevents.Bus`/`WorkerPool` as real dependencies, none of which the
  source's constructor accepted (§3.1, §3.6).
- **New**: `ErrNoTargetSpecified`, `ErrExperimentNotFound`,
  `ErrExperimentHasNoVariants`. 12 dead sentinels removed (§3.3).

---

## 6. Polyglot persistence — confirmed plan

| Store | Backend | Notes |
|---|---|---|
| `TokenStore` | **MongoDB** primary, **Postgres/GORM** alt | unchanged data model from source |
| `IdempotencyStore` | **Redis** primary via `grcache`, **Mongo** alt via `grcache` | one generic adapter, not two bespoke stores |
| `PreferencesStore` | **PostgreSQL** source of truth + **Redis** read-through cache via `grcache` | tag-invalidated on write |
| `DLQHandler` | **PostgreSQL** primary (`FOR UPDATE SKIP LOCKED`) | **Mongo** alt (`findOneAndUpdate` + `$inc`, no transaction needed) |
| `EventConsumer` | **Kafka** consumer (unchanged) + **new Kafka producer** for analytics | |
| `ExperimentStore` (definitions) | **PostgreSQL** | small, relational, admin-managed |
| Experiment assignment cache | **Redis** via `grcache` | pure-function memoization |
| `RateLimiter` | **Redis**-backed distributed token bucket, raw client | local in-memory variant stays default/dev |
| `CircuitBreaker`, `WorkerPool` queue | in-memory, per-instance, **deliberately** | centralizing risks a synchronized thundering-herd retry on FCM recovery |
| Lifecycle events (`notification.sent`/`failed`, `experiment.assigned`) | **grevents.Bus**, optional/nil-safe | real dependency now, per §1.2 |

---

## 7. Ecosystem conventions to match

- `// File: <relative-path>` header on every `.go` file + `Makefile`,
  maintained by `bark` (`.bark.toml` already present).
- Sentinel errors: `errors.Is`-compatible, defined once, no `IsX(err) bool`
  helpers.
- `Logger` interface + `NopLogger()`/`OrNop()`; `*grlog.Logger` used only in
  test files.
- `Close()` idempotent via `sync.Once` + `atomic.Bool` on every component
  holding a connection/goroutine.
- **Testing — gourdiantoken-style, not grcache/graudit-style**, per §4: one
  shared factory-table test helper per storage interface, table-driven
  `t.Run(backendName, ...)` subtests across all backends of that interface.
  Scope a run to one backend when iterating (`go test -run
  TestTokenStore_Contract/Memory ./...`) to avoid needing every live service
  up.
- Real local services in tests, no mocks, `-race` mandatory. `docker run`
  commands for Redis/Postgres/Mongo(-replica-set, for any transactional
  path)/Kafka in the eventual `CLAUDE.md`, using a distinct set of DB
  names/ports/indices from grcache's, graudit's, and gourdiantoken's own
  test suites, so all can run concurrently against shared local instances.
- Coverage checked per logical group (each backend's own file +
  its test file), not just aggregate — 70-80% gate, `make coverage-check`.
- `docs/architecture.md` records deliberate divergences once code exists —
  the flat-package decision (§4), the DLQ locking-technique divergence from
  graudit (§1.3), the ExperimentEngine store/algorithm split (§5).
- This plan doc becomes historical context once code exists, per every
  sibling repo's own stated convention.

---

## 8. Implementation stages

Each stage produces a compilable, independently-testable increment. Order
follows dependency direction — later stages only ever depend on earlier
ones, never the reverse, so `go build ./...` and `go test ./...` (scoped to
what exists so far) stay green throughout.

### Stage 0 — Repo scaffolding
`go.mod` (`github.com/gourdian25/grnoti`), `.golangci.yml`,
`.goreleaser.yaml`, `Makefile` (test/race/bench/lint/vet/fmt/coverage-check/
release targets, matching sibling repos), `LICENSE`, `SECURITY.md`, empty
`README.md`/`CHANGELOG.md` skeletons. `.bark.toml` already present in the
repo — confirm its header convention still applies.

### Stage 1 — Core contracts
`interfaces.go`, `types.go`, `eventtypes.go` (incl. `EventTypeRegistry`),
`errors.go`, `logger.go`, `docs.go`. No implementations yet — this stage is
the contract everything else builds against. Compiles standalone; unit
tests cover `EventTypeRegistry`, `Event.Validate()`, and sentinel wiring
only.

### Stage 2 — Pure in-process logic (zero external dependencies)
`circuitbreaker.go`, `workerpool.go`, `ratelimiter.go` (local token bucket),
`retrystrategy.go` (Full-Jitter, §1.2), `batchsplitter.go`,
`payloadvalidator.go`, `templateengine.go` (generic templates, no
`skipp://`), `localization.go` (in-memory default), `topicrouter.go`,
`experiment.go` (stateless assignment function, §2 item 2). Each is
independently unit-testable with no live services — table-driven tests,
`-race` from day one since this is exactly the layer that had the
unsynchronized-map defects in the source.

### Stage 3 — `memory.go`: in-memory storage variants
Real `sync.RWMutex`-protected in-memory implementations of `TokenStore`,
`PreferencesStore`, `DLQHandler`, `IdempotencyStore`, `ExperimentStore` —
the test/dev default and the first backend exercised by the Stage-7 shared
contract tests, since it needs no live service.

### Stage 4 — grcache-backed adapters
`cache.idempotency.go`, `cache.preferences.go`, `cache.experiment.go` (§1.1).
Tested against `grcache/memory` (no live service) and optionally
`grcache/redis` for integration coverage once Stage 8's Redis setup exists.

### Stage 5 — MongoDB backends
`tokenstore.mongo.go`, `dlq.mongo.go` (findOneAndUpdate + `$inc` claim,
§1.3/§5). Tested against real local MongoDB (replica set required for any
transactional path).

### Stage 6 — PostgreSQL backends
`preferences.postgres.go`, `experimentstore.postgres.go`,
`dlq.postgres.go` (`FOR UPDATE SKIP LOCKED` claim, §1.3/§5),
`tokenstore.postgres.go` (alt/GORM). Tested against real local PostgreSQL.

### Stage 7 — Cross-backend contract tests
The gourdiantoken-style shared test-helper file (§4, §7) exercising every
storage interface across every backend built in Stages 3, 5, 6 via
table-driven subtests. This is the point where the "zero test coverage"
defect (§2 item 1) gets closed for the storage layer specifically.

### Stage 8 — Redis: distributed rate limiter
`ratelimiter.redis.go` (§1.1, §2 item 7), raw `*redis.Client`, Lua- or
`INCR`-based token bucket. Tested against real local Redis; extend Stage 4's
`grcache/redis` integration coverage here too.

### Stage 9 — Kafka: consumer + new producer
`consumer.kafka.go` (fixed logger, wired to Stage-2's `WorkerPool`, §3.1),
`producer.kafka.go` (new — experiment analytics, §2 item 9, closing the
no-op `TrackImpression`/`TrackConversion` defect). Tested against real
local Kafka.

### Stage 10 — FCM dispatcher
`dispatcher.fcm.go`, now actually wired to `RateLimiter` + `CircuitBreaker`
+ `WorkerPool` (§3.2) — the source built all three components but never
connected any of them to dispatch. Fixed error taxonomy (§3.3), fixed
metrics label collapsing (§2 item 10).

### Stage 11 — `events.go`: grevents integration
Topic constants + `PublishSent`/`PublishFailed`/`PublishAssigned`, wired as
an optional `grevents.Bus` field on `NotificationService` and
`ExperimentEngine` (§1.2), following graudit's real, already-proven
pattern.

### Stage 12 — `service.go`: orchestration
`NotificationService`, wiring every prior stage together into the
`ProcessEvent` pipeline: fixes the validation/preferences/idempotency
ordering, wires `WorkerPool` as the real ingestion bridge (§3.1), wires
`DLQHandler` so exhausted-retry dispatches actually reach it (§3.6), wires
the optional `EventBus` from Stage 11. This is the integration point where
individually-correct pieces either do or don't compose correctly —
end-to-end tests belong here, on top of each piece's own unit/contract
tests from earlier stages.

### Stage 13 — Polish
`example/` (runnable demo, `package main`), `README.md`, `CLAUDE.md`,
`docs/architecture.md` (recording the divergences flagged throughout this
plan), `CHANGELOG.md` entries, final `make precommit`/`make prerelease`
pass.

---

## 9. Open decisions for review before Stage 0 starts

Judgment calls made during this plan, not dictated by existing precedent —
flagged rather than buried:

1. **§4**: flat single package, no subpackages — confirmed, reverses this
   plan's earlier recommendation. The dependency-graph cost (every consumer
   of `grnoti` pulls in Mongo+Postgres+Redis+Kafka+Firebase driver code
   regardless of which they use) is accepted as the gourdiantoken-lineage
   tradeoff, not grcache/graudit's.
2. **§5/§2 item 2**: splitting `ExperimentEngine` into `ExperimentStore`
   (Postgres CRUD) + a stateless assignment function, instead of the
   source's single mutable-map type.
3. **§2 item 9**: adding a Kafka *producer* for experiment analytics — new
   scope, not present in the source.
4. **§1.4**: deferring grpolicy-backed `PreferencesFilter` entirely.
5. **§5 naming**: `GetRetryableEvents` → `ClaimRetryableEvents` — a
   breaking rename relative to the source, chosen to make the interface's
   atomicity contract explicit in its name.
6. **§1.2/§4**: taking a real `grevents` dependency now that it's confirmed
   built and released, rather than deferring it.

## 10. Next steps

Stage 0, on approval of this plan.

## 11. Implementation log (decisions made or corrected during Stages 0-10)

- **Real local services used throughout**, per the ecosystem's own testing
  philosophy — `grnoti-mongo`, `grnoti-postgres`, `grnoti-redis` docker
  containers, not mocks. This caught two real defaulting bugs no amount of
  code review would have: `NewMemoryDLQHandler`/`NewMongoDLQHandler` both
  originally defaulted `RetryDelay<=0` to 5 minutes, silently breaking any
  caller (including the test suite) that deliberately passed `0` for
  "immediately retry-eligible." Fixed by only defaulting `MaxRetries` (a
  parameter with no sensible zero-value meaning) and passing
  `RetryDelay`/`MaxRetryDelay` through unchanged, `0` included.
- **`preferencesfilter.go` was missing from the original Stage 2 file
  list** — an oversight caught while wiring Stage 4's cache decorator
  (which needed `PreferencesFilter` to exist). Implemented then, including
  `isWithinQuietHours`'s midnight-wraparound math (e.g. `22:00`-`06:00`),
  which the mission brief had explicitly flagged as "needs verification,
  not asserted as a bug" — verified correct via table-driven tests
  (`TestPreferencesFilter_QuietHours_WrappingMidnight`).
- **PostgreSQL: pgx + sqlc, not GORM** — see the correction note at the top
  of this document.
- `ClaimRetryableEvents`'s Postgres implementation turned out simpler with
  sqlc than originally planned: one `UPDATE ... WHERE id IN (SELECT ... FOR
  UPDATE SKIP LOCKED) RETURNING *` statement instead of an explicit
  GORM transaction wrapping a SELECT-then-UPDATE pair.
- Every concurrent-claim design (`memoryDLQHandler`, `mongoDLQHandler`,
  `postgresDLQHandler`) has a dedicated stress test proving no event is
  ever claimed twice under real concurrent load — run under `-race` for the
  in-memory backend, against real MongoDB/PostgreSQL instances for the
  other two.
- **Stage 7 test-hygiene bugs, both in the harness, not the product** —
  worth recording since both were only found by actually re-running the
  suite twice against live services, not by review:
  1. `newStore func() TokenStore`-shaped factories that call `t.Skip` on an
     *outer* `*testing.T` captured by closure, invoked from inside a nested
     `t.Run`, target the wrong test's bookkeeping. Fixed by threading the
     currently-executing `*testing.T` through every contract-test factory
     (`func(t *testing.T) TokenStore`, etc.) instead of capturing one.
  2. A cleanup that reused the store's own connection/pool silently did
     nothing: a test body's `defer store.Close()` always runs before that
     test's `t.Cleanup`-registered functions, regardless of registration
     order, so cleanup code needs its own connection independent of the
     thing under test's lifecycle. Compounded by discarding the resulting
     error (`_, _ = pool.Exec(...)`) — with the error checked, this would
     have failed loudly instead of silently leaving data behind. Same root
     cause hit both the Postgres row-cleanup helper and, separately, the
     Mongo per-subtest collection naming (`t.Name()` is stable *within* one
     `go test` invocation but identical *across* separate ones, so without
     an explicit drop a real MongoDB instance accumulates every prior run
     under the same name). Fixed by giving both `cleanupContractRows` and
     `cleanupContractCollection` their own short-lived connections, and by
     verifying the fix with three consecutive fresh (`-count=1`) runs, not
     one.
- **Stage 8 — `ratelimiter.redis.go`**: distributed token bucket over a raw
  `*redis.Client`, following grcache/redis's own-config/own-client/
  Ping-on-construct/`sync.Once` Close convention rather than gourdiantoken's
  take-a-handle pattern (matching grnoti's existing Mongo backends' choice).
  Refill-then-consume runs as a single Lua script (`tokenBucketScript`) so
  concurrent callers across replicas see one consistent bucket instead of a
  read-modify-write race from separate GET/compute/SET round trips —
  verified with `TestRedisRateLimiter_SharedBucketIsGloballyConsistent`,
  which runs two independent limiter instances against the same key
  concurrently and confirms they share one quota rather than each
  enforcing the full rate independently (the defect this backend exists to
  fix, §2 item 7). `Wait` polls `Allow` (20ms interval) since there's no
  Redis-native blocking primitive for a Lua-scripted bucket the way BLPOP
  gives a plain list. `RateLimiter`'s interface intentionally still has no
  `Close()` (documented on the interface directly) — `localRateLimiter`
  owns no resource, so `redisRateLimiter`'s `Close() error` lives on its
  concrete type only, reached via type assertion, the same pattern already
  used for `UpdateLimit`.
- Also closed Stage 4's deferred item: `cache.redis_test.go` extends the
  `grcache`-backed adapters' coverage (previously exercised only against
  `grcache/memory`) to real local Redis via `grcache/redis`, now that
  Stage 8 established the Redis test setup.
- **Same test-hygiene bug as Stage 7, caught the same way (three
  consecutive fresh runs, not one)**: the initial `cache.redis_test.go`
  derived its cache keys/IDs from `t.Name()` alone. Real Redis, like real
  Mongo/Postgres, persists across separate `go test` invocations — a rerun
  within the entries' TTL window collided with the prior run's cached
  value, so `TestCacheIdempotencyStore_Redis_MarkAndCheck`'s "unmarked"
  assertion and `TestCachedPreferencesStore_Redis_ReadThroughAndInvalidate`'s
  call-count assertion both failed on the second run despite passing on the
  first. Fixed by appending a `time.Now().UnixNano()` nonce to every
  dynamic key/ID in that file, same fix shape as the Mongo DLQ tests'
  `fmt.Sprintf("dlq_%d", time.Now().UnixNano())` collection naming.
- **Stage 9 — `consumer.kafka.go` / `producer.kafka.go`**: no sibling repo
  has a Kafka convention to follow (`grevents` is in-process pub/sub only,
  confirmed by inspecting its file list — no `sarama`/broker code
  anywhere), so both files follow the reference implementation's structure
  directly, with defect #5's fix already generalized (grnoti's own `Logger`
  interface, not `*grlog.Logger`) rather than a new deviation.
  `github.com/IBM/sarama` v1.60.0 added as a new dependency; tested against
  a real local single-broker Kafka (`apache/kafka:3.7.0`, KRaft mode, no
  Zookeeper needed — `docker run -d --name grnoti-kafka -p 9092:9092
  apache/kafka:3.7.0`), continuing this repo's real-services-only testing
  philosophy.
  - `kafkaEventConsumer` has no compile-time dependency on `WorkerPool` —
    `EventConsumer.Start`'s handler is supplied by the caller, so "wired to
    WorkerPool" (§3.1) is proven by
    `TestKafkaEventConsumer_WiresToWorkerPool` (a handler that does nothing
    but `pool.Submit`), not a direct import. Full composition is Stage 12's
    job.
  - `TestKafkaEventConsumer_HandlerErrorRedeliversAfterRestart` proves the
    at-least-once contract for real: a message whose handler errors is
    never marked, so a second consumer in the same group (simulating a
    crash-and-restart) receives it again — not just asserted from reading
    the code.
  - `kafkaAnalyticsPublisher` (`producer.kafka.go`) closes defect §2 item 9
    (no-op `TrackImpression`/`TrackConversion`): a synchronous
    (`sarama.SyncProducer`, not async) Kafka producer, messages keyed by
    `userID` for per-user partition ordering. `experiment.go`'s
    `AnalyticsPublisher` wiring (added in Stage 2/6) needed no changes — it
    already called through the interface.
  - **Same test-hygiene bug as Stages 7-8, caught the same way**: an
    earlier draft of `kafka_test.go` derived topic names from `t.Name()`
    alone; fixed before landing by adding a `time.Now().UnixNano()` nonce
    (`testKafkaTopic`), consistent with the Mongo/Redis precedent — Kafka
    topics persist across `go test` invocations exactly like Mongo
    collections and Redis keys do.
- **Stage 10 — `dispatcher.fcm.go`**: `firebase.google.com/go/v4` added as a
  new dependency. `FCMClient` (the Admin SDK subset fcmDispatcher needs) is
  the one deliberate exception to this repo's real-services testing
  policy — FCM has no local emulator for actually delivering pushes, unlike
  Mongo/Postgres/Redis/Kafka, which all run in real local docker containers
  for their own suites — so `dispatcher.fcm_test.go` exercises the
  dispatcher's own logic (batching, platform grouping, retry, error
  classification, rate-limiter/circuit-breaker wiring) against a fake
  `FCMClient`, matching the reference implementation's own justification
  for the same interface.
  - §3.2's actual fix landed here: `RateLimiter.Wait` gates every
    outbound batch/single-send before it reaches `FCMClient`, and
    `CircuitBreaker.Execute` wraps every `FCMClient` call — both optional
    on `FCMDispatcherDeps`, proven by `TestFCMDispatcher_Send_
    RateLimiterGatesEachBatch` (asserts one `Wait` call per batch) and
    `TestFCMDispatcher_Send_CircuitBreakerOpensAndShortCircuits` (asserts
    zero further client calls once the breaker trips).
  - `Metrics.IncInvalidTokens` is called from here (the dispatcher does
    classify invalid tokens); `IncNotificationsSent`/`Failed` and
    `ObserveDispatchLatency` are deliberately *not* called from
    `dispatcher.fcm.go` — `PushDispatcher.Send` only receives
    tokens+`Message`, never the originating `Event`/`EventType` those
    calls require, so that wiring belongs in `NotificationService`
    (Stage 12), not here.
  - **Real bug found while writing `TestFCMDispatcher_Send_
    RetriesRetryableErrorsAndRecovers`, fixed before landing**: the first
    draft of `sendBatchWithRetry` only continued retrying while
    `result.RetryableErrors > 0` — but a *total* request-level failure
    (the whole `SendEachForMulticast` call erroring, e.g. a transient
    network error) never populates `RetryableErrors` at all, since that
    counter only comes from classifying individual per-token responses in
    a batch that otherwise succeeded. The bug: a batch that fails
    outright was retried exactly once (attempt 0) and then silently
    abandoned, `RetryableErrors == 0` looking identical to "nothing left
    to retry." Fixed by tracking the two failure shapes separately: a
    total failure (`sendBatch` returns a non-nil error) now defers to
    `RetryStrategy.ShouldRetry(attempt, err)`, while a partial per-token
    failure (`nil` error, `RetryableErrors > 0`) is its own retry signal —
    routing the latter through `ShouldRetry(attempt, nil)` would never
    retry at all, since `fullJitterRetry.ShouldRetry` unconditionally
    treats a `nil` err as "don't retry."
  - `classifyFCMError`'s substring-matching approach is kept as-is from
    the reference (`fcm.dispatcher.go:632-666`) — the Admin SDK exposes no
    structured error-code type, only message text, so there's no better
    signal to classify on. §3.3's 12 dead `ErrFCM*` sentinels were never
    carried into grnoti's `errors.go` in the first place (Stage 1), so
    there was nothing left to delete at this stage.
