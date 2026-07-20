# grnoti — Scope & Implementation Plan

**Status:** draft, pending review. No implementation code has been written against
this plan yet — per the mission brief, do not start coding until this has been
reviewed.

**Repo path (to be created):** `~/Dev/gourdian25/grnoti`, module
`github.com/gourdian25/grnoti`.

**Behavioral reference (spec, not code to port verbatim):**
`~/Dev/skipptech/skipp.app.shared.golang.library/grnoti` — 22 files, 7,166
lines, a working but untested single-package push-notification service
currently embedded in a shared library. Read end-to-end for this plan (see
"Research method" below).

---

## 0. Research method

The reference source was read in full via two parallel research passes (not
summarized from memory): storage/dispatch (`interfaces.go`, `types.go`,
`errors.go`, `config.full.go`, `service.go`, `fcm.dispatcher.go`,
`store.redis.go`, `store.mongo.go`, `preferences.go`, `preferences.mongo.go`,
`dlq.handler.go`) and experiment/template/support
(`experiment.go`, `template.engine.go`, `localization.go`, `topic.router.go`,
`consumer.kafka.go`, `worker.pool.go`, `circuitbreaker.go`, `ratelimiter.go`,
`retry.strategy.go`, `batch.splitter.go`, `payload.validator.go`,
`metrics.prometheus.go`, `event.types.complete.go`). Every defect and design
decision below is traceable to a specific file:line in that source, not
inferred.

Five sibling repos were read for ecosystem convention before any design
decision was made: `grcache` (interface + `cache.go` + `conformance/` +
`docs/architecture.md`), `graudit` (`events.go`, `postgres/postgres.go`,
`docs/architecture.md`, `audit.go`, `errors.go`), `grevents`
(`docs/plan/grevents-plan.md` in full — it has no code yet), `grpolicy`
(`engine.go`), `gourdiantoken` (`gourdiantoken.factories.go`, CLAUDE.md).

---

## 1. Mandatory research questions — answered

### 1.1 Should `IdempotencyStore` / preferences cache / rate limiter build on `grcache.Cache`?

**Yes for two of three, no for one — and it's a bigger win than "avoid some
boilerplate."**

`grcache.Cache` is `Get/Set/Delete/Exists/InvalidateTag/Stats/Close`, generic
`[]byte` + TTL + tags, with backends for memory/redis/memcached/postgres/mongo
already built, tested, and conformance-covered.

- **`IdempotencyStore`** (`IsProcessed`/`MarkProcessed`) maps onto `Get`/`Exists`
  + `Set(..., ttl)` exactly — no backend-specific logic is actually needed.
  The source's separate `RedisIdempotencyStore` (257 lines) and
  `MongoIdempotencyStore` (~150 lines inside `store.mongo.go`) can collapse
  into **one generic adapter in root** (`NewCacheIdempotencyStore(cache
  grcache.Cache) IdempotencyStore`) that works against *any* grcache backend.
  Backend choice becomes "which `grcache.Cache` you construct," not "which
  grnoti store type you construct." This eliminates ~400 lines of bespoke
  client code the source hand-rolled.
- **Preferences read-cache**: same reasoning, plus `InvalidateTag` is a
  perfect fit for "invalidate this user's cached preferences on write" — tag
  every cached entry with `"user:" + userID`. One generic decorator,
  `NewCachedPreferencesStore(store PreferencesStore, cache grcache.Cache, ttl
  time.Duration) PreferencesStore`, wraps any durable `PreferencesStore` with
  read-through caching against any `grcache.Cache`.
- **Experiment assignment cache** (not asked about explicitly, but same
  shape): assignment is a pure function of `hash(userID+experimentID)` — an
  `Get`/`Set` memoization, no atomicity required. Same generic
  grcache-adapter pattern applies. See §4.6.
- **Distributed rate limiter — no.** `grcache.Cache`'s interface has no
  atomic increment/CAS primitive. A correct distributed token bucket needs
  `INCR`+`PEXPIRE` or a Lua script (GCRA), and `Set`/`Get` alone cannot
  provide that without a race between the read and the write. Extending
  `grcache.Cache` itself is out of scope (it's a stable sibling library with
  a fixed, documented interface — see its CLAUDE.md). **Decision: the
  distributed `RateLimiter` gets its own `grnoti/redis` package with a raw
  `*redis.Client` and a Lua-scripted or `INCR`-based token bucket.** This is
  the one place grnoti needs a backend-specific client of its own.

Net effect: `grnoti/redis` shrinks from "IdempotencyStore + whatever else"
to just the distributed rate limiter — a small, sharply-scoped package.

### 1.2 grevents — DLQ/retry overlap

**Do not reuse or wait on grevents' `DeadLetterSink`; do reserve the
lifecycle-event integration for later.**

grevents has zero code — only `docs/plan/grevents-plan.md`. Its planned
`DeadLetterSink` is explicitly scoped to grevents' own **in-process pub/sub
redelivery**: "an event that exhausts its retries [being delivered to
in-process subscribers] is handed to a DeadLetterSink (default: in-memory,
bounded)... Durability across restarts [is explicitly out of scope] — Events
(and the DLQ) live in memory only. A process restart loses any queued or
dead-lettered events." (grevents plan §2.2, §3.5)

grnoti's `DLQHandler` solves a different problem: **durable, queryable,
cross-restart tracking of FCM push-delivery failures**, so an ops engineer
can inspect and manually or automatically retry a specific failed push days
later. These are not the same durability contract — grevents' DLQ is
allowed to lose everything on restart by design; grnoti's cannot. Building
grnoti's DLQ on top of grevents' in-memory sink would silently downgrade
grnoti's own durability promise. **Decision: grnoti's `DLQHandler` stays
grnoti-owned, backed by its own Postgres/Mongo storage** (see §4.4). Nothing
here should be "contributed to grevents" — they're solving different
problems, not duplicating one.

Where grevents *does* eventually matter for grnoti: lifecycle-event
publishing, following graudit's exact precedent (`PublishRecorded`,
`"resource.pastTenseVerb"` topics, nil-safe optional `EventBus`, best-effort,
never blocks the durable operation). grnoti should reserve
`"notification.sent"`, `"notification.failed"`, `"experiment.assigned"` as
topic names now, but **not** take an import dependency on grevents in v1,
since grevents doesn't exist yet and nothing can genuinely depend on an
unbuilt package. Document the intended shape in `docs/architecture.md` once
built (mirroring graudit's `events.go`) so it's a drop-in addition, not a
redesign, once grevents ships.

### 1.3 graudit precedent — structure and the Postgres locking technique

Structural precedent (subpackage-per-backend, `conformance/` suite,
`docs/architecture.md` for divergences) is adopted wholesale — see §5.

**The specific locking technique needs to differ, not just be copied**, and
this is worth being precise about: graudit's `pg_advisory_xact_lock` is a
**single global serialization point** — correct for graudit because there is
exactly one hash chain and only one writer may ever append at a time, by
design. grnoti's DLQ retry-claiming is the opposite shape: N worker
replicas should each be able to claim a *different* pending row
**concurrently**, with no reason to serialize them behind one lock. The
right technique is `SELECT ... FOR UPDATE SKIP LOCKED` (the source's own
defect list already names this), which lets N workers each grab a disjoint
batch without contention. Both are "transactional claim, not read-then-write"
— that's the shared principle worth taking from graudit — but they are not
the same mechanism, and using `pg_advisory_xact_lock` for DLQ claiming would
wrongly serialize an embarrassingly-parallel retry-worker pool. See §4.4 for
the full DLQ redesign, including a MongoDB alternative that turns out even
simpler (`findOneAndUpdate` + `$inc`, both natively atomic per-document, no
transaction needed at all).

### 1.4 grpolicy — is `PreferencesFilter` a fit?

**Viable, not adopted for v1.** `grpolicy.Engine.Compile`/`Evaluate` operates
on `map[string]any` attributes and would technically express quiet-hours +
opt-out logic. But grpolicy's own CLAUDE.md frames it as "the future
`grauth` repo's primary dependency" for RBAC/ABAC — pulling an expression
parser (`expr-lang/expr`) and a second execution model into grnoti for logic
that's currently a handful of straightforward boolean/time checks would be
disproportionate, and would couple grnoti's release cadence to grpolicy's.
**Decision: keep `PreferencesFilter` as native Go logic in v1** (fixing its
defects, not its architecture), but keep the interface small enough
(`ShouldSendNotification(ctx, event) (bool, string, error)`) that a
grpolicy-backed implementation could be dropped in later as an alternate
`PreferencesFilter` without an interface change. Note this as a future
option in `docs/architecture.md`, not a v1 task.

### 1.5 gourdiantoken precedent

Confirmed and adopted: sentinel-error style (`errors.Is`, no `IsX(err) bool`
helpers), `sync.Once`-guarded idempotent `Close()`. Its flat single-package
layout is explicitly **not** followed — grnoti needs Mongo + Postgres +
Redis + Kafka client libraries, and a consumer using only one backend
shouldn't pull in the other three, which is exactly the problem
grcache/graudit's subpackage-per-backend layout exists to solve. Its
`New<Maker>With<Backend>(...)` factory-naming convention *is* followed, but
grcache's config-struct-owns-its-client variant is preferred over
gourdiantoken's take-an-already-built-client variant, per grcache's own
documented reasoning (`docs/architecture.md` item 2) — this also sidesteps
gourdiantoken's own documented inconsistency (Mongo's constructor taking an
extra positional `transactionsEnabled bool` that breaks the otherwise
uniform shape).

---

## 2. Defects confirmed in the reference — and how the rewrite fixes each

All confirmed via direct code read, with file:line references into the
*source* (not the rewrite, which doesn't exist yet).

| # | Defect | Source location | Fix in grnoti |
|---|---|---|---|
| 1 | Zero test coverage across 7,166 lines | entire repo | Full `conformance/` suite per storage interface + `-race` mandatory + 70-80% per-package coverage gate, matching every sibling repo |
| 2 | `deterministicExperimentEngine.experiments`/`.assignments` maps mutated with no synchronization | `experiment.go:54-66,69-88,91-123,140-142` (zero `sync.*`/`atomic.*` in file) | **Not just "add a mutex."** Split storage from algorithm (§4.6): experiment *definitions* move to a Postgres-backed `ExperimentStore`; variant assignment becomes a pure function of `hash(userID, experimentID, variants)` with no mutable map to race on at all; assignment caching (optional, for perf) goes through a `grcache.Cache` adapter, which is already conformance-tested for concurrent access. The race is designed away, not locked away. |
| 3 | `InMemoryPreferencesStore.preferences` map mutated with no synchronization | `preferences.mongo.go:143-145,156,176,181-199` | The in-memory variant becomes a real `grnoti/memory` conformance-tested backend with a `sync.RWMutex`, matching grcache/memory's and graudit/memory's own pattern — not a silent test-only shortcut. |
| 4 | `MongoDLQHandler.MarkRetried` read-then-write race (two concurrent retries can both compute the same `newRetryCount`, one write silently loses) | `dlq.handler.go:286-354`, filter at line 324 has no version/status guard | Redesigned atomic-claim DLQ, full detail in §4.4. Postgres: `FOR UPDATE SKIP LOCKED` claim transitions `pending→retrying` atomically; `MarkRetried`'s `UPDATE` is scoped `WHERE event_id=$1 AND status='retrying'`. Mongo: `findOneAndUpdate` for the claim, `$inc` (not Go-side read+`$set`) for the counter — both natively atomic per-document, no transaction required. |
| 5 | Hard `*grlog.Logger` (concrete type) threaded through every constructor | confirmed in all 11 storage/dispatch files, e.g. `service.go:18,33,57`; `fcm.dispatcher.go:36,44,55`; `store.mongo.go:27,35,382,390`; `preferences.mongo.go:21,25` (not nil-checked — panics on first use if `nil`); `dlq.handler.go:93,104` (nil-checked) | Structural `Logger` interface (`Infof`/`Warnf`/`Errorf`) + `NopLogger()`/`OrNop()` in root `logger.go`, matching grcache/graudit/grpolicy verbatim. Every constructor calls `OrNop` — no nil-check inconsistency across backends like the source has. `*grlog.Logger` used only in test files to prove conformance. |
| 6 | Sentinel error reuse hiding real error classes | `types.go:104` (`ErrInvalidUserID` reused for "no target specified," source's own comment admits it: `// Reusing error, could create ErrNoTargetSpecified`); `experiment.go:72,93` (`ErrTemplateNotFound` reused for "experiment not found" *and* "experiment has no variants" — two different conditions, one borrowed sentinel whose message literally says "template") | New sentinels: `ErrNoTargetSpecified`, `ErrExperimentNotFound`, `ErrExperimentHasNoVariants`. See also finding beyond the original list, §3.3: 12 of 21 source sentinels are dead code, and there are two disconnected FCM-error taxonomies to consolidate into one. |
| 7 | No distributed rate limiting — `golang.org/x/time/rate` is per-process; N replicas each enforce the full FCM quota independently | `ratelimiter.go:11,56,90` (confirmed via import list: no redis, no network I/O in the file at all) | `grnoti/redis`'s Lua/`INCR`-based distributed token bucket, per §1.1. Local per-process limiter (same `x/time/rate` wrapping) stays as the default/dev option in root. |
| 8 | Skipp-specific coupling: hardcoded `skipp://` scheme, ~130 e-commerce `EventType` constants in the core package | `template.engine.go` (8 of 9 default templates, all quoted with line numbers in research — e.g. lines 49-160); `event.types.complete.go` (136 constants across a single 175-line block, lines 12-186); ~50 total `skipp://` occurrences across `template.engine.go` + `localization.go` | See §4.7 — generic vocabulary + `EventTypeCustom` + a real `EventTypeRegistry`, replacing the source's copy-pasted 8-trait switch statements with one data table. Skipp's catalog moves to a consumer-side package, not `example/` (it's real production data, not a demo). |
| 9 | No-op A/B analytics: `TrackImpression`/`TrackConversion` unconditionally `return nil` | `experiment.go:125-137`, comments explicitly say "placeholder" | New `grnoti/kafka` **producer** (source only ever consumes Kafka — this is new scope, not a port) publishes real impression/conversion events to a Kafka topic. Swappable for a grevents publish once grevents exists, per §1.2. |
| 10 | Prometheus label triple-counting: `IncNotificationsSent`/`Failed` write into the *same* two-label `CounterVec` three separate ways (unlabeled `("","")`, by-type `(type,"")`, by-platform `("",platform)`), so a caller using more than one variant for the same logical send silently triple-counts on `sum()` | `metrics.prometheus.go:76-136`, all three call patterns quoted in research | Collapse the `Metrics` interface's three incrementer variants into one call taking both labels together (`IncNotificationsSent(eventType EventType, platform Platform, count int)`), so there is exactly one label tuple per logical send, not three orphaned partial tuples. |

---

## 3. Findings beyond the original defect list

Found during the full read; not in the mission's original defect
enumeration, but material to the rewrite's architecture.

### 3.1 `WorkerPool` is built but never wired to anything

`worker.pool.go` (out-of-scope-file grep confirms `NewWorkerPool` exists) and
`ServiceConfig.EnableBackpressure` / `FullServiceConfig.WorkerPoolConfig`
(`interfaces.go:174-175`, `config.full.go:51,81-88`) exist, but
`notificationService` (`service.go:12-23`) has **no `workerPool` field at
all**, and `EnableBackpressure` is never read in `service.go`'s
`ProcessEvent`. Setting it has zero observable effect. Separately,
`consumer.kafka.go`'s `ConsumeClaim` invokes the injected `handler` closure
**synchronously**, one Kafka message at a time — there is no queue between
ingestion and processing in the source at all, despite `WorkerPool` existing
in the same package for exactly this purpose.

**Fix:** wire `WorkerPool` as the actual ingestion→processing bridge: the
Kafka consumer's handler becomes `func(ctx, event) error { return
pool.Submit(event) }`, and the pool's own workers call
`service.ProcessEvent`. This is a real architectural connection the source
never made, not a cosmetic cleanup.

### 3.2 `RateLimiter` and `CircuitBreaker` have zero touchpoints with dispatch

Grep across `fcm.dispatcher.go` and `service.go` (case-insensitive) returns
zero hits for either type. Both exist as fully-implemented, independently
correct components (`ratelimiter.go`, `circuitbreaker.go`) but
`fcmDispatcher` (`fcm.dispatcher.go:34-40`) has no field for either. The
mission's own polyglot-persistence table treats both as real, load-bearing
components of the dispatch path — the rewrite needs to actually wire them
in (`Execute`-wrap the FCM client call through the circuit breaker;
`Wait`/`Allow`-gate each outbound batch through the rate limiter), not just
port the source's already-partial wiring.

### 3.3 Two disconnected FCM-error taxonomies, and 12 dead sentinels

`errors.go:9-43` defines 21 sentinels; a targeted grep across the whole
source tree found **12 with zero references anywhere outside their own
declaration** (`ErrEventAlreadyProcessed`, `ErrNoActiveTokens`,
`ErrMessageBuildFailed`, all 6 `ErrFCM*` variants, `ErrTokenStoreFailure`,
`ErrIdempotencyStoreFailure`, `ErrContextCanceled`, `ErrContextTimeout`).
Meanwhile, `fcm.dispatcher.go`'s actual error classification
(`classifyError`, lines 633-666) builds a completely separate
`FCMErrorCode`/`FCMError` typed-error system via string-matching on the raw
SDK error text — never touching the `ErrFCM*` sentinels that exist for
exactly this purpose. **Fix:** delete the 12 dead sentinels rather than port
dead code forward; keep and extend the `FCMErrorCode`/`FCMError` taxonomy
(it's the one actually wired to real behavior via `IsRetryable`/
`IsPermanent`).

### 3.4 `RateLimiter.Reserve()` leaks a third-party type through the public interface

`ratelimiter.go`'s interface declares `Reserve() *rate.Reservation` — a
concrete type from `golang.org/x/time/rate` in the package's own public API
surface (matches every sibling repo's stated rule: "a backend-native error
[or type] must never leak through the interface unwrapped," per grcache's
`docs/architecture.md`). This isn't gratuitous cleanup — it's forced by
§1.1/§2 item 7: the new Redis-backed distributed limiter has no equivalent
concept to return, so the interface as written cannot be implemented by both
backends. **Fix:** drop `Reserve()` from the shared `RateLimiter` interface
(or replace with a grnoti-native `Reservation` type) as part of adding the
distributed variant.

### 3.5 `MongoDLQHandler.updateExistingDLQEvent` is a second, uncoordinated writer

`PublishToDLQ`'s duplicate-key fallback (`dlq.handler.go:230-252`) does its
own unguarded `UpdateOne` against the same document `MarkRetried` writes to,
always pushing `AttemptNumber: 0` regardless of true attempt count — a
second source of the same lost-update class of bug as finding #4, on a
different code path. The redesigned atomic-claim DLQ (§4.4) needs to route
both paths through the same claim/update discipline, not fix `MarkRetried`
alone and leave this one.

---

## 4. Package layout

```
grnoti/                      # root: interfaces, types, errors, logger, docs — plus
│                             # pure in-process logic with no swappable backend
│                             # (see divergence note below)
├── interfaces.go             # TokenStore, IdempotencyStore, PreferencesStore,
│                             # PreferencesFilter, DLQHandler, PushDispatcher,
│                             # EventConsumer, Metrics, ExperimentEngine, ExperimentStore,
│                             # RateLimiter, CircuitBreaker, TemplateEngine,
│                             # LocalizationStore, LocaleResolver, TopicRouter,
│                             # BatchSplitter, RetryStrategy, PayloadValidator
├── types.go                  # Event, DeviceToken, Message, DispatchResult,
│                             # ProcessingResult, IdempotencyRecord,
│                             # NotificationPreferences, NotificationAction, Priority, Platform
├── eventtypes.go             # EventType, EventTypeCustom, EventTypeRegistry (§4.7) —
│                             # NOT the 130-constant Skipp catalog
├── errors.go                 # sentinels (post-cleanup, §3.3)
├── logger.go                 # Logger interface + NopLogger/OrNop
├── docs.go                   # godoc only
├── service.go                # NotificationService (orchestrator) — see divergence note
├── circuitbreaker.go         # stdlib-only, no swappable backend
├── workerpool.go             # stdlib-only, no swappable backend; now actually wired (§3.1)
├── ratelimiter.go            # local in-memory token bucket (default/dev), fixed interface (§3.4)
├── retrystrategy.go          # stdlib-only
├── batchsplitter.go          # stdlib-only
├── payloadvalidator.go       # stdlib-only
├── templateengine.go         # default in-memory impl, generic templates only (§4.7)
├── localization.go           # LocalizationStore interface + in-memory default + LocaleResolver
├── topicrouter.go            # stdlib-only, depends only on TokenStore interface
├── experiment.go             # ExperimentEngine (pure assignment function, §4.6) — no map, no mutex needed
├── metrics.go                # Metrics interface only (no Prometheus import)
├── cache_idempotency.go      # grcache.Cache-backed generic IdempotencyStore adapter (§1.1)
├── cache_preferences.go      # grcache.Cache-backed generic PreferencesStore read-cache decorator (§1.1)
├── cache_experiment.go       # grcache.Cache-backed generic assignment-cache decorator (§4.6)
├── events.go                 # reserved topic constants + PublishSent/PublishFailed/PublishAssigned,
│                             # written once grevents exists (§1.2) — stub/unwired until then
├── conformance/               # shared behavioral suites, one per storage interface
├── mongo/                      # TokenStore (primary), DLQHandler (alt)
├── postgres/                    # TokenStore (alt/GORM), PreferencesStore (source of truth),
│                               # DLQHandler (primary), ExperimentStore
├── redis/                        # distributed RateLimiter only (§1.1) — the one genuinely
│                               # backend-specific new component
├── kafka/                         # EventConsumer (fixed logger, wired to WorkerPool §3.1) +
│                               # new analytics producer (§2 item 9)
├── fcm/                            # PushDispatcher — FCM implementation, now actually wired
│                               # to RateLimiter + CircuitBreaker + WorkerPool (§3.2)
└── example/                        # runnable demo
```

**Divergence from grcache/graudit's "root is contract-only" rule — flagged
for review, not silently decided.** grcache and graudit both keep *every*
implementation, including the in-memory one, in its own subpackage
(`grcache/memory`, `graudit/memory`), so root imports nothing beyond
stdlib (graudit's one exception being `events.go`'s grevents import).
grnoti's storage interfaces (`TokenStore`, `PreferencesStore`, `DLQHandler`,
`IdempotencyStore`, `ExperimentStore`) follow that rule exactly — their
in-memory/test variants live in a `grnoti/memory` subpackage, not root.

But grnoti also has a second category the storage-only sibling repos don't:
components with **exactly one implementation and no swappable backend at
all** (`CircuitBreaker`, `WorkerPool`, the local `RateLimiter`,
`TemplateEngine`'s rendering logic, `TopicRouter`, `BatchSplitter`,
`RetryStrategy`, `PayloadValidator`, and the orchestrating
`NotificationService` itself). None of these have a competing
implementation that isolating them into a subpackage would protect other
consumers from — there's nothing to keep out of anyone's build. This is
closer to grevents' plan (`Bus`, `async.go`, `retry.go` all live in *its*
root, because there's only ever one `Bus`). **Recommendation: keep these in
grnoti root**, since subpackaging them would fragment the API for zero
dependency-hygiene benefit — but this is a judgment call, not dictated by
existing convention, and should be confirmed before implementation starts.

**"Split mongo/postgres further?" — no.** The mission asked whether a
store's Mongo impl and another store's Mongo impl might not share enough to
co-locate. They share the one thing that matters for this rule: the same
underlying driver dependency. The subpackage-per-backend split exists to
keep *different* client libraries (Redis driver vs. Mongo driver vs.
GORM/Postgres) out of consumers who don't want them — not to further split
by which interface a given driver happens to back. `grnoti/mongo` holding
both `TokenStore` and `DLQHandler` implementations costs a consumer nothing
they weren't already paying for by importing `grnoti/mongo` at all.

---

## 5. Interface & type surface (deltas from the source only)

Full interfaces are in the source's `interfaces.go`/`types.go`/`preferences.go`
/`dlq.handler.go` — reproduced in the actual code, not this doc. Only the
changes are listed here.

- **`IdempotencyStore`** — unchanged method signatures (`IsProcessed`,
  `MarkProcessed`); implementation is now the one `grcache`-backed adapter
  (§1.1), not per-backend hand-rolled clients.
- **`DLQHandler`** — `GetRetryableEvents` becomes `ClaimRetryableEvents`,
  documented as atomically transitioning claimed rows to a "claiming" state
  as part of the same call (§4.4), not a plain read. `MarkRetried` keeps its
  signature but its contract note changes: implementations must scope the
  update to the claimed state (Postgres: `WHERE status='retrying'`; Mongo:
  `$inc` not read-then-`$set`).
- **`RateLimiter`** — drops `Reserve() *rate.Reservation` (§3.4).
- **`Metrics`** — `IncNotificationsSentByType`/`IncNotificationsSentByPlatform`/
  `IncNotificationsFailedByType`/`IncNotificationsFailedByPlatform` and their
  latency-observation counterparts collapse into
  `IncNotificationsSent(eventType EventType, platform Platform, count int)`
  / `IncNotificationsFailed(...)` / `ObserveDispatchLatency(eventType,
  platform, duration)` — one call site, both labels always supplied
  together, no more orphaned partial-label tuples (§2 item 10).
- **`ExperimentEngine`** — splits into `ExperimentStore` (CRUD for
  definitions: `Create`/`Get`/`Update`/`Delete`/`List`, Postgres-backed) and
  a leaner `ExperimentEngine` (`AssignVariant`, `GetVariant`,
  `TrackImpression`, `TrackConversion`) that takes experiment definitions as
  input rather than owning them in a mutable map (§4.6, §2 item 2).
- **`EventType`** — stays a `string`-backed type; new `EventTypeRegistry`
  interface (§4.7) replaces the source's 8 separate exhaustive `switch`
  statements over 136 hardcoded constants with one data table plus a
  `Register` method for consumer-defined types.
- **New**: `ErrNoTargetSpecified`, `ErrExperimentNotFound`,
  `ErrExperimentHasNoVariants` (§2 item 6). 12 dead sentinels removed
  (§3.3).

---

## 6. Polyglot persistence — confirmed plan

| Store | Backend | Notes |
|---|---|---|
| `TokenStore` | **MongoDB** primary, **Postgres/GORM** alt | unchanged from source's data model |
| `IdempotencyStore` | **Redis** primary via `grcache`, **Mongo** alt via `grcache` | one generic adapter, not two bespoke stores (§1.1) |
| `PreferencesStore` | **PostgreSQL** source of truth + **Redis** read-through cache via `grcache` | tag-invalidated on write (`InvalidateTag(ctx, "user:"+userID)`) |
| `DLQHandler` | **PostgreSQL** primary (`FOR UPDATE SKIP LOCKED` claim) | **Mongo** alt (`findOneAndUpdate` + `$inc`, no transaction needed — simpler than Postgres here) |
| `EventConsumer` | **Kafka** (consumer, unchanged) + **new Kafka producer** for analytics (§2 item 9) | |
| `ExperimentStore` (definitions) | **PostgreSQL** | small, relational, admin-managed |
| Experiment assignment cache | **Redis** via `grcache` | pure-function memoization, not source of truth (§4.6) |
| `RateLimiter` | **Redis**-backed distributed token bucket, raw client (not `grcache`, §1.1) | local in-memory variant stays default/dev |
| `CircuitBreaker`, `WorkerPool` queue | in-memory, per-instance, **deliberately** | centralizing risks a synchronized thundering-herd retry on FCM recovery — unchanged from mission brief |

---

## 7. Ecosystem conventions to match exactly

- `// File: <relative-path>` header on every `.go` file + `Makefile`,
  maintained by `bark` (`.bark.toml` already present in the repo).
- Sentinel errors: `errors.Is`-compatible, defined once, no `IsX(err) bool`
  helpers.
- `Logger` interface (`Infof`/`Warnf`/`Errorf`) + `NopLogger()`/`OrNop()` in
  root; `*grlog.Logger` used only in test files.
- `Close()` idempotent via `sync.Once` + `atomic.Bool` on every component
  holding a connection/goroutine.
- `conformance.Run(t, newX, opts...)` per storage interface, importing only
  the root package (avoids the import cycle the subpackage-per-backend
  layout would otherwise create) — matching grcache/graudit exactly,
  including the "options only ever relax a specific, documented guarantee"
  rule (see grcache's `WithBestEffortTagConcurrency` precedent) if any
  backend needs one.
- Real local services in tests, no mocks, `-race` mandatory. `docker run`
  commands for Redis/Postgres/Mongo(-replica-set, for any transactional
  path)/Kafka in the eventual `CLAUDE.md`, using yet another set of
  DB names/ports/indices distinct from grcache's, graudit's, and
  gourdiantoken's, so all four suites can run concurrently against shared
  local instances (matching grcache's documented reasoning).
- Coverage checked **per-package**, not just aggregate — 70-80% gate,
  `make coverage-check`.
- `docs/architecture.md` records every deliberate divergence flagged in this
  plan (the root-package judgment call in §4, the DLQ locking-technique
  divergence from graudit in §1.3) once the code exists, matching how every
  sibling repo does this.
- `docs/plan/grnoti-plan.md` (this file) becomes historical context once
  code exists, per every sibling repo's own stated convention.

---

## 8. Open decisions for review before implementation starts

These are judgment calls made during this plan, not dictated by existing
ecosystem precedent — flagging explicitly rather than burying the choice:

1. **§4 divergence**: keeping `NotificationService`/`CircuitBreaker`/
   `WorkerPool`/local-`RateLimiter`/`TemplateEngine`/`TopicRouter`/
   `BatchSplitter`/`RetryStrategy`/`PayloadValidator` in root rather than a
   subpackage, since none have a competing backend implementation.
2. **§4.6/§2 item 2**: splitting `ExperimentEngine` into `ExperimentStore`
   (Postgres CRUD) + a stateless assignment function, instead of the
   source's single mutable-map type — bigger structural change than "add a
   mutex," worth confirming before it's built.
3. **§2 item 9**: adding a Kafka *producer* for experiment analytics — new
   scope not present in the source at all (source only ever consumes
   Kafka).
4. **§1.4**: deferring grpolicy-backed `PreferencesFilter` entirely rather
   than building even an optional adapter now.
5. **§8.6 (naming)**: `GetRetryableEvents` → `ClaimRetryableEvents` rename —
   confirms the interface's atomicity contract in its name, but is a
   breaking rename relative to the source's naming.

---

## 9. Next steps

1. Review this plan (this document).
2. On approval, scaffold the repo: `go.mod`, `.golangci.yml`,
   `.goreleaser.yaml`, `Makefile`, `SECURITY.md`, `LICENSE`, `README.md`,
   `CHANGELOG.md`, `CLAUDE.md` — matching every sibling repo's hygiene set.
3. Root package first (interfaces/types/errors/logger — the contract
   everything else implements against), then `conformance/`, then each
   backend subpackage, then `example/`.
4. Write `docs/architecture.md` incrementally as real implementation
   decisions get made, not as a batch at the end — matching how grcache/
   graudit/grpolicy actually did it.
