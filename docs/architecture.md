# grnoti architecture

This document records how grnoti is put together and *why*, with particular
attention to where it deliberately diverges from the reference
implementation it was built from (see [docs/plan/grnoti-plan.md](plan/grnoti-plan.md)
for the stage-by-stage account these decisions were made in). It is a
design reference, not a tutorial — see the [README](../README.md) for a
quickstart and [example/main.go](../example/main.go) for a runnable,
narrated walkthrough.

## 1. Package shape

grnoti's public API is a single flat package with no subpackages — every
backend (MongoDB, PostgreSQL, Redis, Kafka, FCM) lives in this one module,
distinguished by a `<concern>.<backend>.go` file-naming convention:

```
tokenstore.mongo.go       tokenstore.postgres.go
dlq.mongo.go               dlq.postgres.go
preferences.postgres.go
experimentstore.postgres.go
ratelimiter.go (local)     ratelimiter.redis.go
consumer.kafka.go          producer.kafka.go
dispatcher.fcm.go
cache.idempotency.go       cache.preferences.go       cache.experiment.go
memory.go                  (in-memory variant of every store, one file)
```

The one exception is `internal/postgresdb` — sqlc-generated query code. It
is a real Go subpackage, but an unexported `internal/` one: not importable
outside this module, so it doesn't undermine the "flat public API" claim,
and it's excluded from this repo's coverage target (see §5).

**This is a deliberate divergence from sibling repos** like `grcache` and
`graudit`, which use one subpackage per backend (e.g. `grcache/redis`,
`grcache/postgres`) specifically to keep unused backend drivers out of a
consumer's dependency graph. grnoti accepts that cost instead — importing
`grnoti` at all pulls in the MongoDB driver, pgx/v5 + sqlc-generated
Postgres code, go-redis, sarama (Kafka), and the Firebase Admin SDK,
regardless of which backends a given deployment actually uses. This
follows `gourdiantoken`'s precedent rather than `grcache`'s. The tradeoff:
a simpler package to navigate and import (one `import "grnoti"`, not one
per backend) at the cost of a heavier dependency graph for every consumer.

## 2. Interfaces and backend matrix

Every capability is a small interface (`interfaces.go`), with 2-3
concrete implementations each:

| Interface           | In-memory (`memory.go`) | Cache-backed (`cache.*.go`) | Mongo | Postgres | Other |
|----------------------|:---:|:---:|:---:|:---:|---|
| `TokenStore`          | ✓ | — | ✓ | ✓ | |
| `PreferencesStore`     | ✓ | ✓ (read-through) | — | ✓ | |
| `DLQHandler`           | ✓ | — | ✓ | ✓ | |
| `ExperimentStore`      | ✓ | — | — | ✓ | |
| `ExperimentEngine`     | — | ✓ (`cacheExperimentEngine`) | — | — | `deterministicExperimentEngine` (in-process map) |
| `IdempotencyStore`     | — | ✓ | — | — | thin adapter over any `grcache.Cache` |
| `RateLimiter`          | — | — | — | — | `localRateLimiter` (per-process) / `redisRateLimiter` (distributed) |
| `PushDispatcher`       | — | — | — | — | `fcmDispatcher` (the only real implementation; FCM is the notification backend) |
| `EventConsumer`/`AnalyticsPublisher` | — | — | — | — | Kafka only (`consumer.kafka.go`/`producer.kafka.go`) |
| `TemplateEngine`       | `defaultTemplateEngine` (in-process) | — | — | — | `localizedTemplateEngine` decorator (locale-aware, wraps another `TemplateEngine`) |
| `TopicRouter`          | — | — | — | — | `eventTypeTopicRouter` / `staticTopicRouter` / `tokenOnlyRouter` — all pure in-process logic |
| `CircuitBreaker`       | — | — | — | — | `standardCircuitBreaker` — one implementation, deliberately a concrete type in the public API, not an interface (see §3.6) |

`NotificationService` (`service.go`) is the orchestrator that composes all
of the above via `ServiceDeps`; see `example/main.go` for a full wiring.

## 3. Key design decisions

### 3.1 `NotificationService.Submit` as the ingestion bridge

`EventConsumer.Start(ctx, handler func(context.Context, Event) error)` and
`NotificationService.Submit(ctx context.Context, event Event) error` have
identical signatures on purpose:

```go
consumer.Start(ctx, service.Submit)
```

is the entire Kafka-to-processing wiring. When the service is constructed
with `ServiceConfig.EnableBackpressure`, `Submit` enqueues onto the
service's own internal `*WorkerPool` (non-blocking, `ErrWorkerPoolFull` on
a full queue) instead of processing on the ingestion goroutine; otherwise
it's `ProcessEvent` with the `ProcessingResult` discarded. The reference
implementation had `EventConsumer` call `ProcessEvent` directly with no
queue and no backpressure at all — an ingestion burst had nowhere to go
but straight into synchronous processing.

### 3.2 RateLimiter/CircuitBreaker are actually wired into dispatch

`FCMDispatcherDeps.RateLimiter`/`CircuitBreaker`, when set, gate every
outbound FCM batch/single-send (`sendBatch`, `SendToToken`, `SendToTopic`
in `dispatcher.fcm.go`). The reference implementation built both
components in full but never connected either to the actual dispatch
path — they existed and were tested in isolation, but a real FCM outage
would never have tripped the breaker or been rate-limited.

### 3.3 Two independent batching layers that compose, not conflict

`dispatcher.fcm.go` always internally batches at `FCMMaxBatchSize` (FCM's
own per-multicast-call limit) — this is not configurable and not
optional. `service.go`'s `ServiceConfig.EnforceBatching`/
`MaxTokensPerBatch` is a separate, optional, caller-controlled pre-split
of a resolved token list into smaller `PushDispatcher.Send` calls before
FCM's own batching ever sees them. The two are orthogonal: enabling the
service-level one just means more, smaller calls into a dispatcher that
still batches internally at its own fixed limit.

### 3.4 `RateLimiter` has no `Reserve()`

Unlike the reference implementation, this package's `RateLimiter`
interface does not expose `Reserve() *rate.Reservation` — that leaked a
`golang.org/x/time/rate`-specific type through the interface, which
`redisRateLimiter` (the distributed variant, backed by a Lua token-bucket
script, not `x/time/rate` at all) has no equivalent for. `Allow`/`Wait` are
sufficient for every real caller in this codebase.

### 3.5 `ExperimentEngine` split: stateless assignment + a separate store

Variant assignment (`deterministicPick` in `experiment.go`) is a pure,
deterministic function of `(userID, experiment.ID, experiment.Variants)` —
the same inputs always produce the same variant, with or without an
assignment cache in front of it. This is split from `ExperimentStore`
(Postgres/in-memory CRUD for `Experiment` definitions themselves), unlike
the reference implementation's single mutable-map type that conflated
"define an experiment" with "remember who got which variant." Two
`ExperimentEngine` implementations exist purely as a performance choice
for where the assignment cache lives: `deterministicExperimentEngine`
(an in-process map guarded by a `sync.RWMutex`) for single-instance
deployments, and
`cacheExperimentEngine` (backed by any `grcache.Cache`, e.g.
`grcache/redis`) so multiple service instances share one assignment
cache instead of each independently recomputing and caching — both
produce the identical variant for a given input regardless, since
assignment is deterministic; the cache is purely an optimization, never a
correctness dependency.

### 3.6 `DLQHandler.ClaimRetryableEvents`: atomic claim, not a plain read

The reference implementation's `GetRetryableEvents` was a plain read with
no claim semantics — two concurrent workers could read and retry the same
failed event simultaneously. `ClaimRetryableEvents` (a deliberate,
breaking rename to make the contract explicit) atomically transitions
every event it returns to `DLQStatusRetrying` as part of the same
operation: Postgres does this with a single `SELECT ... FOR UPDATE SKIP
LOCKED`-based `UPDATE` statement (all-or-nothing — a mid-claim error
returns `nil`, since a single SQL statement can't partially fail);
MongoDB, which has no equivalent single-statement "claim up to N" query,
claims one document per iteration via `FindOneAndUpdate`, so a mid-loop
error can return a **non-nil, non-empty** slice alongside the error —
those documents were already durably claimed before the failure, and
`DLQHandler.ClaimRetryableEvents`'s own doc comment (`interfaces.go`)
states explicitly that callers must still process a non-nil slice even
when `err != nil`, since there is no reclaim-timeout sweep in this
package to recover an abandoned claim otherwise.

`NotificationService` never calls `ClaimRetryableEvents` itself — it's a
public API for an external retry-worker process to call periodically,
exactly like the reference. `service.go`'s own wiring
(`publishToDLQIfUnresolved`) only ever calls `PublishToDLQ`.

### 3.7 DLQ durability vs. `grevents.DeadLetterSink`

`DLQHandler`'s durability guarantee is independent of and stronger than
`grevents`' own `DeadLetterSink`: an event dead-lettered via `DLQHandler`
survives a process restart (Mongo/Postgres-backed), while
`grevents.DeadLetterSink` is an in-memory, best-effort recent-history
buffer by design. grnoti optionally publishes lifecycle events
(`notification.sent`, `notification.failed`, `experiment.assigned`) via an
injected `grevents.Bus` (`events.go`), but that publish is always
best-effort — a nil bus or a publish failure never affects the durable
operation it follows (`PublishSent`/`PublishFailed`/`PublishAssigned` are
all nil-safe and only log a warning on failure).

### 3.8 Two-tier caching, not two more hand-rolled backend clients

`cache.idempotency.go`, `cache.preferences.go` (read-through), and
`cache.experiment.go` (assignment cache) are all thin adapters over any
`grcache.Cache` — the reference implementation instead hand-wrote separate
~150-400 line Redis and Mongo clients for idempotency alone. Backend
choice for these three concerns becomes "which `grcache.Cache` the caller
constructs and passes in" (`grcache/redis`, `grcache/memory`, ...), not
"which grnoti-specific store type to construct." A cache read/write
failure always degrades to hitting the durable store directly (for
preferences) or is surfaced as an error the caller can log (for
idempotency) — never a silent correctness gap.

### 3.9 Distinct sentinel errors, not reused ones

`ErrNoTargetSpecified`, `ErrExperimentNotFound`, and
`ErrExperimentHasNoVariants` are each their own sentinel. The reference
implementation reused `ErrInvalidUserID` for the semantically different
"event has no target at all" condition, and reused `ErrTemplateNotFound`
for both "no experiment with this ID" and "experiment has no variants" —
two unrelated failure modes collapsed into one error a caller's
`errors.Is` check couldn't distinguish.

### 3.10 Templates: no hardcoded scheme, compiled once per registration

`NewTemplateEngine()`'s built-in defaults are deliberately scheme-free and
deep-link-free — the reference implementation hardcoded a `"skipp://"`
deep-link scheme into 8 of its 9 default templates, baking one specific
application's URL scheme into a supposedly generic library default.
Consumers register their own application-specific templates (including
their own deep-link scheme) via `RegisterTemplate`.
`localizedTemplateEngine` (the locale-aware decorator, `localization.go`)
compiles a localized `MessageTemplate` once, at registration time, and
reuses the compiled form on every `BuildMessage` call — the reference
implementation's equivalent constructed and fully re-registered a brand
new `TemplateEngine`, all default templates included, on every single
`BuildMessage` call just to render one localized message.

### 3.11 FCM error classification: kept, dead sentinels dropped

`classifyFCMError` (`dispatcher.fcm.go`) substring-matches the FCM Admin
SDK's raw error text into a small `FCMErrorCode` enum, because the SDK
exposes no structured error-code type — this is the same approach the
reference implementation used, kept as-is since it's the classification
actually wired into retry/invalid-token decisions. The reference also
declared 12 `ErrFCM*` sentinel errors that this classification never
touched at all; those simply don't exist in `errors.go` here — there was
nothing to remove because they were never added.

### 3.12 Postgres via pgx/v5 + sqlc, not GORM

Every Postgres-backed store (`internal/postgresdb`, generated by sqlc from
`internal/postgresdb/queries/*.sql`) uses `pgx/v5` directly rather than
GORM — a pivot made mid-build after starting with GORM as originally
planned. `postgres.go` holds the shared connection/pooling logic
(`connectPostgres`, `PostgresConfig`) every Postgres store's own
constructor calls into, plus the embedded schema (`//go:embed
internal/postgresdb/schema.sql`) applied via `CREATE TABLE IF NOT EXISTS`
on every connect — grnoti has one linear schema and no separate migration
tool dependency.

## 4. Testing philosophy

Every backend is tested against a real local instance — Docker containers
for MongoDB, PostgreSQL, Redis, and Kafka (KRaft-mode, no Zookeeper) — not
mocks. **FCM is the one deliberate exception**, via a hand-rolled fake
`FCMClient` (`dispatcher.fcm_test.go`), since Firebase has no local
emulator to run against. This is a stronger bar than typical
interface-mocking unit tests, and it caught real, previously-latent bugs
that code review alone did not — most notably:

- The reference implementation had a confirmed data race in its
  experiment-assignment map under concurrent access (see §9 item 2 in the
  plan doc); `experiment.go` was designed to avoid it from the start, and
  `TestDeterministicExperimentEngine_ConcurrentAssignVariant` is the
  falsifying test run under `-race` with real concurrent goroutines that
  proves grnoti's own design doesn't reproduce it — the kind of guarantee
  a single-threaded review can't provide either way.
- `eventTypeTopicRouter`/`tokenOnlyRouter`'s token-resolution fallback
  silently dropped anonymous and direct-token events when topic routing
  was enabled — invisible until an actual end-to-end pipeline test wired
  a real `TopicRouter` against a real `TokenStore`.
- `sendBatchWithRetry` (`dispatcher.fcm.go`) only retried on a partial
  per-token failure, silently abandoning a *total* request-level failure
  after one attempt — only visible once a fake client was made to return
  a real total-failure error across a real retry loop.

See `docs/plan/grnoti-plan.md`'s §11 implementation log for the full,
stage-by-stage account of every bug found this way, plus the three found
during the post-Stage-12 hardening pass (a Redis rate limiter missing a
`Close`-guard, a template-rendering error silently swallowed instead of
propagated, and a DLQ contract-documentation gap).

## 5. Coverage scope

The `internal/postgresdb` package (sqlc-generated query wrappers) is
excluded from this repo's coverage target — it's generated glue code,
already exercised indirectly through every Postgres store's own
real-database tests, and directly unit-testing generated 1:1 SQL wrappers
adds no real signal. Run `go test -cover .` (not `go test -cover ./...`)
for the number that matters; the latter dilutes the total with that
package's necessarily-zero direct coverage. See `Makefile`'s
`coverage-check` target, which already scopes to `.` for this reason.

## 6. Known, documented gaps

A few branches are deliberately left untested rather than chased with
disproportionate new fault-injection infrastructure — each has an inline
comment at its site explaining why:

- Kafka's deep consumer-group-session races (`consumer.kafka.go`'s
  `Start`/`WaitReady`) and the `consumerGroup.Close()`/`producer.Close()`
  error branches — would need a fake sarama consumer-group/producer.
- `sendBatchWithRetry`'s ctx-canceled-mid-backoff-wait branch — a precise
  timing race.
- `dlq.mongo.go`'s generic mid-loop claim error — needs Mongo-level fault
  injection distinct from the connectivity/empty-config branches that are
  covered.
- Two structurally-unreachable defensive branches kept for future-proofing
  rather than removed: `service.go`'s `NewNotificationService` "build
  worker pool" error wrap, and `payloadvalidator.go`'s `EstimateSize`
  JSON-marshal-failure fallback.
