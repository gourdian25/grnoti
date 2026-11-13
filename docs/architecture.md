# grnoti architecture

This document records how grnoti is put together and *why* (see
[docs/plan/grnoti-plan.md](plan/grnoti-plan.md) for the stage-by-stage
account these decisions were made in). It is a
design reference, not a tutorial — see the [README](../README.md) for a
quickstart and [example/main.go](../example/main.go) for a runnable,
narrated walkthrough.

## 1. Package shape

grnoti's public API is a single flat package with no subpackages — every
backend (MongoDB, PostgreSQL, Redis, Kafka, FCM) lives in this one module,
distinguished by a `<concern>.<backend>.go` file-naming convention:

```
devicestore.mongo.go       devicestore.postgres.go
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

**Every gourdian25 library now shares this shape.** grnoti was the first
built this way; `grcache` and `graudit` used to split one subpackage per
backend (e.g. `grcache/redis`, `grcache/postgres`) specifically to keep
unused backend drivers out of a consumer's dependency graph, and have
since flattened to match. The cost is accepted on purpose — importing
`grnoti` at all pulls in the MongoDB driver, pgx/v5 + sqlc-generated
Postgres code, go-redis, sarama (Kafka), and the Firebase Admin SDK,
regardless of which backends a given deployment actually uses. The
tradeoff: a simpler package to navigate and import (one `import "grnoti"`,
not one per backend) at the cost of a heavier dependency graph for every
consumer.

## 2. Interfaces and backend matrix

Every capability is a small interface (`interfaces.go`), with 2-3
concrete implementations each:

| Interface           | In-memory (`memory.go`) | Cache-backed (`cache.*.go`) | Mongo | Postgres | Other |
|----------------------|:---:|:---:|:---:|:---:|---|
| `DeviceStore`          | ✓ | — | ✓ | ✓ | |
| `PreferencesStore`     | ✓ | ✓ (read-through) | ✓ | ✓ | |
| `DLQHandler`           | ✓ | — | ✓ | ✓ | |
| `ExperimentStore`      | ✓ | — | — | ✓ | |
| `ExperimentEngine`     | — | ✓ (`cacheExperimentEngine`) | — | — | `deterministicExperimentEngine` (in-process map) |
| `IdempotencyStore`     | — | ✓ | — | — | thin adapter over any `grcache.Cache` |
| `RateLimiter`          | — | — | — | — | `localRateLimiter` (per-process) / `redisRateLimiter` (distributed) |
| `PushDispatcher`       | — | — | — | — | `fcmDispatcher` (the only real implementation; FCM is the notification backend) |
| `EventConsumer`/`EventProducer`/`AnalyticsPublisher` | — | — | — | — | Kafka only (`consumer.kafka.go`/`producer.kafka.go`) |
| `TemplateEngine`       | `defaultTemplateEngine` (in-process) | — | — | — | `localizedTemplateEngine` decorator (locale-aware, wraps another `TemplateEngine`) |
| `TopicRouter`          | — | — | — | — | `eventTypeTopicRouter` / `staticTopicRouter` / `deviceOnlyRouter` — all pure in-process logic |
| `CircuitBreaker`       | — | — | — | — | `standardCircuitBreaker` — the one implementation; `CircuitBreaker` is itself an exported interface, and both constructors (`NewCircuitBreaker`, `NewCircuitBreakerWithConfig`) return that interface type, not the concrete struct (see §3.2 for how it's wired into dispatch) |

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
it's `ProcessEvent` with the `ProcessingResult` discarded. Without the
queue, an ingestion burst would have nowhere to go but straight into
synchronous processing.

`EventProducer` (`NewKafkaEventProducer`) is the upstream half, for the
services that emit events. It runs `Event.Validate` on every event before
sending anything (a batch with one invalid event sends none), encodes
with `encoding/json`, the format `ConsumeClaim` decodes, and keys each
message by `EventID`. A consumer therefore never receives an event that
fails `Event.Validate`. Since an event is keyed by its own ID,
Kafka keeps no ordering between events; `IdempotencyStore` dedupes a
re-published one.

### 3.2 RateLimiter/CircuitBreaker are actually wired into dispatch

`FCMDispatcherDeps.RateLimiter`/`CircuitBreaker`, when set, gate every
outbound FCM batch/single-send (`sendBatch`, `SendToDevice`, `SendToTopic`
in `dispatcher.fcm.go`). Both sit on the actual dispatch path, not
beside it: a component tested only in isolation would never trip the
breaker or rate-limit anything during a real FCM outage.

### 3.3 Two independent batching layers that compose, not conflict

`dispatcher.fcm.go` always internally batches at `FCMMaxBatchSize` (FCM's
own per-multicast-call limit) — this is not configurable and not
optional. `service.go`'s `ServiceConfig.EnforceBatching`/
`MaxDevicesPerBatch` is a separate, optional, caller-controlled pre-split
of a resolved device list into smaller `PushDispatcher.Send` calls before
FCM's own batching ever sees them. The two are orthogonal: enabling the
service-level one just means more, smaller calls into a dispatcher that
still batches internally at its own fixed limit.

### 3.4 `RateLimiter` has no `Reserve()`

This package's `RateLimiter` interface deliberately does not expose
`Reserve() *rate.Reservation` — that would leak a
`golang.org/x/time/rate`-specific type through the interface, which
`redisRateLimiter` (the distributed variant, backed by a Lua token-bucket
script, not `x/time/rate` at all) has no equivalent for. `Allow`/`Wait` are
sufficient for every real caller in this codebase.

### 3.5 `ExperimentEngine` split: stateless assignment + a separate store

Variant assignment (`deterministicPick` in `experiment.go`) is a pure,
deterministic function of `(userID, experiment.ID, experiment.Variants)` —
the same inputs always produce the same variant, with or without an
assignment cache in front of it. This is split from `ExperimentStore`
(Postgres/in-memory CRUD for `Experiment` definitions themselves), so
"define an experiment" and "remember who got which variant" are never
conflated in one mutable-map type. Two
`ExperimentEngine` implementations exist purely as a performance choice
for where the assignment cache lives: `deterministicExperimentEngine`
(an in-process map guarded by a `sync.RWMutex`) for single-instance
deployments, and
`cacheExperimentEngine` (backed by any `grcache.Cache`, e.g. one from
`grcache.NewRedisCache`) so multiple service instances share one assignment
cache instead of each independently recomputing and caching — both
produce the identical variant for a given input regardless, since
assignment is deterministic; the cache is purely an optimization, never a
correctness dependency.

### 3.6 `DLQHandler.ClaimRetryableEvents`: atomic claim, not a plain read

A plain read with no claim semantics would let two concurrent workers
read and retry the same failed event simultaneously.
`ClaimRetryableEvents` (named to make the contract explicit) atomically
transitions
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
public API for an external retry-worker process to call periodically.
`service.go`'s own wiring
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
`grcache.Cache`, rather than separate hand-written Redis and Mongo clients
for each concern. Backend
choice for these three concerns becomes "which `grcache.Cache` the caller
constructs and passes in" (`grcache.NewRedisCache`,
`grcache.NewMemoryCache`, ...), not
"which grnoti-specific store type to construct." A cache read/write
failure always degrades to hitting the durable store directly (for
preferences) or is surfaced as an error the caller can log (for
idempotency) — never a silent correctness gap.

### 3.9 Distinct sentinel errors, not reused ones

`ErrNoTargetSpecified`, `ErrExperimentNotFound`, and
`ErrExperimentHasNoVariants` are each their own sentinel. Reusing
`ErrInvalidUserID` for the semantically different "event has no target at
all" condition, or `ErrTemplateNotFound` for both "no experiment with this
ID" and "experiment has no variants", would collapse unrelated failure
modes into one error a caller's
`errors.Is` check couldn't distinguish.

### 3.10 Templates: no hardcoded scheme, compiled once per registration

`NewTemplateEngine()`'s built-in defaults are deliberately scheme-free and
deep-link-free, so no specific application's URL scheme is baked into a
generic library default. Consumers register their own application-specific templates (including
their own deep-link scheme) via `RegisterTemplate`.
`localizedTemplateEngine` (the locale-aware decorator, `localization.go`)
compiles a localized `MessageTemplate` once, at registration time, and
reuses the compiled form on every `BuildMessage` call, rather than
building and re-registering a whole `TemplateEngine`, all default
templates included, per call just to render one localized message.

### 3.11 FCM error classification: two-tier, no per-code sentinels

`classifyFCMError` (`dispatcher.fcm.go`) classifies a raw FCM SDK error
into a small `FCMErrorCode` enum using a two-tier approach. First,
`classifyFCMErrorStructured` tries the FCM Admin SDK's own structured
`messaging.IsX(err)` checks (e.g. `messaging.IsUnregistered`,
`messaging.IsInternal`) — these type-assert against the SDK's own
unexported internal error type, populated by its HTTP layer from a
`messagingErrorCode` field parsed out of the real FCM response body, and
are what real production errors from `client.Send`/
`client.SendEachForMulticast` actually are. That type is unexported (Go's
`internal/` package rules), so this repo's own fake `FCMClient`
(`dispatcher.fcm_test.go`, the one deliberate exception to the
real-services testing policy since FCM has no local emulator) can never
construct one — meaning the structured path runs in production but has no
dedicated test in this repo. When no structured check matches,
classification falls through to substring-matching the error text, kept
as the fallback since it's what classifies errors from the fake client in
tests, and any other error not shaped like the SDK's internal type (e.g.
wrapped/decorated errors). There are deliberately no sentinels per FCM
error code in `errors.go` (`ErrFCMClientNil` is a construction error, not
a delivery one): `FCMError.Code` is the one taxonomy, so a second set of
sentinels would only drift from it.

### 3.12 Postgres via pgx/v5 + sqlc, not GORM

Every Postgres-backed store (`internal/postgresdb`, generated by sqlc from
`internal/postgresdb/queries/*.sql`) uses `pgx/v5` directly rather than
GORM — a pivot made mid-build after starting with GORM as originally
planned. `postgres.go` holds the shared connection/pooling logic
(`connectPostgres`, `PostgresConfig`) every Postgres store's own
constructor calls into, plus the embedded schema (`//go:embed
internal/postgresdb/schema.sql`) — grnoti has one linear schema and no
migration tool of its own. `connectPostgres` never applies this schema
itself (see below); the exported `SchemaSQL()` returns it as text for the
consuming application to apply through its own migration pipeline.

Each store's constructor originally dialed its own `*pgxpool.Pool` from
`PostgresConfig.DSN`, with no way to share one pool across stores — fine
in isolation, but a real backend wiring all four Postgres stores together
would open four independent pools (and four independent schema-apply
round-trips) against the same database. `PostgresConfig.Pool` now lets a
caller inject an already-built `*pgxpool.Pool` instead of supplying a DSN;
`connectPostgres` tracks whether it dialed the pool itself or received it
externally (`ownsPool`), and every store's `Close()` only closes the pool
when it dialed it — an injected, shared pool is never grnoti's to close.
The same rule now covers every injected connection:
`RedisRateLimiterConfig.Client` (exactly one of `Addr` or `Client`,
tracked by `redisRateLimiter.ownsClient`) and
`KafkaProducerConfig.Producer` (always injected, so
`kafkaEventProducer.Close` never closes it).
See [docs/postgres.md](postgres.md) for the full pattern (this is
documentation, not a new exported connection-builder function — see that
doc's own reasoning for keeping the pool-construction snippet
application-side rather than growing grnoti's API surface for it).

An earlier iteration of this package had `connectPostgres` apply the
embedded schema (`CREATE TABLE/INDEX IF NOT EXISTS`) on every connect —
wrapped in a Postgres advisory lock (`grnotiSchemaLockKey`, a fixed
constant, via `applyPostgresSchema`) so concurrent connects (multiple
stores constructed from goroutines, or multiple service replicas racing
on first boot) serialized instead of racing on catalog DDL, with
`PostgresConfig.SkipSchemaEnsure` as a per-store opt-out (both removed
in v0.2.0). That required
the runtime connection's role to have `CREATE` on the target schema,
which a deliberately least-privilege application role (a common
production setup — a separate role owns migrations, the app connects
with a DML-only role) won't have, failing construction with `permission
denied for schema ...`. `connectPostgres` now never applies schema at
all — see [docs/postgres.md](postgres.md) for the current pattern and the
full "why no auto-apply" reasoning. `applyPostgresSchema`/
`grnotiSchemaLockKey` still exist, unexported, used only by grnoti's own
test setup now (`ensureTestPostgresSchema` in
`devicestore.postgres_test.go`) — the advisory lock still matters there,
since multiple test files apply schema against the same database.

### 3.13 FCM addressing: Firebase Installation IDs only

FCM is moving from registration tokens to Firebase Installation IDs
(FIDs): firebase-admin-go deprecates `messaging.Message.Token` and
`MulticastMessage.Tokens` in favour of `Fid`/`Fids`, and the client SDKs
replace `getToken()`/`onNewToken()` with `register()`/`onRegistered`
(Android firebase-messaging 25.1.0+, Apple SDK 12.16.0+ behind an
`Info.plist` flag, the Web JS SDK). grnoti addresses FCM by FID only, so
the record is a `Device` keyed by `FID` (`devicestore.*.go`, table
`grnoti_devices`, column `fid`), and `dispatcher.fcm.go` sends with
`Fids`/`Fid` alone.

Keeping registration tokens alongside was rejected. The SDK offers no
non-deprecated way to send to one: `Fid` takes an installation ID, a
different value, and Firebase's own integration test shows a non-FID sent
there answered `UNREGISTERED` (Firebase documents only the reverse, `token`
accepting an FID during the migration). Supporting tokens would have meant
keeping the deprecated fields, with their lint suppressions, until Firebase
removes them.

That `UNREGISTERED` answer is also why `Device.Validate` exists. FCM's
reply is classified permanent and the FID is marked invalid, so an app still
on the token APIs would have every push fail and its devices silently
pruned. Instead, every `DeviceStore.SaveDevice` and `Event.Validate` refuse
anything that is not FID-shaped with `ErrInvalidFID`: `validFID` is the
Firebase JS SDK's own `VALID_FID_PATTERN`, 22 URL-safe base64 characters
whose first is `c`, `d`, `e` or `f` (the 0b0111 FID header). A registration
token is far longer and never matches. The `fid` column keeps its generous
width on purpose: the format check lives in Go alone, so a change to it
never needs a schema migration in a consumer.

### 3.14 The consumer never commits past a message it didn't handle (v0.6.0)

A consumer group commits one offset per partition, so marking message n + 1
commits every message before it. v0.5.x marked a message that wouldn't decode
and skipped one whose handler failed, and the next message's mark then
committed past it: both were lost for good. Since v0.6.0 each message is:

1. decoded; one that won't decode never will, so it skips step 2;
2. handled, and handled again with backoff while it fails
   (`KafkaConsumerConfig.Retry`, `DefaultConsumerRetryPolicy`: five attempts,
   500 ms doubling);
3. if it still fails, written to `KafkaDLQTopic(<topic>)` (`<topic>.dlq`) as
   it came, with `grnoti-error`, `grnoti-original-topic`, `-partition`,
   `-offset` and `-attempts` headers, through the caller's `DLQProducer`
   (required, never closed by grnoti);
4. only then marked.

A dead-letter write that fails stops the partition: the session ends with
nothing marked past the message, and after `HaltPause` a new one starts from
the committed offset. A session that ends mid-retry leaves the message
unmarked. A handler answering `ErrClosed` (the service has shut down) leaves
it unmarked and ends `Start` with that error, rather than rejoining to be
refused in a loop. A handler's panic is a failure; its type alone is logged.
Delivery is at least once, so a handler is idempotent (`NotificationService`
is, by `EventID`).

This is a Kafka dead-letter topic, not `DLQHandler`: `DLQHandler` stores an
`Event` whose delivery failed, while a message that won't decode has no
`Event` at all; the topic takes the bytes as they came.

`NewKafkaAnalyticsPublisher`'s default producer is idempotent since v0.6.0
(`Producer.Idempotent`, one request in flight, `WaitForAll`; v0.5.x acked
`WaitForLocal`).

### 3.15 `SkipIndexCreation`: when migrations own the collections (v0.6.0)

Each Mongo store (`MongoDeviceStoreConfig`, `MongoPreferencesStoreConfig`,
`MongoDLQHandlerConfig`) creates its indexes on connect by default. A
deployment whose migrations own the collections, and whose application user
has no `createIndex` (grsentry's), sets `SkipIndexCreation`: the constructor
then creates nothing and checks through `listIndexes` that every index it
relies on exists (`ensureMongoIndexes`, `indexes.mongo.go`): the same keys in
the same order, the same uniqueness and the same TTL, whatever its name. One
missing or weaker is `ErrMongoIndexMissing`, so a uniqueness a store relies
on (one device per FID, one preferences document per user, one DLQ record
per event) is never silently absent. The user then needs `listIndexes`.

## 4. Testing philosophy

Every backend is tested against a real local instance — Docker containers
for MongoDB, PostgreSQL, Redis, and Kafka (KRaft-mode, no Zookeeper), shared
with every gourdian25 repo and built per grsentry's `docker-compose.yml`
(`make docker-up`) — not mocks. **FCM is the one deliberate exception**, via a hand-rolled fake
`FCMClient` (`dispatcher.fcm_test.go`), since Firebase has no local
emulator to run against. This is a stronger bar than typical
interface-mocking unit tests, and it caught real, previously-latent bugs
that code review alone did not — most notably:

- `eventTypeTopicRouter`/`deviceOnlyRouter`'s device-resolution fallback
  silently dropped anonymous and direct-FID events when topic routing
  was enabled — invisible until an actual end-to-end pipeline test wired
  a real `TopicRouter` against a real `DeviceStore`.
- `sendBatchWithRetry` (`dispatcher.fcm.go`) only retried on a partial
  per-FID failure, silently abandoning a *total* request-level failure
  after one attempt — only visible once a fake client was made to return
  a real total-failure error across a real retry loop.

Concurrency is held to the same bar: the experiment-assignment map is
shared by concurrent workers, so `experiment.go` locks it from the start,
and `TestDeterministicExperimentEngine_ConcurrentAssignVariant` runs real
concurrent goroutines under `-race` to prove it never races — the kind of
guarantee a single-threaded review can't provide either way.

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

- Kafka's `Start`'s non-`closed` error-return branch and the
  `consumerGroup.Close()`/`producer.Close()` error-wrap branches — both
  `consumerGroup`/`producer` are stored as sarama's own interface types
  (`sarama.ConsumerGroup`/`sarama.SyncProducer`), so a same-package test
  could substitute hand-rolled fakes without any production-code change,
  but building those fakes is real, standalone test infrastructure —
  deliberately deferred to a separate follow-up rather than bundled with
  smaller changes. (`WaitReady`'s own `ctx.Done()` branch, previously
  listed alongside these, is now covered by
  `TestKafkaEventConsumer_WaitReady_ContextCanceled` — no fake needed
  there, since it's a pre-canceled-context call with no timing window.)
- `dlq.mongo.go`'s generic mid-loop claim error — needs Mongo-level fault
  injection distinct from the connectivity/empty-config branches that are
  covered. A real test is possible (the same top-level `ctx` is reused
  unchanged across every claim iteration, so canceling it after the first
  iteration's real round-trip completes but before the second's would
  plausibly reproduce this against the real local Mongo instance, no mock
  needed) but is a genuine network-timing race with no iteration-boundary
  hook in production code to pin it deterministically — judged not worth
  the flakiness risk against this repo's own `make coverage-check`/CI gate
  for one error-wrap line of coverage. Left undone.
- Two structurally-unreachable defensive branches kept for future-proofing
  rather than removed: `service.go`'s `NewNotificationService` "build
  worker pool" error wrap, and `payloadvalidator.go`'s `EstimateSize`
  JSON-marshal-failure fallback.
