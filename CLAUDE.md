# CLAUDE.md

Developer setup and working conventions for grnoti — for both human
contributors and Claude Code sessions working in this repo.

## What this is

A push-notification service library for the gourdian ecosystem
(`github.com/gourdian25/grnoti`). Single flat Go package, no subpackages
(except the unexported, sqlc-generated `internal/postgresdb`) — see
[docs/architecture.md](docs/architecture.md) for why, and
[docs/plan/grnoti-plan.md](docs/plan/grnoti-plan.md) for the full
stage-by-stage build history.

## Architecture at a glance

Full rationale lives in [docs/architecture.md](docs/architecture.md) — this
is the condensed version so you don't have to open it for routine changes.

**File naming**: every backend for a given concern is
`<concern>.<backend>.go` with tests in `<concern>.<backend>_test.go` (e.g.
`tokenstore.mongo.go`, `dlq.postgres.go`, `ratelimiter.redis.go`). In-memory
variants of every store live together in `memory.go`; `grcache`-backed
adapters (idempotency, preferences read-through, experiment-assignment
cache) live in `cache.*.go`. This is a deliberate divergence from sibling
repos (`grcache`, `graudit`) that use one subpackage per backend to keep
unused drivers out of a consumer's dependency graph — grnoti accepts a
heavier import (Mongo driver, pgx, go-redis, sarama, Firebase Admin SDK,
regardless of which backends you actually use) for a simpler, flat package.

**Every capability is a small interface** (`interfaces.go`) with 2-3
concrete implementations; `NotificationService` (`service.go`) is the
orchestrator that composes them all via `ServiceDeps`. The interface list
and its full in-memory/cache/Mongo/Postgres/other matrix is in
docs/architecture.md §2 — key ones to know before touching related code:
`TokenStore`, `PreferencesStore`, `DLQHandler`, `ExperimentStore` +
`ExperimentEngine` (assignment is a pure deterministic function, split
from the store — see §3.5), `IdempotencyStore`, `RateLimiter`,
`PushDispatcher` (FCM only), `EventConsumer`/`AnalyticsPublisher` (Kafka
only), `TemplateEngine`, `TopicRouter`, `CircuitBreaker`.

**Load-bearing design decisions** (each has a full writeup in
docs/architecture.md §3 — read the relevant one before changing the file
it names):

- `EventConsumer.Start(ctx, service.Submit)` is the entire Kafka-to-processing
  wiring — the two signatures match on purpose (§3.1).
- `RateLimiter`/`CircuitBreaker`, when set in `FCMDispatcherDeps`, actually
  gate every outbound FCM call in `dispatcher.fcm.go` (§3.2) — don't build a
  new dispatch path that bypasses them.
- `DLQHandler.ClaimRetryableEvents` atomically claims (not just reads)
  retryable events; MongoDB's per-document claim loop can return a
  **non-nil slice alongside a non-nil error** and callers must still
  process it (§3.6, doc comment in `interfaces.go`).
- A backend-native error must never leak through a grnoti interface
  unwrapped — translate to a sentinel in `errors.go` first (see
  Conventions below).
- Postgres stores use `pgx/v5` + sqlc-generated code directly, not GORM
  (§3.12).
- Every Postgres store's `PostgresConfig` accepts either `DSN` (dials its
  own pool) or `Pool` (reuses an externally-supplied `*pgxpool.Pool` —
  the shared-pool pattern for wiring multiple stores off one backend
  connection pool). `Close()` only closes a pool the store dialed itself
  — never one passed in via `Pool`. See [docs/postgres.md](docs/postgres.md).

## Testing philosophy: real local services, not mocks

Every backend (MongoDB, PostgreSQL, Redis, Kafka) is tested against a real
local instance, not a mock — this has caught real bugs that mocked tests
would have missed (see docs/architecture.md §4). **FCM is the one
deliberate exception**: it has no local emulator, so `dispatcher.fcm_test.go`
uses a hand-rolled fake `FCMClient` instead. Don't add mocks for the other
four backends; if a test needs one, something's wrong with the approach.

Tests skip gracefully (`t.Skip`, not fail) when a backend isn't running
locally, so `go test .` works even without any containers up — but the
tests that matter for a given change need the real thing.

### Starting the backends

These containers are **shared across the whole gourdian25 workspace** (grnoti,
graudit, grcache, gourdiantoken all test against the same running Postgres/
Redis/Mongo/Kafka/Memcached instances, each using its own database/keyspace/
DB-index) — not grnoti-exclusive despite the `gourdian-*` naming below.

```sh
docker run -d --name gourdian-postgres -p 5432:5432 \
  -e POSTGRES_USER=postgres_user \
  -e POSTGRES_PASSWORD=postgres_password \
  -e POSTGRES_DB=grnoti_test \
  postgres:16
# other repos' own databases (graudit_test, grcache_test, gourdiantoken_test)
# are created the same way inside this same container — see each repo's own
# CLAUDE.md.

docker run -d --name gourdian-redis -p 6379:6379 \
  redis:7 --requirepass redis_password
# grnoti uses DB 0 (the default); grcache uses DB 14, gourdiantoken uses DB 15.

# Mongo requires a keyFile when --replSet is combined with auth, even for a
# single-node set — see docs/postgres.md-equivalent Mongo note, or graudit's
# CLAUDE.md for the full keyfile-generation steps.
docker volume create gourdian-mongo-keyfile
docker run --rm -v gourdian-mongo-keyfile:/keyfile-dir mongo:7 bash -c \
  "openssl rand -base64 756 > /keyfile-dir/mongo-keyfile && chmod 400 /keyfile-dir/mongo-keyfile && chown 999:999 /keyfile-dir/mongo-keyfile"
docker run -d --name gourdian-mongo-auth -p 27018:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=mongo_password \
  -v gourdian-mongo-keyfile:/etc/mongo-keyfile-dir \
  mongo:7 --replSet rs0 --keyFile /etc/mongo-keyfile-dir/mongo-keyfile
docker exec gourdian-mongo-auth mongosh -u root -p mongo_password \
  --authenticationDatabase admin --eval 'rs.initiate()'

# KRaft mode, single broker, no Zookeeper needed
docker run -d --name gourdian-kafka -p 9092:9092 apache/kafka:3.7.0
```

Postgres needs no separate migration step: every Postgres-backed store's
constructor applies `internal/postgresdb/schema.sql` via `CREATE TABLE IF
NOT EXISTS` on connect (see `connectPostgres` in `postgres.go`), guarded
by a Postgres advisory lock so concurrent connects don't race on the DDL
— opt out per store with `PostgresConfig.SkipSchemaEnsure` if you manage
this schema through your own migration pipeline instead (see
[docs/postgres.md](docs/postgres.md)). Mongo
indexes are similarly ensured on connect by each Mongo store's own
constructor.

Connection details the tests expect (see `const test*` in each `*_test.go`
file if these ever drift):

| Backend  | Value |
|----------|-------|
| Mongo    | `mongodb://root:mongo_password@localhost:27018/?directConnection=true`, database `grnoti_test` — `replicaSet=rs0`/`authSource=admin` aren't needed on the connection string; `directConnection=true` alone is sufficient (confirmed empirically, matching graudit's/grcache's/gourdiantoken's own test URIs) |
| Postgres | `host=localhost user=postgres_user password=postgres_password dbname=grnoti_test port=5432 sslmode=disable` |
| Redis    | `localhost:6379`, password `redis_password`, DB 0 (the default) |
| Kafka    | `localhost:9092` |

### Scoping a test run to one backend while iterating

```sh
go test -run TestPostgres ./...       # every Postgres-backed test
go test -run TestMongo ./...          # every Mongo-backed test
go test -run TestRedisRateLimiter ./...
go test -run TestKafka ./...
go test -run TestTokenStore_Contract/Memory ./...   # one contract-test variant
```

## Everyday commands

```sh
make test        # go test -cover ./...
make race         # go test -race ./... — mandatory before any commit touching
                   # experiment.go, workerpool.go, dlq.*.go, or any store
make coverage-summary   # per-function coverage breakdown
make lint         # golangci-lint run ./...
make precommit    # fmt + vet + lint + race + coverage-check — run this before every commit
make prerelease   # precommit + goreleaser-check — run before `make tag`/`make release`
```

**Coverage note**: use `go test -cover .` (a single dot), not `go test
-cover ./...`. The latter also compiles+instruments `internal/postgresdb`
(no test file of its own, so it reports a flat 0%), which drags the
printed total down by several points and is not the number that reflects
this package's actual test coverage. `make coverage-check` already scopes
correctly; `go tool cover -func` output for `internal/postgresdb` lines
can be ignored.

## Regenerating sqlc code

```sh
sqlc generate
```

run from the repo root (reads `sqlc.yaml`, writes into
`internal/postgresdb/`). After regenerating, confirm the output still
starts with sqlc's own `// Code generated by sqlc. DO NOT EDIT.` marker —
see the next section for why that specifically matters here.

## The `bark` tool and generated code

This repo uses `bark` (config: `.bark.toml`) to keep a `// File: <path>`
header on every source file. `internal/postgresdb/**` is listed in
`.bark.toml`'s `[exclude].patterns` and must stay there: bark's header
would otherwise overwrite sqlc's `// Code generated by sqlc. DO NOT EDIT.`
first line, which is what golangci-lint (and every other Go tool) relies
on to recognize and skip generated code — this has actually happened
twice during development, both times reintroducing a batch of gosec
`G101` false positives on generated SQL text. If you ever see those
findings reappear, check `internal/postgresdb/*.go`'s first line before
anything else, and re-run `sqlc generate` to restore it.

## Conventions

- One flat package. New backend support for an existing concern goes in
  `<concern>.<backend>.go` (e.g. a hypothetical `tokenstore.dynamodb.go`),
  with its tests in `<concern>.<backend>_test.go`.
- Every source file starts with a `// File: <path>` comment (bark-managed,
  see above — don't remove it, and don't hand-maintain it either, bark
  does).
- Sentinel errors (`errors.go`) are used with `errors.Is`, never a
  per-error `IsX(err) bool` helper — matches every other gourdian repo.
- A backend-native error (`mongo.ErrNoDocuments`, `pgx.ErrNoRows`,
  `redis.Nil`, ...) must never leak through a grnoti interface unwrapped —
  translate it to a sentinel first, then wrap with `%w` if further context
  is needed.
- New tests follow the real-services rule above; use
  `time.Now().UnixNano()` to nonce any dynamic key/collection/topic name
  used against a real persistent backend — `t.Name()` alone collides
  across separate `go test` invocations against state that outlives the
  process.
