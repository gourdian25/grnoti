# grnoti

Push-notification service library for the gourdian ecosystem
(`github.com/gourdian25/grnoti`): FCM dispatch, idempotent event
processing, device-token management, durable dead-letter retry, circuit
breaking, distributed rate limiting, deterministic A/B experiment
assignment, localization, and topic-based routing, behind a set of
storage-agnostic interfaces.

Status: feature-complete per the 14-stage build plan
([docs/plan/grnoti-plan.md](docs/plan/grnoti-plan.md)), pre-tagged-release.
`golangci-lint run` reports 0 issues; test coverage is ~95%, verified
against real local MongoDB/PostgreSQL/Redis/Kafka instances (see
[CLAUDE.md](CLAUDE.md) for the docker setup).

## Install

```sh
go get github.com/gourdian25/grnoti
```

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

## Development

```sh
make docker-up   # start the shared Postgres/Redis/Mongo/Kafka test containers
make precommit   # fmt + vet + lint + race + coverage-check
make docker-down # stop them when you're done
```

These containers are shared with graudit, grcache, and gourdiantoken (each
gets its own database/keyspace) — see [CLAUDE.md](CLAUDE.md) for backend
setup, test scoping, and conventions.

## License

MIT — see [LICENSE](LICENSE).
