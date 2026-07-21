# Using grnoti's Postgres stores in a backend

This is the pattern for wiring `TokenStore`, `PreferencesStore`,
`ExperimentStore`, and `DLQHandler` (the four `NewPostgres*` constructors)
into a real backend service, where you want one connection pool shared
across all of them rather than each store dialing its own. It's a
companion to [architecture.md §3.12](architecture.md) (why pgx/sqlc, not
GORM) and the "Using real backends instead" comment in
[../example/main.go](../example/main.go).

## Why share one pool

Each `New*Postgres*` constructor takes a `PostgresConfig`. If you give each
one its own `DSN`, you get one `*pgxpool.Pool` per store — four separate
pools if you use all four backends, each with its own `MaxConns`. Set
`MaxConns: 10` per store and your service opens up to 40 real connections
to Postgres, not 10, plus four independent dial+ping round-trips at boot
instead of one.

`PostgresConfig.Pool` lets you build the pool once, in your own
backend's bootstrap code, and hand the same `*pgxpool.Pool` to every
store:

```go
package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool builds the one *pgxpool.Pool this service's grnoti stores (and
// any other Postgres access the backend needs) share. Plain pgxpool
// bootstrap — nothing grnoti-specific about it.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = time.Hour

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return pgxpool.NewWithConfig(ctx, poolCfg)
}
```

```go
pool, err := db.NewPool(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
defer pool.Close() // the backend owns this pool — see Close() semantics below

tokenStore, err := grnoti.NewPostgresTokenStore(grnoti.PostgresConfig{Pool: pool, Logger: logger})
preferencesStore, err := grnoti.NewPostgresPreferencesStore(grnoti.PostgresConfig{Pool: pool, Logger: logger})
experimentStore, err := grnoti.NewPostgresExperimentStore(grnoti.PostgresConfig{Pool: pool, Logger: logger})
dlqHandler, err := grnoti.NewPostgresDLQHandler(grnoti.PostgresDLQHandlerConfig{
	PostgresConfig: grnoti.PostgresConfig{Pool: pool, Logger: logger},
	MaxRetries:     5,
})
```

`PostgresConfig.DSN` and `PostgresConfig.Pool` are mutually exclusive —
set exactly one. `MaxConns`/`MinConns`/`MaxConnLifetime`/`ConnectTimeout`
only apply when connecting via `DSN`; tune the pool yourself before
passing it in via `Pool`.

Construct your Postgres stores **sequentially**, not concurrently
(e.g. not from an `errgroup`), even when sharing one pool. Schema
application is safe under concurrent connects now (see below), but
there's no benefit to parallelizing four calls that are dominated by the
same handful of round-trips against the same pool, and it removes any
temptation to fan out store construction elsewhere in the codebase where
schema application might not be lock-guarded (e.g. a future backend that
doesn't go through `connectPostgres` at all).

## `Close()` ownership

grnoti never closes a pool it didn't create. Each store's `Close()` only
calls `pool.Close()` when that store dialed the pool itself from `DSN`.
When you inject `Pool`, calling `Close()` on any (or every) store built
from it is a no-op with respect to the pool — closing it is entirely your
backend's job, typically once at shutdown, after every store using it is
done.

## Schema application and `SkipSchemaEnsure`

By default, every `New*Postgres*` call applies grnoti's embedded schema
(`internal/postgresdb/schema.sql`, plain `CREATE TABLE/INDEX IF NOT
EXISTS`) before returning — no separate migration step needed for a
first-time setup. This is now safe under concurrent connects: schema
application acquires a Postgres advisory lock first, so N stores
connecting at once (or N service replicas booting simultaneously against
a fresh database) serialize instead of racing on the same DDL.

If you inject one shared `Pool` into all four stores, each one still
independently re-applies the (now lock-guarded, so correct, just not
free) schema check by default. To skip the redundant round-trips, set
`SkipSchemaEnsure: true` on all but one store's config:

```go
tokenStore, err := grnoti.NewPostgresTokenStore(grnoti.PostgresConfig{Pool: pool})               // applies schema
preferencesStore, err := grnoti.NewPostgresPreferencesStore(grnoti.PostgresConfig{Pool: pool, SkipSchemaEnsure: true})
experimentStore, err := grnoti.NewPostgresExperimentStore(grnoti.PostgresConfig{Pool: pool, SkipSchemaEnsure: true})
```

### If you run your own migration pipeline

Set `SkipSchemaEnsure: true` on every store's config so grnoti never
touches schema in that environment. `internal/postgresdb/schema.sql` is
plain, idempotent DDL with no grnoti-specific magic — copy its statements
into your own migration tool (golang-migrate, Flyway, a plain SQL file
run in CI, whatever you already use) as a one-time "up" migration.

### What grnoti's schema handling does *not* do

It only ever adds (`CREATE ... IF NOT EXISTS`). There's no down-migration,
no versioning, and no support for evolving the schema beyond that — an
`ALTER TABLE`, a column type change, or a backfill is entirely your own
migration tool's responsibility, independent of and unaffected by
grnoti's auto-apply. If your application needs to evolve grnoti's tables
beyond what `schema.sql` defines, manage that through your own migrations
and set `SkipSchemaEnsure: true` everywhere once you do.

## `ConnectTimeout`

Only relevant in `DSN` mode: bounds dialing and the initial `Ping`.
Defaults to 10 seconds (matching grnoti's previous hardcoded behavior) if
left at zero. Ignored when `Pool` is set — pinging an already-established
pool always uses the 10-second default regardless.
