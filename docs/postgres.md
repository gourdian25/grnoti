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

## `Close()` ownership

grnoti never closes a pool it didn't create. Each store's `Close()` only
calls `pool.Close()` when that store dialed the pool itself from `DSN`.
When you inject `Pool`, calling `Close()` on any (or every) store built
from it is a no-op with respect to the pool — closing it is entirely your
backend's job, typically once at shutdown, after every store using it is
done.

## Applying the schema — required, and always your job

None of `NewPostgresTokenStore`, `NewPostgresPreferencesStore`,
`NewPostgresExperimentStore`, or `NewPostgresDLQHandler` ever run `CREATE
TABLE`/`CREATE INDEX` — grnoti has no migration tool of its own and
doesn't try to be one. The four tables each one queries (`grnoti_tokens`,
`grnoti_preferences`, `grnoti_experiments`, `grnoti_dlq` — one shared
`internal/postgresdb/schema.sql`, not four separate schemas) must already
exist before you construct any of them, or every real store call
(`SaveToken`, `GetPreferences`, `ClaimRetryableEvents`, ...) fails with a
plain Postgres "relation does not exist" error — construction itself
still succeeds, since it only pings the pool.

Call `grnoti.SchemaSQL()` to get the exact schema as text, and apply it
through whatever migration tool your own project already uses:

```go
fmt.Println(grnoti.SchemaSQL())
```

Paste that into a new migration file (golang-migrate, Flyway, a plain
`.sql` file run once in CI, whatever you already have) and run it with
your project's normal migration process, once, before your application
first constructs any of these four stores — regardless of whether you
give each store its own `DSN` or share one `Pool` across all four, per
"Why share one pool" above.

### Why no auto-apply

An earlier iteration of this package had every `New*Postgres*` constructor
apply its schema automatically on every connect, with a
`PostgresConfig.SkipSchemaEnsure` flag to opt individual stores out of it.
That works fine as long as the pool's connection role has `CREATE` on the
target schema — but a common, deliberate production setup is the
opposite: one Postgres role owns migrations (`CREATE`/`ALTER`/`DROP`),
while the application's own runtime connection uses a separate,
least-privilege role with only `SELECT`/`INSERT`/`UPDATE`/`DELETE` on
already-existing tables. Against a role like that, auto-apply failed
loudly at construction with `permission denied for schema ...` — and
pre-creating the tables through some other path didn't help either, since
`CREATE TABLE IF NOT EXISTS` still checks `CREATE` privilege before
checking whether the table exists, so the attempt itself failed every
time, not just the first.

Rather than keep the opt-out flag, grnoti simply never auto-applies
anymore: you always apply the schema yourself, through your own project's
existing migration pipeline, with whatever role already owns that
pipeline. This keeps grnoti's runtime connection requirements identical
regardless of how your application's own roles are set up, and means
there's exactly one way this works, not a default plus an exception to
remember.

### What `SchemaSQL()` does *not* do

It only ever adds (`CREATE ... IF NOT EXISTS`). There's no down-migration,
no versioning, and no support for evolving the schema beyond that — an
`ALTER TABLE`, a column type change, or a backfill is entirely your own
migration tool's responsibility. If you upgrade grnoti and its schema
changes, `CHANGELOG.md` documents it; re-sync your vendored copy by hand
the same way you'd pick up any other dependency's breaking schema change.

## `ConnectTimeout`

Only relevant in `DSN` mode: bounds dialing and the initial `Ping`.
Defaults to 10 seconds (matching grnoti's previous hardcoded behavior) if
left at zero. Ignored when `Pool` is set — pinging an already-established
pool always uses the 10-second default regardless.
