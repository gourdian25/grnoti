// File: postgres.go

package grnoti

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gourdian25/grnoti/internal/postgresdb"
)

// pgTimestamptz and pgTime convert between time.Time and pgtype.Timestamptz
// — shared by every Postgres store's row<->domain mapping.
func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()}
}

func pgTime(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
}

// pgInt32 converts a caller-supplied int (e.g. a MaxRetries config value or
// a ClaimRetryableEvents limit) to the int32 every generated query param
// expects, clamping instead of silently wrapping on overflow — an actual
// bounds check rather than a lint suppression, since these values do
// originate from caller-supplied config/arguments, not internal constants
// with a provably-safe range.
func pgInt32(n int) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n)
	}
}

// postgresSchemaSQL is internal/postgresdb/schema.sql, embedded so it can
// be returned by SchemaSQL() and applied by grnoti's own test setup. None
// of the four NewPostgres* constructors apply this themselves — see
// SchemaSQL's own doc comment for why. The query code itself is generated
// by sqlc from the same directory (see internal/postgresdb) into its own
// internal package — standard Go practice for isolating generated code
// from hand-written code, and distinct from the subpackage-per-backend
// pattern grnoti deliberately avoids (see docs/plan/grnoti-plan.md §4):
// internal/ packages aren't importable outside this module at all, so
// this isn't a public API surface the way grcache/redis or grcache/postgres
// are.
//
//go:embed internal/postgresdb/schema.sql
var postgresSchemaSQL string

// SchemaSQL returns grnoti's Postgres schema (plain CREATE TABLE/INDEX IF
// NOT EXISTS, no grnoti-specific magic) as text, for the caller to apply
// through their own project's migration tool (golang-migrate, Flyway, a
// plain SQL file run in CI, whatever you already use) before constructing
// any of grnoti's four Postgres-backed stores:
//
//   - grnoti_tokens      — NewPostgresTokenStore
//   - grnoti_preferences — NewPostgresPreferencesStore
//   - grnoti_experiments — NewPostgresExperimentStore
//   - grnoti_dlq         — NewPostgresDLQHandler
//
// grnoti deliberately never applies its own schema at runtime: doing so
// requires the runtime connection's role to have CREATE on the target
// schema, which a deliberately least-privilege application role (a common
// production setup — a separate role owns migrations, the app connects
// with a DML-only role) won't have. Vendor this text into your own
// migration once; re-sync it by hand (see CHANGELOG.md) if you upgrade
// grnoti and its schema changes. See docs/postgres.md for the full
// pattern, including how to share one pool across all four stores.
func SchemaSQL() string {
	return postgresSchemaSQL
}

// grnotiSchemaLockKey is a fixed Postgres advisory-lock key used by
// applyPostgresSchema to serialize schema application across concurrent
// callers — e.g. multiple of grnoti's own test files applying schema
// against the same database at once (see
// TestConnectPostgres_ConcurrentSchemaApplyDoesNotRace) — since concurrent
// CREATE TABLE/INDEX IF NOT EXISTS statements are not fully race-free in
// Postgres. Advisory locks are global to the database, not namespaced per
// application, so an unrelated app that happens to choose this same
// int64 key could in principle collide with it — an accepted, extremely
// unlikely risk, not worth a namespacing scheme for a single fixed lock.
// Distinct from gourdiantoken's own lock key (8_314_672_205), since the
// two libraries may share a database.
const grnotiSchemaLockKey int64 = 7_927_100_419

// PostgresConfig is the common connection configuration shared by every
// Postgres-backed store.
type PostgresConfig struct {
	// DSN is a standard libpq/pgx connection string. Exactly one of DSN
	// or Pool must be set.
	DSN string
	// Pool, if set, is used directly instead of dialing a new pool from
	// DSN — lets multiple grnoti Postgres stores (or the rest of your
	// backend) share one pgxpool.Pool instead of each store opening its
	// own. grnoti never closes a Pool it did not create itself: every
	// store's Close() only closes the pool when it was dialed from DSN.
	// Exactly one of DSN or Pool must be set. See docs/postgres.md for
	// the recommended shared-pool pattern.
	Pool *pgxpool.Pool
	// MaxConns caps the pgxpool connection pool size. 0 means use pgxpool's
	// own default. Ignored when Pool is set — tune the pool yourself
	// before passing it in.
	MaxConns int32
	// MinConns keeps at least this many connections open. 0 means use
	// pgxpool's own default. Ignored when Pool is set.
	MinConns int32
	// MaxConnLifetime bounds how long a pooled connection may be reused
	// before being recycled. 0 means pgxpool's own default (unlimited).
	// Ignored when Pool is set.
	MaxConnLifetime time.Duration
	// ConnectTimeout bounds dialing and the initial Ping when connecting
	// from DSN. 0 means 10 seconds. Ignored when Pool is set (the Ping
	// against an already-established Pool uses the 10-second default
	// unconditionally).
	ConnectTimeout time.Duration
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

// connectPostgres resolves cfg to a pgxpool.Pool — either dialing a new
// one from cfg.DSN or reusing cfg.Pool directly — validates connectivity
// via Ping, and returns whether the pool is owned by this call: true only
// when dialed from DSN here, false when cfg.Pool was supplied externally,
// since an externally-supplied pool is never grnoti's to close. Shared by
// every Postgres store's own constructor.
//
// This never applies grnoti's schema — see SchemaSQL's own doc comment
// for why. The tables the returned *postgresdb.Queries queries must
// already exist before this is called, applied through your own
// project's migration tool. Calling this against a database where they
// don't exist yet doesn't fail here — it fails on the first actual store
// method call (SaveToken, GetPreferences, ClaimRetryableEvents, ...) with
// a plain Postgres "relation does not exist" error.
func connectPostgres(ctx context.Context, cfg PostgresConfig, component string) (*pgxpool.Pool, *postgresdb.Queries, bool, error) {
	if (cfg.DSN == "") == (cfg.Pool == nil) {
		return nil, nil, false, fmt.Errorf("grnoti/postgres: exactly one of DSN or Pool is required for %s", component)
	}

	if cfg.Pool != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := cfg.Pool.Ping(pingCtx); err != nil {
			return nil, nil, false, fmt.Errorf("grnoti/postgres: ping: %w", errors.Join(err, ErrBackendUnavailable))
		}
		return cfg.Pool, postgresdb.New(cfg.Pool), false, nil
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, nil, false, fmt.Errorf("grnoti/postgres: parse DSN: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}

	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(connectCtx, poolCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("grnoti/postgres: connect: %w", errors.Join(err, ErrBackendUnavailable))
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, nil, false, fmt.Errorf("grnoti/postgres: ping: %w", errors.Join(err, ErrBackendUnavailable))
	}

	return pool, postgresdb.New(pool), true, nil
}

// applyPostgresSchema applies the embedded schema (CREATE TABLE/INDEX IF
// NOT EXISTS) against pool, serialized by a Postgres session-level
// advisory lock (grnotiSchemaLockKey) so concurrent callers apply it one
// at a time instead of racing on catalog DDL. The lock/exec/unlock
// sequence runs on a single acquired connection, since advisory locks are
// session-scoped, and is unlocked explicitly before the connection is
// released back to the pool — a released pooled connection is reused,
// not reset, so the lock would otherwise leak onto whichever caller
// acquires that connection next.
func applyPostgresSchema(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// The two Exec error branches below (lock acquisition, schema apply)
	// are only reachable if the connection breaks between Acquire
	// succeeding and the respective Exec — not deterministically
	// triggerable against a live Postgres without a flaky timing race, so
	// they're accepted as untested rather than force-covered (see
	// TestApplyPostgresSchema_AcquireFailsOnClosedPool for the one branch
	// here that is triggered deterministically).
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", grnotiSchemaLockKey); err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", grnotiSchemaLockKey)
	}()

	if _, err := conn.Exec(ctx, postgresSchemaSQL); err != nil {
		return err
	}
	return nil
}
