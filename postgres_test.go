// File: postgres_test.go

package grnoti

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPgInt32(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int32
	}{
		{"zero", 0, 0},
		{"typical maxRetries", 3, 3},
		{"typical claim limit", 100, 100},
		{"exactly MaxInt32", math.MaxInt32, math.MaxInt32},
		{"overflow clamps to MaxInt32", math.MaxInt32 + 1000, math.MaxInt32},
		{"negative passes through", -1, -1},
		{"exactly MinInt32", math.MinInt32, math.MinInt32},
		{"underflow clamps to MinInt32", math.MinInt32 - 1000, math.MinInt32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pgInt32(tc.in); got != tc.want {
				t.Fatalf("pgInt32(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestPgTime(t *testing.T) {
	if got := pgTime(pgtype.Timestamptz{Valid: false}); !got.IsZero() {
		t.Fatalf("pgTime(invalid) = %v, want the zero time.Time", got)
	}
}

func TestConnectPostgres_EmptyDSN(t *testing.T) {
	_, _, _, err := connectPostgres(context.Background(), PostgresConfig{}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(neither DSN nor Pool) = nil error, want non-nil")
	}
}

func TestConnectPostgres_MalformedDSN(t *testing.T) {
	_, _, _, err := connectPostgres(context.Background(), PostgresConfig{DSN: "not a valid dsn ::://"}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(malformed DSN) = nil error, want non-nil")
	}
}

func TestConnectPostgres_UnreachableHost(t *testing.T) {
	_, _, _, err := connectPostgres(context.Background(), PostgresConfig{DSN: "host=127.0.0.1 port=1 user=x dbname=x connect_timeout=1"}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(unreachable host) = nil error, want non-nil")
	}
}

func TestConnectPostgres_PoolTuningOverrides(t *testing.T) {
	pool, _, ownsPool, err := connectPostgres(context.Background(), PostgresConfig{
		DSN: testPostgresDSN, MaxConns: 7, MinConns: 2, MaxConnLifetime: time.Hour,
	}, "TestComponent")
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()

	if !ownsPool {
		t.Error("ownsPool = false for a DSN-dialed pool, want true")
	}

	cfg := pool.Config()
	if cfg.MaxConns != 7 {
		t.Errorf("MaxConns = %d, want 7", cfg.MaxConns)
	}
	if cfg.MinConns != 2 {
		t.Errorf("MinConns = %d, want 2", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != time.Hour {
		t.Errorf("MaxConnLifetime = %v, want 1h", cfg.MaxConnLifetime)
	}
}

func TestConnectPostgres_RequiresExactlyOneOfDSNOrPool(t *testing.T) {
	somePool, err := pgxpool.New(context.Background(), testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer somePool.Close()
	if err := somePool.Ping(context.Background()); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}

	if _, _, _, err := connectPostgres(context.Background(), PostgresConfig{DSN: testPostgresDSN, Pool: somePool}, "TestComponent"); err == nil {
		t.Fatal("connectPostgres(both DSN and Pool set) = nil error, want non-nil")
	}
	if _, _, _, err := connectPostgres(context.Background(), PostgresConfig{}, "TestComponent"); err == nil {
		t.Fatal("connectPostgres(neither DSN nor Pool set) = nil error, want non-nil")
	}
}

func TestConnectPostgres_SharedPool(t *testing.T) {
	externalPool, err := pgxpool.New(context.Background(), testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer externalPool.Close()
	if err := externalPool.Ping(context.Background()); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}

	pool, queries, ownsPool, err := connectPostgres(context.Background(), PostgresConfig{Pool: externalPool}, "TestComponent")
	if err != nil {
		t.Fatalf("connectPostgres(Pool: externalPool): %v", err)
	}
	if ownsPool {
		t.Error("ownsPool = true for an externally-supplied Pool, want false")
	}
	if pool != externalPool {
		t.Error("connectPostgres returned a different pool than the one supplied via cfg.Pool")
	}

	// The schema must actually have been applied against the shared pool
	// — spot-check one of grnoti's tables exists and is queryable.
	if _, err := queries.GetActiveTokensByUserID(context.Background(), "no-such-user"); err != nil {
		t.Errorf("query against shared pool after connectPostgres: %v", err)
	}
}

func TestConnectPostgres_ConcurrentSchemaApplyDoesNotRace(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- applyPostgresSchema(context.Background(), pool)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent applyPostgresSchema: %v", err)
		}
	}
}
