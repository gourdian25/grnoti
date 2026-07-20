// File: postgres_test.go

package grnoti

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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
	_, _, err := connectPostgres(context.Background(), PostgresConfig{}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(empty DSN) = nil error, want non-nil")
	}
}

func TestConnectPostgres_MalformedDSN(t *testing.T) {
	_, _, err := connectPostgres(context.Background(), PostgresConfig{DSN: "not a valid dsn ::://"}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(malformed DSN) = nil error, want non-nil")
	}
}

func TestConnectPostgres_UnreachableHost(t *testing.T) {
	_, _, err := connectPostgres(context.Background(), PostgresConfig{DSN: "host=127.0.0.1 port=1 user=x dbname=x connect_timeout=1"}, "TestComponent")
	if err == nil {
		t.Fatal("connectPostgres(unreachable host) = nil error, want non-nil")
	}
}

func TestConnectPostgres_PoolTuningOverrides(t *testing.T) {
	pool, _, err := connectPostgres(context.Background(), PostgresConfig{
		DSN: testPostgresDSN, MaxConns: 7, MinConns: 2, MaxConnLifetime: time.Hour,
	}, "TestComponent")
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()

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
