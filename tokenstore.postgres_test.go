// File: tokenstore.postgres_test.go

package grnoti

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testPostgresDSN = "host=localhost user=postgres_user password=postgres_password dbname=grnoti_test port=5432 sslmode=disable"

func newTestPostgresTokenStore(t *testing.T) TokenStore {
	t.Helper()
	store, err := NewPostgresTokenStore(PostgresConfig{DSN: testPostgresDSN})
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	t.Cleanup(func() {
		if s, ok := store.(*postgresTokenStore); ok {
			s.pool.Exec(context.Background(), "DELETE FROM grnoti_tokens")
		}
		_ = store.Close()
	})
	return store
}

func TestNewPostgresTokenStore_ConnectError(t *testing.T) {
	if _, err := NewPostgresTokenStore(PostgresConfig{}); err == nil {
		t.Fatal("NewPostgresTokenStore(empty DSN) = nil error, want non-nil")
	}
}

func TestPostgresTokenStore_SaveAndGet(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx := context.Background()

	if err := store.SaveToken(ctx, DeviceToken{Token: "pgt1", UserID: "pgu1", Platform: PlatformIOS}); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}
	tokens, err := store.GetActiveTokens(ctx, "pgu1")
	if err != nil || len(tokens) != 1 || tokens[0].Token != "pgt1" {
		t.Fatalf("GetActiveTokens() = (%v, %v), want [pgt1]", tokens, err)
	}
}

func TestPostgresTokenStore_SaveToken_UpsertReactivates(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx := context.Background()

	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt2", UserID: "pgu2"})
	_ = store.MarkInvalid(ctx, "pgt2")
	if tokens, _ := store.GetActiveTokens(ctx, "pgu2"); len(tokens) != 0 {
		t.Fatalf("expected inactive after MarkInvalid, got %v", tokens)
	}
	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt2", UserID: "pgu2"})
	if tokens, _ := store.GetActiveTokens(ctx, "pgu2"); len(tokens) != 1 {
		t.Fatalf("expected reactivated after re-save, got %v", tokens)
	}
}

func TestPostgresTokenStore_GetActiveTokensByAnonymousID(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt3", AnonymousID: "pga1"})

	tokens, err := store.GetActiveTokensByAnonymousID(ctx, "pga1")
	if err != nil || len(tokens) != 1 {
		t.Fatalf("GetActiveTokensByAnonymousID() = (%v, %v), want 1 token", tokens, err)
	}
}

func TestPostgresTokenStore_GetActiveTokensBatch(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt4", UserID: "pgu4"})
	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt5", UserID: "pgu5"})

	out, err := store.GetActiveTokensBatch(ctx, []string{"pgu4", "pgu5", "pgu6"})
	if err != nil || len(out) != 2 {
		t.Fatalf("GetActiveTokensBatch() = (%v, %v), want 2 users", out, err)
	}
}

func TestPostgresTokenStore_DeleteToken(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "pgt6", UserID: "pgu6"})

	if err := store.DeleteToken(ctx, "pgt6"); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}
	if tokens, _ := store.GetActiveTokens(ctx, "pgu6"); len(tokens) != 0 {
		t.Fatalf("token still present: %v", tokens)
	}
	if err := store.DeleteToken(ctx, "pgt6"); err != nil {
		t.Fatalf("DeleteToken (already deleted): %v", err)
	}
}

func TestPostgresTokenStore_Close_Idempotent(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if _, err := store.GetActiveTokens(context.Background(), "pgu1"); err != ErrClosed {
		t.Fatalf("GetActiveTokens after Close error = %v, want ErrClosed", err)
	}
}

// TestPostgresTokenStore_GenericQueryError uses an already-canceled
// context to force a real query-level error from every method — the
// generic (non-ErrClosed) "backend unavailable" wrap branch each method
// has, otherwise unreachable against a healthy database without fault
// injection.
func TestPostgresTokenStore_GenericQueryError(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"}); err == nil {
		t.Error("SaveToken(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveTokens(ctx, "u1"); err == nil {
		t.Error("GetActiveTokens(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveTokensByAnonymousID(ctx, "a1"); err == nil {
		t.Error("GetActiveTokensByAnonymousID(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveTokensBatch(ctx, []string{"u1"}); err == nil {
		t.Error("GetActiveTokensBatch(canceled ctx) = nil error, want non-nil")
	}
	if err := store.MarkInvalid(ctx, "t1"); err == nil {
		t.Error("MarkInvalid(canceled ctx) = nil error, want non-nil")
	}
	if err := store.DeleteToken(ctx, "t1"); err == nil {
		t.Error("DeleteToken(canceled ctx) = nil error, want non-nil")
	}
}

// TestPostgresStores_SharedPool_CloseDoesNotAffectSiblingStore is the
// regression test for the ownership fix: two stores built from one
// injected *pgxpool.Pool (PostgresConfig.Pool) must not close that pool
// out from under each other — only a store that dialed its own pool from
// DSN should ever close it.
func TestPostgresStores_SharedPool_CloseDoesNotAffectSiblingStore(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}

	tokenStore, err := NewPostgresTokenStore(PostgresConfig{Pool: pool})
	if err != nil {
		t.Fatalf("NewPostgresTokenStore(Pool: pool): %v", err)
	}
	preferencesStore, err := NewPostgresPreferencesStore(PostgresConfig{Pool: pool})
	if err != nil {
		t.Fatalf("NewPostgresPreferencesStore(Pool: pool): %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM grnoti_tokens")
		pool.Exec(ctx, "DELETE FROM grnoti_preferences")
	})

	if err := tokenStore.Close(); err != nil {
		t.Fatalf("tokenStore.Close(): %v", err)
	}

	// The shared pool, and the sibling store still using it, must remain
	// usable after tokenStore.Close() — proving Close() didn't close the
	// pool it doesn't own.
	if err := preferencesStore.SavePreferences(ctx, &NotificationPreferences{UserID: "shared-pool-user", GlobalEnabled: true}); err != nil {
		t.Fatalf("SavePreferences after sibling store's Close(): %v", err)
	}
	if _, err := preferencesStore.GetPreferences(ctx, "shared-pool-user"); err != nil {
		t.Fatalf("GetPreferences after sibling store's Close(): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pool.Ping() after sibling store's Close(): %v, want the shared pool to still be open", err)
	}

	_ = preferencesStore.Close()
}

func TestPostgresTokenStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestPostgresTokenStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if err := store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"}); err != ErrClosed {
		t.Errorf("SaveToken after Close = %v, want ErrClosed", err)
	}
	if _, err := store.GetActiveTokensByAnonymousID(ctx, "a1"); err != ErrClosed {
		t.Errorf("GetActiveTokensByAnonymousID after Close = %v, want ErrClosed", err)
	}
	if _, err := store.GetActiveTokensBatch(ctx, []string{"u1"}); err != ErrClosed {
		t.Errorf("GetActiveTokensBatch after Close = %v, want ErrClosed", err)
	}
	if err := store.MarkInvalid(ctx, "t1"); err != ErrClosed {
		t.Errorf("MarkInvalid after Close = %v, want ErrClosed", err)
	}
	if err := store.DeleteToken(ctx, "t1"); err != ErrClosed {
		t.Errorf("DeleteToken after Close = %v, want ErrClosed", err)
	}
}
