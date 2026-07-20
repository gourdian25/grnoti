// File: tokenstore.postgres_test.go

package grnoti

import (
	"context"
	"testing"
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
