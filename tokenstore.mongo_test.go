// File: tokenstore.mongo_test.go

package grnoti

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const testMongoURI = "mongodb://localhost:27017"

func newTestMongoTokenStore(t *testing.T) TokenStore {
	t.Helper()
	store, err := NewMongoTokenStore(MongoTokenStoreConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: fmt.Sprintf("tokens_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestMongoTokenStore_SaveAndGet(t *testing.T) {
	store := newTestMongoTokenStore(t)
	ctx := context.Background()

	if err := store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1", Platform: PlatformAndroid}); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}

	tokens, err := store.GetActiveTokens(ctx, "u1")
	if err != nil {
		t.Fatalf("GetActiveTokens: %v", err)
	}
	if len(tokens) != 1 || tokens[0].Token != "t1" {
		t.Fatalf("GetActiveTokens() = %v, want [t1]", tokens)
	}
}

func TestMongoTokenStore_SaveToken_UpsertReactivates(t *testing.T) {
	store := newTestMongoTokenStore(t)
	ctx := context.Background()

	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})
	_ = store.MarkInvalid(ctx, "t1")
	if tokens, _ := store.GetActiveTokens(ctx, "u1"); len(tokens) != 0 {
		t.Fatalf("expected token inactive after MarkInvalid, got %v", tokens)
	}

	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})
	tokens, err := store.GetActiveTokens(ctx, "u1")
	if err != nil || len(tokens) != 1 {
		t.Fatalf("GetActiveTokens after re-save = (%v, %v), want 1 active token", tokens, err)
	}
}

func TestMongoTokenStore_GetActiveTokensByAnonymousID(t *testing.T) {
	store := newTestMongoTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", AnonymousID: "a1"})

	tokens, err := store.GetActiveTokensByAnonymousID(ctx, "a1")
	if err != nil || len(tokens) != 1 {
		t.Fatalf("GetActiveTokensByAnonymousID() = (%v, %v), want 1 token", tokens, err)
	}
}

func TestMongoTokenStore_GetActiveTokensBatch(t *testing.T) {
	store := newTestMongoTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})
	_ = store.SaveToken(ctx, DeviceToken{Token: "t2", UserID: "u2"})

	out, err := store.GetActiveTokensBatch(ctx, []string{"u1", "u2", "u3"})
	if err != nil || len(out) != 2 {
		t.Fatalf("GetActiveTokensBatch() = (%v, %v), want 2 users", out, err)
	}
}

func TestMongoTokenStore_DeleteToken(t *testing.T) {
	store := newTestMongoTokenStore(t)
	ctx := context.Background()
	_ = store.SaveToken(ctx, DeviceToken{Token: "t1", UserID: "u1"})

	if err := store.DeleteToken(ctx, "t1"); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}
	if tokens, _ := store.GetActiveTokens(ctx, "u1"); len(tokens) != 0 {
		t.Fatalf("token still present after DeleteToken: %v", tokens)
	}
	// Deleting again (nonexistent) is not an error.
	if err := store.DeleteToken(ctx, "t1"); err != nil {
		t.Fatalf("DeleteToken (already deleted): %v", err)
	}
}

func TestMongoTokenStore_Close_Idempotent(t *testing.T) {
	store := newTestMongoTokenStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}
	if _, err := store.GetActiveTokens(context.Background(), "u1"); err != ErrClosed {
		t.Fatalf("GetActiveTokens after Close error = %v, want ErrClosed", err)
	}
}
