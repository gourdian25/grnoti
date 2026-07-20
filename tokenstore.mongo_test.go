// File: tokenstore.mongo_test.go

package grnoti

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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

func TestNewMongoTokenStore_EmptyURI(t *testing.T) {
	_, err := NewMongoTokenStore(MongoTokenStoreConfig{Database: "grnoti_test"})
	if err == nil {
		t.Fatal("NewMongoTokenStore(empty URI) = nil error, want non-nil")
	}
}

func TestNewMongoTokenStore_EmptyDatabase(t *testing.T) {
	_, err := NewMongoTokenStore(MongoTokenStoreConfig{URI: testMongoURI})
	if err == nil {
		t.Fatal("NewMongoTokenStore(empty Database) = nil error, want non-nil")
	}
}

func TestNewMongoTokenStore_PingError(t *testing.T) {
	_, err := NewMongoTokenStore(MongoTokenStoreConfig{
		URI: "mongodb://127.0.0.1:1/?connectTimeoutMS=500&serverSelectionTimeoutMS=500", Database: "grnoti_test",
	})
	if err == nil {
		t.Fatal("NewMongoTokenStore(unreachable host) = nil error, want non-nil")
	}
}

func TestNewMongoTokenStore_DefaultsCollectionName(t *testing.T) {
	store, err := NewMongoTokenStore(MongoTokenStoreConfig{
		URI: testMongoURI, Database: fmt.Sprintf("grnoti_test_defaultcoll_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	defer func() { _ = store.Close() }()
	if got := store.(*mongoTokenStore).collection.Name(); got != DefaultTokenCollection {
		t.Fatalf("collection name = %q, want %q (the default)", got, DefaultTokenCollection)
	}
}

func TestMongoTokenStore_GetActiveTokensBatch_SkipsUndecodableDoc(t *testing.T) {
	store := newTestMongoTokenStore(t)
	s := store.(*mongoTokenStore)
	ctx := context.Background()

	_ = store.SaveToken(ctx, DeviceToken{Token: "t-good", UserID: "u1"})
	// is_active with the wrong BSON type: mongoTokenDoc expects a bool, so
	// this document can't be decoded and must be skipped, not fail the
	// whole batch.
	if _, err := s.collection.InsertOne(ctx, bson.M{
		"token": "t-bad", "user_id": "u1", "platform": "android", "is_active": "not-a-bool",
		"created_at": time.Now(), "updated_at": time.Now(),
	}); err != nil {
		t.Fatalf("raw insert: %v", err)
	}

	out, err := store.GetActiveTokensBatch(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("GetActiveTokensBatch: %v", err)
	}
	if len(out["u1"]) != 1 || out["u1"][0].Token != "t-good" {
		t.Fatalf("GetActiveTokensBatch()[u1] = %v, want only the well-formed token", out["u1"])
	}
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

// TestMongoTokenStore_GenericQueryError uses an already-canceled context
// to force a real query-level error from every method — the generic
// (non-ErrClosed) "backend unavailable" wrap branch each method has,
// otherwise unreachable against a healthy database without fault injection.
func TestMongoTokenStore_GenericQueryError(t *testing.T) {
	store := newTestMongoTokenStore(t)
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

func TestMongoTokenStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestMongoTokenStore(t)
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
