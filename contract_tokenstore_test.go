// File: contract_tokenstore_test.go

package grnoti

import (
	"context"
	"testing"
)

// testTokenStoreContract is the shared behavioral contract every TokenStore
// backend must satisfy identically, per gourdiantoken's own table-driven
// cross-backend testing convention (see docs/plan/grnoti-plan.md §4, §7) —
// grnoti's flat, no-subpackage layout means there's no import-cycle reason
// for a separate conformance package the way grcache/graudit need one.
//
// newStore takes the currently-executing *testing.T (not a captured outer
// one) so a backend-unavailable skip inside it calls Skip on the actual
// running subtest — calling an outer *testing.T's Skip/Fatal from inside a
// nested t.Run's goroutine targets the wrong test's bookkeeping.
func testTokenStoreContract(t *testing.T, newStore func(t *testing.T) TokenStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("SaveThenGetActiveTokens", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.SaveToken(ctx, DeviceToken{Token: "contract-t1", UserID: "contract-u1", Platform: PlatformAndroid}); err != nil {
			t.Fatalf("SaveToken: %v", err)
		}
		tokens, err := store.GetActiveTokens(ctx, "contract-u1")
		if err != nil {
			t.Fatalf("GetActiveTokens: %v", err)
		}
		if len(tokens) != 1 || tokens[0].Token != "contract-t1" {
			t.Fatalf("GetActiveTokens() = %v, want [contract-t1]", tokens)
		}
	})

	t.Run("GetActiveTokensForUnknownUserIsEmptyNotError", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		tokens, err := store.GetActiveTokens(ctx, "contract-never-seen")
		if err != nil {
			t.Fatalf("GetActiveTokens(unknown user) error = %v, want nil", err)
		}
		if len(tokens) != 0 {
			t.Fatalf("GetActiveTokens(unknown user) = %v, want empty", tokens)
		}
	})

	t.Run("MarkInvalidRemovesFromActiveList", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveToken(ctx, DeviceToken{Token: "contract-t2", UserID: "contract-u2"})
		if err := store.MarkInvalid(ctx, "contract-t2"); err != nil {
			t.Fatalf("MarkInvalid: %v", err)
		}
		tokens, _ := store.GetActiveTokens(ctx, "contract-u2")
		if len(tokens) != 0 {
			t.Fatalf("GetActiveTokens after MarkInvalid = %v, want empty", tokens)
		}
	})

	t.Run("MarkInvalidOnNonexistentIsNotAnError", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.MarkInvalid(ctx, "contract-never-existed"); err != nil {
			t.Fatalf("MarkInvalid(nonexistent) error = %v, want nil", err)
		}
	})

	t.Run("DeleteTokenRemovesIt", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveToken(ctx, DeviceToken{Token: "contract-t3", UserID: "contract-u3"})
		if err := store.DeleteToken(ctx, "contract-t3"); err != nil {
			t.Fatalf("DeleteToken: %v", err)
		}
		tokens, _ := store.GetActiveTokens(ctx, "contract-u3")
		if len(tokens) != 0 {
			t.Fatalf("GetActiveTokens after DeleteToken = %v, want empty", tokens)
		}
		if err := store.DeleteToken(ctx, "contract-t3"); err != nil {
			t.Fatalf("DeleteToken(already deleted) error = %v, want nil", err)
		}
	})

	t.Run("GetActiveTokensByAnonymousID", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveToken(ctx, DeviceToken{Token: "contract-t4", AnonymousID: "contract-a1"})
		tokens, err := store.GetActiveTokensByAnonymousID(ctx, "contract-a1")
		if err != nil || len(tokens) != 1 {
			t.Fatalf("GetActiveTokensByAnonymousID() = (%v, %v), want 1 token", tokens, err)
		}
	})

	t.Run("GetActiveTokensBatchGroupsByUser", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveToken(ctx, DeviceToken{Token: "contract-t5", UserID: "contract-u5"})
		_ = store.SaveToken(ctx, DeviceToken{Token: "contract-t6", UserID: "contract-u6"})
		out, err := store.GetActiveTokensBatch(ctx, []string{"contract-u5", "contract-u6", "contract-u-missing"})
		if err != nil {
			t.Fatalf("GetActiveTokensBatch: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("GetActiveTokensBatch() = %v, want exactly 2 users present", out)
		}
	})
}

func TestTokenStore_Contract(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		testTokenStoreContract(t, func(t *testing.T) TokenStore { return NewMemoryTokenStore() })
	})
	t.Run("Mongo", func(t *testing.T) {
		testTokenStoreContract(t, func(t *testing.T) TokenStore {
			collection := contractCollectionName(t)
			store, err := NewMongoTokenStore(MongoTokenStoreConfig{
				URI: testMongoURI, Database: "grnoti_test", CollectionName: collection,
			})
			if err != nil {
				t.Skipf("MongoDB not available, skipping: %v", err)
			}
			cleanupContractCollection(t, "grnoti_test", collection)
			return store
		})
	})
	t.Run("Postgres", func(t *testing.T) {
		testTokenStoreContract(t, func(t *testing.T) TokenStore {
			store, err := NewPostgresTokenStore(PostgresConfig{DSN: testPostgresDSN})
			if err != nil {
				t.Skipf("PostgreSQL not available, skipping: %v", err)
			}
			cleanupContractRows(t, "grnoti_tokens", "token")
			return store
		})
	})
}
