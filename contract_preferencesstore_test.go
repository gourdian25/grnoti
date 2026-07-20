// File: contract_preferencesstore_test.go

package grnoti

import (
	"context"
	"testing"
)

// testPreferencesStoreContract is the shared behavioral contract every
// PreferencesStore backend must satisfy identically. See
// testTokenStoreContract's doc comment for why newStore takes the
// currently-executing *testing.T.
func testPreferencesStoreContract(t *testing.T, newStore func(t *testing.T) PreferencesStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("GetUnsetReturnsNotFound", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if _, err := store.GetPreferences(ctx, "contract-pu1"); err != ErrPreferencesNotFound {
			t.Fatalf("GetPreferences(unset) error = %v, want ErrPreferencesNotFound", err)
		}
	})

	t.Run("SaveThenGetRoundTrips", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		want := &NotificationPreferences{
			UserID: "contract-pu2", GlobalEnabled: true, Locale: "en",
			EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
		}
		if err := store.SavePreferences(ctx, want); err != nil {
			t.Fatalf("SavePreferences: %v", err)
		}
		got, err := store.GetPreferences(ctx, "contract-pu2")
		if err != nil {
			t.Fatalf("GetPreferences: %v", err)
		}
		if got.GlobalEnabled != want.GlobalEnabled || got.Locale != want.Locale {
			t.Fatalf("GetPreferences() = %+v, want matching %+v", got, want)
		}
	})

	t.Run("IsEventTypeEnabled_UnconfiguredUserDefaultsTrue", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		enabled, err := store.IsEventTypeEnabled(ctx, "contract-pu-never-seen", EventTypeSystemAlert)
		if err != nil || !enabled {
			t.Fatalf("IsEventTypeEnabled(unconfigured) = (%v, %v), want (true, nil)", enabled, err)
		}
	})

	t.Run("IsEventTypeEnabled_GlobalDisabled", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SavePreferences(ctx, &NotificationPreferences{UserID: "contract-pu3", GlobalEnabled: false})
		enabled, err := store.IsEventTypeEnabled(ctx, "contract-pu3", EventTypeSystemAlert)
		if err != nil || enabled {
			t.Fatalf("IsEventTypeEnabled(global disabled) = (%v, %v), want (false, nil)", enabled, err)
		}
	})

	t.Run("IsEventTypeEnabled_PerTypeOptOut", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SavePreferences(ctx, &NotificationPreferences{
			UserID: "contract-pu4", GlobalEnabled: true,
			EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
		})
		enabled, _ := store.IsEventTypeEnabled(ctx, "contract-pu4", EventTypeGenericMarketing)
		if enabled {
			t.Fatal("IsEventTypeEnabled(opted-out type) = true, want false")
		}
		enabled, _ = store.IsEventTypeEnabled(ctx, "contract-pu4", EventTypeSystemAlert)
		if !enabled {
			t.Fatal("IsEventTypeEnabled(no explicit setting) = false, want true (defaults enabled)")
		}
	})
}

func TestPreferencesStore_Contract(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		testPreferencesStoreContract(t, func(t *testing.T) PreferencesStore { return NewMemoryPreferencesStore() })
	})
	t.Run("Postgres", func(t *testing.T) {
		testPreferencesStoreContract(t, func(t *testing.T) PreferencesStore {
			store, err := NewPostgresPreferencesStore(PostgresConfig{DSN: testPostgresDSN})
			if err != nil {
				t.Skipf("PostgreSQL not available, skipping: %v", err)
			}
			cleanupContractRows(t, "grnoti_preferences", "user_id")
			return store
		})
	})
}
